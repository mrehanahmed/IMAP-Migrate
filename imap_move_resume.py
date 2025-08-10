import json
import yaml
import argparse
import logging
import sqlite3
import time
import threading
from imapclient import IMAPClient
from email.parser import BytesParser
from email.policy import default as default_policy
import imaplib

# --- Database schema for resume support ---
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY,
    src_mailbox TEXT NOT NULL,
    src_uid TEXT NOT NULL,
    dst_mailbox TEXT,
    dst_uid TEXT,
    message_id TEXT,
    transferred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(src_mailbox, src_uid)
);
CREATE INDEX IF NOT EXISTS idx_message_id ON transfers(message_id);
"""

def open_db(path: str):
    conn = sqlite3.connect(path)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.executescript(DB_SCHEMA)
    conn.commit()
    return conn

def record_transfer(conn, src_mailbox, src_uid, dst_mailbox, dst_uid, message_id):
    conn.execute(
        'INSERT OR REPLACE INTO transfers (src_mailbox, src_uid, dst_mailbox, dst_uid, message_id) VALUES (?,?,?,?,?)',
        (src_mailbox, str(src_uid), dst_mailbox, str(dst_uid) if dst_uid else None, message_id),
    )
    conn.commit()

def already_transferred_by_src(conn, src_mailbox, src_uid):
    cur = conn.execute('SELECT 1 FROM transfers WHERE src_mailbox = ? AND src_uid = ? LIMIT 1',
                       (src_mailbox, str(src_uid)))
    return cur.fetchone()

# --- IMAP connection helpers ---
def connect_imap(host, username, password, port=None, ssl=True):
    logging.info(f'Connecting to {host}:{port or "default"} as {username}')
    client = IMAPClient(host, port=port, ssl=ssl)
    client.login(username, password)
    return client

def reconnect_imap(old_client, host, username, password, port=None, ssl=True):
    try:
        old_client.logout()
    except Exception:
        pass
    time.sleep(3)
    return connect_imap(host, username, password, port, ssl)

def list_mailboxes(client):
    return [mbox.decode() if isinstance(mbox, bytes) else mbox
            for flags, delim, mbox in client.list_folders()]

def ensure_mailbox(client, mailbox):
    try:
        client.select_folder(mailbox)
        return True
    except:
        try:
            client.create_folder(mailbox)
            return True
        except:
            logging.warning(f'Could not create mailbox {mailbox}')
            return False

# --- Spinner for visual feedback ---
def spinner_task(stop_event):
    spinner_chars = '|/-\\'
    idx = 0
    last_log_time = time.time()
    while not stop_event.is_set():
        print(spinner_chars[idx % len(spinner_chars)], end='\r', flush=True)
        idx += 1
        if time.time() - last_log_time > 15:
            logging.info("Migration still in progress...")
            last_log_time = time.time()
        time.sleep(0.1)
    print(' ', end='\r', flush=True)

# --- Safe search with retries ---
def safe_search(client, criteria='ALL', max_retries=5, base_delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            return client.search(criteria)
        except imaplib.IMAP4.abort as e:
            logging.warning(f"SEARCH failed (attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                raise
            time.sleep(base_delay * attempt)

# --- Main migration logic ---
def migrate_mailbox(src_cfg, dst_cfg, conn_db, src_mailbox, dst_mailbox, batch=50, sleep_between=2, dry_run=False):
    src_host, src_user, src_pass, src_port, src_ssl = src_cfg
    dst_host, dst_user, dst_pass, dst_port, dst_ssl = dst_cfg

    logging.info(f'Migrating {src_mailbox} -> {dst_mailbox}')
    src = connect_imap(*src_cfg)
    dst = connect_imap(*dst_cfg)

    if not ensure_mailbox(dst, dst_mailbox):
        src.logout()
        dst.logout()
        return

    archive_mailbox = f"Migrated/{src_mailbox}"
    ensure_mailbox(src, archive_mailbox)

    try:
        src.select_folder(src_mailbox, readonly=False)
    except Exception as e:
        logging.error(f"Cannot select source folder {src_mailbox}: {e}")
        src.logout()
        dst.logout()
        return

    try:
        uids = safe_search(src, 'ALL')
    except Exception as e:
        logging.error(f"SEARCH failed on {src_mailbox}: {e}")
        src.logout()
        dst.logout()
        return

    stop_spinner = threading.Event()
    spinner_thread = threading.Thread(target=spinner_task, args=(stop_spinner,))
    spinner_thread.start()

    try:
        for i in range(0, len(uids), batch):
            batch_uids = uids[i:i+batch]
            fetch_data = None

            for attempt in range(3):
                try:
                    fetch_data = src.fetch(batch_uids, ['RFC822', 'FLAGS', 'INTERNALDATE'])
                    break
                except imaplib.IMAP4.abort as e:
                    logging.warning(f"FETCH abort: {e}, reconnecting source...")
                    src = reconnect_imap(src, *src_cfg)
                    src.select_folder(src_mailbox, readonly=False)
            if fetch_data is None:
                continue

            for uid in batch_uids:
                if already_transferred_by_src(conn_db, src_mailbox, uid):
                    continue
                raw_msg = fetch_data.get(uid)
                if not raw_msg:
                    continue
                msg_bytes = raw_msg[b'RFC822']
                parser = BytesParser(policy=default_policy)
                msg = parser.parsebytes(msg_bytes)
                message_id = msg.get('Message-ID')

                if not dry_run:
                    for attempt in range(3):
                        try:
                            dst.append(dst_mailbox, msg_bytes,
                                       flags=raw_msg[b'FLAGS'],
                                       msg_time=raw_msg[b'INTERNALDATE'])
                            break
                        except imaplib.IMAP4.abort as e:
                            logging.warning(f"APPEND abort: {e}, reconnecting destination...")
                            dst = reconnect_imap(dst, *dst_cfg)
                            ensure_mailbox(dst, dst_mailbox)

                    try:
                        src.move([uid], archive_mailbox)
                    except Exception as e:
                        logging.error(f"Failed to move UID {uid} to archive: {e}")
                        continue

                record_transfer(conn_db, src_mailbox, uid, dst_mailbox, None, message_id)

            if sleep_between > 0:
                time.sleep(sleep_between)
    finally:
        stop_spinner.set()
        spinner_thread.join()
        src.logout()
        dst.logout()

# --- Loaders ---
def load_mapping(path):
    with open(path, 'r', encoding='utf-8') as f:
        if path.endswith('.json'):
            return json.load(f)
        elif path.endswith(('.yml', '.yaml')):
            return yaml.safe_load(f)
        else:
            raise ValueError('Mapping file must be JSON or YAML')

def load_exclude_list(path):
    with open(path, 'r', encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}

def load_config(path):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

# --- CLI parsing ---
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--config', required=True)
    p.add_argument('--mapping-file')
    p.add_argument('--exclude-file')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--verbose', action='store_true')
    return p.parse_args()

# --- Main ---
def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s')
    config = load_config(args.config)

    exclude_folders = load_exclude_list(args.exclude_file) if args.exclude_file else set()
    if exclude_folders:
        logging.info(f'Excluding folders: {exclude_folders}')

    conn_db = open_db(config['database']['path'])
    src_cfg = (config['source']['host'], config['source']['user'], config['source']['pass'],
               config['source'].get('port'), config['source'].get('ssl', True))
    dst_cfg = (config['destination']['host'], config['destination']['user'], config['destination']['pass'],
               config['destination'].get('port'), config['destination'].get('ssl', True))

    mapping = load_mapping(args.mapping_file) if args.mapping_file else None

    src = connect_imap(*src_cfg)
    mailboxes = list_mailboxes(src)
    src.logout()

    for src_mailbox in mailboxes:
        if src_mailbox in exclude_folders:
            logging.info(f"Skipping excluded folder: {src_mailbox}")
            continue
        dst_mailbox = mapping.get(src_mailbox, src_mailbox) if mapping else src_mailbox
        migrate_mailbox(src_cfg, dst_cfg, conn_db, src_mailbox, dst_mailbox, dry_run=args.dry_run)

    conn_db.close()

if __name__ == '__main__':
    main()
