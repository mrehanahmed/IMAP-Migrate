# IMAP-Migrate
IMAP Migration from one email account to another (Non OAuth2)

full ready-to-run IMAP migration script with:

Per-folder move from source to Migrated/<Folder>

SQLite resume support (skips already-migrated emails)

Reconnect + retry logic for large mailboxes

Config-driven (YAML for IMAP creds + database)

# Run using the command 

python imap_move_resume.py --config config.yaml --exclude-file skip_folders.txt --verbose


# With optional - Example mapping file (folder_map.yaml) if you want to rename folders during migration:

python imap_move_resume.py --config config.yaml --exclude-file skip_folders.txt --verbose --mapping-file folder_map.yaml 

# What each part means
--config config.yaml → Uses your email credentials and DB path from config.yaml.

--exclude-file skip_folders.txt → Skips folders you don’t want to migrate.

--mapping-file folder_map.yaml → (Optional) Renames folders during migration.

--verbose → Prints detailed logs so you can watch it work.

# First run
It will create migrate_resume.sqlite automatically.

Migrated emails will be moved into Migrated/<OriginalFolder> on Yahoo.

If the script stops (timeout, network issue, etc.), re-run the same command —
it will resume exactly where it left off.

#  Second run / Resume
No need to change anything — just run the same command.

Already-migrated messages are skipped thanks to the SQLite database.

If you want, I can now give you the final complete Python script that has the move → Migrated/<Folder> logic and resume support fully built-in so you can run this exact command.

