# IMAP-Migrate
IMAP Migration from one email account to another (Non OAuth2)

#Run using the command 

python imap_move_resume.py --config config.yaml --exclude-file skip_folders.txt --verbose


# With optional - Example mapping file (folder_map.yaml) if you want to rename folders during migration:

python imap_move_resume.py --config config.yaml --exclude-file skip_folders.txt --verbose --mapping-file folder_map.yaml 

