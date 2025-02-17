# MongoDB batch deletion tool (`pt-archive` delete function)

## Usage
mongo_pt_delete2.py [-h] --mongo_uri MONGO_URI --db_name DB_NAME [--collection COLLECTION] [--username USERNAME] [--password PASSWORD] [--auth_db AUTH_DB] [--days_to_keep DAYS_TO_KEEP] [--batch_size BATCH_SIZE] [--sleep_time SLEEP_TIME] [--progress_interval PROGRESS_INTERVAL] [--order_by_field ORDER_BY_FIELD] [--dry_run] [--delete_rate DELETE_RATE] [--filter FILTER] [--log_file LOG_FILE]

## Optional arguments

| Argument | Description |
|----------|------------|
| `-h, --help` | Show this help message and exit |
| `--mongo_uri MONGO_URI` | MongoDB connection URI (e.g., `mongodb://host:port`) |
| `--db_name DB_NAME` | Target MongoDB database for cleanup |
| `--collection COLLECTION` | Target collection to clean up (if not specified, all collections will be processed) |
| `--username USERNAME` | MongoDB authentication username (optional) |
| `--password PASSWORD` | MongoDB authentication password (optional) |
| `--auth_db AUTH_DB` | MongoDB authentication database (default: `admin`) |
| `--days_to_keep DAYS_TO_KEEP` | Number of days to retain data (default: `30`) |
| `--batch_size BATCH_SIZE` | Batch size for deletions (default: `5000`) |
| `--sleep_time SLEEP_TIME` | Sleep time (seconds) between deletions to minimize impact (default: `0.5`) |
| `--progress_interval PROGRESS_INTERVAL` | Show progress after deleting this many records (default: `20000`) |
| `--order_by_field ORDER_BY_FIELD` | Field used for sorting deletions (default: `timestamp`) |
| `--dry_run` | Run in dry mode, showing deletion count without actually deleting records |
| `--filter FILTER` | Custom MongoDB filter condition in JSON format |
| `--log_file LOG_FILE` | Log file path (if not specified, logs will be output to `STDOUT`) |


Hope this tool can help every mongodb user.
