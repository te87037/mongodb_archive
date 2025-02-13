# mongodb_archive
simple tool for mongodb archive
python3 mongo_pt_delete2.py --help
usage: mongo_pt_delete2.py [-h] --mongo_uri MONGO_URI --db_name DB_NAME
                           [--collection COLLECTION] [--username USERNAME]
                           [--password PASSWORD] [--auth_db AUTH_DB]
                           [--days_to_keep DAYS_TO_KEEP]
                           [--batch_size BATCH_SIZE] [--sleep_time SLEEP_TIME]
                           [--progress_interval PROGRESS_INTERVAL]
                           [--order_by_field ORDER_BY_FIELD] [--dry_run]
                           [--delete_rate DELETE_RATE] [--filter FILTER]
                           [--log_file LOG_FILE]

MongoDB 批次刪除工具 (`pt-archive` 刪除功能)

optional arguments:
  -h, --help            show this help message and exit
  --mongo_uri MONGO_URI
                        MongoDB 連線 URI (格式: mongodb://host:port)
  --db_name DB_NAME     要清理的 MongoDB 資料庫名稱
  --collection COLLECTION
                        要清理的集合名稱 (未指定則清理所有集合)
  --username USERNAME   MongoDB 登入帳號 (可選)
  --password PASSWORD   MongoDB 登入密碼 (可選)
  --auth_db AUTH_DB     MongoDB 驗證資料庫，預設: admin
  --days_to_keep DAYS_TO_KEEP
                        保留多少天的資料 (預設: 30 天)
  --batch_size BATCH_SIZE
                        每次刪除的批次大小 (預設: 5000)
  --sleep_time SLEEP_TIME
                        每批刪除後的休息時間 (秒，預設: 0.5)
  --progress_interval PROGRESS_INTERVAL
                        每刪除多少筆顯示一次進度 (預設: 20000)
  --order_by_field ORDER_BY_FIELD
                        用於刪除排序的索引欄位 (預設: timestamp)
  --dry_run             只顯示預計刪除的筆數，不執行刪除
  --delete_rate DELETE_RATE
                        限制每秒刪除的筆數 (0 表示不限制)
  --filter FILTER       自定義的 MongoDB 過濾條件 (JSON 格式)
  --log_file LOG_FILE   指定日誌存放位置，未指定則輸出至 STDOUT
