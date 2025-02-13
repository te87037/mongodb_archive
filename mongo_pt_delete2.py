import os
import time
import logging
import argparse
import signal
import json
from datetime import datetime, timedelta
from pymongo import MongoClient, errors

# 儲存刪除統計資訊
deleted_stats = {}

def setup_logging(log_file):
    """設定日誌輸出方式"""
    if log_file:
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]  # STDOUT 輸出
        )

def signal_handler(sig, frame):
    """捕捉 Ctrl+C（SIGINT）或其他終止訊號，並輸出刪除統計"""
    logging.info("\n=== Execution Interrupted ===")
    print("\n=== Execution Interrupted ===")
    print("Deleted records summary:")
    for coll, count in deleted_stats.items():
        print(f"  - {coll}: {count} documents deleted")
    logging.info(f"Deleted records summary: {deleted_stats}")
    exit(1)

# 設定訊號處理器（支援 Ctrl+C 統計）
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def pt_delete(args):
    """MongoDB 批次刪除工具，支援帳號密碼，統計刪除數量"""

    # 設定日誌
    setup_logging(args.log_file)

    # 解析 JSON 過濾條件
    try:
        filter_condition = json.loads(args.filter)
    except json.JSONDecodeError:
        print("❌ 錯誤: `--filter` 參數的 JSON 格式錯誤！請輸入正確的 JSON 字串。")
        exit(1)

    # 計算要刪除的時間門檻
    threshold = datetime.utcnow() - timedelta(days=args.days_to_keep)
    logging.info(f"Connecting to MongoDB: {args.mongo_uri}")
    logging.info(f"Target Database: {args.db_name}")

    try:
        # 連線到 MongoDB
        client = MongoClient(args.mongo_uri, username=args.username, password=args.password, authSource=args.auth_db)
        db = client[args.db_name]
    except errors.ConnectionFailure as e:
        print(f"❌ 錯誤: 無法連接 MongoDB - {e}")
        exit(1)

    # 取得所有 collections（若 `--collection` 未指定）
    collections = db.list_collection_names() if args.collection is None else [args.collection]
    
    for collection in collections:
        logging.info(f"Processing Collection: {collection}")
        print(f"🗂️  開始處理 `{collection}`...")

        # 準備刪除查詢條件
        query = {args.order_by_field: {"$lt": threshold}}
        query.update(filter_condition)

        # 取得總資料筆數
        total_count = db[collection].count_documents(query)
        logging.info(f"Total documents to delete in {collection}: {total_count}")

        if args.dry_run:
            print(f"✅ 【乾跑模式】將刪除 {total_count} 筆資料，但未實際刪除。")
            continue

        total_deleted = 0
        progress_counter = 0
        deleted_stats[collection] = 0

        while total_deleted < total_count:
            # 1) 先找出最舊的 BATCH_SIZE 筆資料
            old_docs = list(db[collection].find(query, {"_id": 1}).sort(args.order_by_field, 1).limit(args.batch_size))
            if not old_docs:
                logging.info(f"No more old documents to delete in {collection}. Total deleted: {total_deleted}")
                break

            # 2) 透過 `_id` 批次刪除
            delete_result = db[collection].delete_many({"_id": {"$in": [doc["_id"] for doc in old_docs]}})
            total_deleted += delete_result.deleted_count
            progress_counter += delete_result.deleted_count
            deleted_stats[collection] += delete_result.deleted_count

            # 3) 印出進度
            if progress_counter >= args.progress_interval:
                print(f"[{collection}] Deleted {total_deleted}/{total_count} documents")
                progress_counter = 0

            # 4) 控制刪除速率
            if args.delete_rate > 0:
                time.sleep(1 / args.delete_rate)

            # 5) 短暫休息，減少業務影響
            time.sleep(args.sleep_time)

        print(f"✅ `{collection}` 清理完成，總刪除 {total_deleted} 筆資料")

    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MongoDB 批次刪除工具 (`pt-archive` 刪除功能)")

    parser.add_argument("--mongo_uri", type=str, required=True, help="MongoDB 連線 URI (格式: mongodb://host:port)")
    parser.add_argument("--db_name", type=str, required=True, help="要清理的 MongoDB 資料庫名稱")
    parser.add_argument("--collection", type=str, default=None, help="要清理的集合名稱 (未指定則清理所有集合)")
    parser.add_argument("--username", type=str, default=None, help="MongoDB 登入帳號 (可選)")
    parser.add_argument("--password", type=str, default=None, help="MongoDB 登入密碼 (可選)")
    parser.add_argument("--auth_db", type=str, default="admin", help="MongoDB 驗證資料庫，預設: admin")
    parser.add_argument("--days_to_keep", type=int, default=30, help="保留多少天的資料 (預設: 30 天)")
    parser.add_argument("--batch_size", type=int, default=5000, help="每次刪除的批次大小 (預設: 5000)")
    parser.add_argument("--sleep_time", type=float, default=0.5, help="每批刪除後的休息時間 (秒，預設: 0.5)")
    parser.add_argument("--progress_interval", type=int, default=20000, help="每刪除多少筆顯示一次進度 (預設: 20000)")
    parser.add_argument("--order_by_field", type=str, default="timestamp", help="用於刪除排序的索引欄位 (預設: timestamp)")
    parser.add_argument("--dry_run", action="store_true", help="只顯示預計刪除的筆數，不執行刪除")
    parser.add_argument("--delete_rate", type=int, default=0, help="限制每秒刪除的筆數 (0 表示不限制)")
    parser.add_argument("--filter", type=str, default="{}", help="自定義的 MongoDB 過濾條件 (JSON 格式)")
    parser.add_argument("--log_file", type=str, default=None, help="指定日誌存放位置，未指定則輸出至 STDOUT")

    args = parser.parse_args()
    pt_delete(args)
