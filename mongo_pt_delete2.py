import os
import time
import logging
import argparse
import signal
import json
from datetime import datetime, timedelta
from pymongo import MongoClient, errors

# Store deletion statistics
deleted_stats = {}

def setup_logging(log_file):
    """Configure log output"""
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
            handlers=[logging.StreamHandler()]  # STDOUT output
        )

def signal_handler(sig, frame):
    """Handle Ctrl+C (SIGINT) or termination signals and output deletion statistics"""
    logging.info("\n=== Execution Interrupted ===")
    print("\n=== Execution Interrupted ===")
    print("Deleted records summary:")
    for coll, count in deleted_stats.items():
        print(f"  - {coll}: {count} documents deleted")
    logging.info(f"Deleted records summary: {deleted_stats}")
    exit(1)

# Setup signal handling for graceful termination
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def pt_delete(args):
    """MongoDB batch deletion tool with authentication and deletion statistics"""

    # Set up logging
    setup_logging(args.log_file)

    # Parse JSON filter condition
    try:
        filter_condition = json.loads(args.filter)
    except json.JSONDecodeError:
        print("❌ ERROR: Invalid JSON format for `--filter` parameter. Please provide a valid JSON string.")
        exit(1)

    # Calculate deletion threshold
    threshold = datetime.utcnow() - timedelta(days=args.days_to_keep)
    logging.info(f"Connecting to MongoDB: {args.mongo_uri}")
    logging.info(f"Target Database: {args.db_name}")

    try:
        # Connect to MongoDB
        client = MongoClient(args.mongo_uri, username=args.username, password=args.password, authSource=args.auth_db)
        db = client[args.db_name]
    except errors.ConnectionFailure as e:
        print(f"❌ ERROR: Unable to connect to MongoDB - {e}")
        exit(1)

    # Get all collections if `--collection` is not specified
    collections = db.list_collection_names() if args.collection is None else [args.collection]
    
    for collection in collections:
        logging.info(f"Processing Collection: {collection}")
        print(f"🗂️  Processing `{collection}`...")

        # Prepare deletion query
        query = {args.order_by_field: {"$lt": threshold}}
        query.update(filter_condition)

        # Get total document count for deletion
        total_count = db[collection].count_documents(query)
        logging.info(f"Total documents to delete in {collection}: {total_count}")

        if args.dry_run:
            print(f"✅ [DRY RUN] {total_count} documents would be deleted, but no actual deletion performed.")
            continue

        total_deleted = 0
        progress_counter = 0
        deleted_stats[collection] = 0

        while total_deleted < total_count:
            # 1) Retrieve the oldest BATCH_SIZE documents
            old_docs = list(db[collection].find(query, {"_id": 1}).sort(args.order_by_field, 1).limit(args.batch_size))
            if not old_docs:
                logging.info(f"No more old documents to delete in {collection}. Total deleted: {total_deleted}")
                break

            # 2) Batch delete using `_id`
            delete_result = db[collection].delete_many({"_id": {"$in": [doc["_id"] for doc in old_docs]}})
            total_deleted += delete_result.deleted_count
            progress_counter += delete_result.deleted_count
            deleted_stats[collection] += delete_result.deleted_count

            # 3) Display progress
            if progress_counter >= args.progress_interval:
                print(f"[{collection}] Deleted {total_deleted}/{total_count} documents")
                progress_counter = 0

            # 4) Control deletion rate
            if args.delete_rate > 0:
                time.sleep(1 / args.delete_rate)

            # 5) Pause to minimize business impact
            time.sleep(args.sleep_time)

        print(f"✅ `{collection}` cleanup completed. Total deleted: {total_deleted} records.")

    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MongoDB batch deletion tool (`pt-archive` delete function)")

    parser.add_argument("--mongo_uri", type=str, required=True, help="MongoDB connection URI (e.g., mongodb://host:port)")
    parser.add_argument("--db_name", type=str, required=True, help="Target MongoDB database for cleanup")
    parser.add_argument("--collection", type=str, default=None, help="Target collection to clean up (if not specified, all collections will be processed)")
    parser.add_argument("--username", type=str, default=None, help="MongoDB authentication username (optional)")
    parser.add_argument("--password", type=str, default=None, help="MongoDB authentication password (optional)")
    parser.add_argument("--auth_db", type=str, default="admin", help="MongoDB authentication database (default: admin)")
    parser.add_argument("--days_to_keep", type=int, default=30, help="Number of days to retain data (default: 30)")
    parser.add_argument("--batch_size", type=int, default=5000, help="Batch size for deletions (default: 5000)")
    parser.add_argument("--sleep_time", type=float, default=0.5, help="Sleep time (seconds) between deletions to minimize impact (default: 0.5)")
    parser.add_argument("--progress_interval", type=int, default=20000, help="Show progress after deleting this many records (default: 20000)")
    parser.add_argument("--order_by_field", type=str, default="timestamp", help="Field used for sorting deletions (default: timestamp)")
    parser.add_argument("--dry_run", action="store_true", help="Run in dry mode, showing deletion count without actually deleting records")
    parser.add_argument("--delete_rate", type=int, default=0, help="Limit deletion rate (records per second, 0 = unlimited)")
    parser.add_argument("--filter", type=str, default="{}", help="Custom MongoDB filter condition in JSON format")
    parser.add_argument("--log_file", type=str, default=None, help="Log file path (if not specified, logs will be output to STDOUT)")

    args = parser.parse_args()
    pt_delete(args)
