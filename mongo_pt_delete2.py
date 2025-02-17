import os
import time
import logging
import argparse
import signal
import json
import configparser
import subprocess
from datetime import datetime, timedelta
from pymongo import MongoClient, errors

# Store deletion statistics
deleted_stats = {}

def get_secret_password(secret_name):
    """Retrieve password from Google Cloud Secret Manager"""
    try:
        result = subprocess.run(
            ["gcloud", "secrets", "versions", "access", "latest", "--secret", secret_name],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error("Failed to retrieve secret password: %s", e)
        return None

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
        if count > 0:
            print(f"  - {coll}: {count} documents deleted")
    logging.info(f"Deleted records summary: {deleted_stats}")
    exit(1)

# Setup signal handling for graceful termination
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def load_config(config_file):
    """Load MongoDB settings from config.ini"""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['MongoDB'] if 'MongoDB' in config else {}

def pt_delete(args):
    """MongoDB batch deletion tool with authentication and deletion statistics"""
    setup_logging(args.log_file)
    
    try:
        filter_condition = json.loads(args.filter) if args.filter else {}
    except json.JSONDecodeError:
        print("❌ ERROR: Invalid JSON format for `--filter` parameter. Please provide a valid JSON string.")
        exit(1)
    
    # Handle password retrieval from Google Cloud Secret Manager
    if args.password.startswith("gcloud_secret:"):
        secret_name = args.password.replace("gcloud_secret:", "").strip()
        args.password = get_secret_password(secret_name)
        if not args.password:
            print("❌ ERROR: Failed to retrieve password from Google Cloud Secret Manager")
            exit(1)
    
    threshold = datetime.utcnow() - timedelta(days=args.days_to_keep)
    logging.info(f"Connecting to MongoDB: {args.mongo_uri}")
    logging.info(f"Target Database: {args.db_name}")
    
    try:
        client = MongoClient(args.mongo_uri, username=args.username, password=args.password, authSource=args.auth_db)
        db = client[args.db_name]
    except errors.ConnectionFailure as e:
        print(f"❌ ERROR: Unable to connect to MongoDB - {e}")
        exit(1)
    
    collections = db.list_collection_names() if args.collection is None else [args.collection]
    
    for collection in collections:
        logging.info(f"Processing Collection: {collection}")
        print(f"🗂️  Processing `{collection}`...")
        
        query = {args.order_by_field: {"$lt": threshold}}
        query.update(filter_condition)
        
        total_count = db[collection].count_documents(query)
        logging.info(f"Total documents to delete in {collection}: {total_count}")
        
        if args.dry_run:
            print(f"✅ [DRY RUN] {total_count} documents would be deleted, but no actual deletion performed.")
            continue
        
        total_deleted = 0
        progress_counter = 0
        deleted_stats[collection] = 0
        
        while total_deleted < total_count:
            old_docs = list(db[collection].find(query, {"_id": 1}).sort(args.order_by_field, 1).limit(args.batch_size))
            if not old_docs:
                break
            
            delete_result = db[collection].delete_many({"_id": {"$in": [doc["_id"] for doc in old_docs]}})
            total_deleted += delete_result.deleted_count
            progress_counter += delete_result.deleted_count
            deleted_stats[collection] += delete_result.deleted_count
            
            if progress_counter >= args.progress_interval:
                print(f"[{collection}] Deleted {total_deleted}/{total_count} documents")
                progress_counter = 0
            
            time.sleep(args.sleep_time)
        
        print(f"✅ `{collection}` cleanup completed. Total deleted: {total_deleted} records.")
    
    print("\n=== Deletion Summary ===")
    for coll, count in deleted_stats.items():
        if count > 0:
            print(f"  - {coll}: {count} documents deleted")
    logging.info(f"Final deletion summary: {deleted_stats}")
    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MongoDB batch deletion tool (`pt-archive` delete function)")
    parser.add_argument("--config", type=str, help="Path to config.ini file")
    
    args, unknown = parser.parse_known_args()
    config = load_config(args.config) if args.config else {}

    parser.add_argument("--progress_interval", action="store_true", default=int(config.get("progress_interval", 10000)), help="Print out the progress with certain counts")
    parser.add_argument("--dry_run", action="store_true", default=False, help="Perform a dry run without actually deleting any documents")
    parser.add_argument("--order_by_field", type=str, default=config.get("order_by_field", "timestamp"), help="Field used for sorting deletions")
    parser.add_argument("--filter", type=str, help="JSON string specifying additional deletion filters, e.g., '{\"status\": \"inactive\"}'")
    parser.add_argument("--mongo_uri", type=str, default=config.get("mongo_uri", "mongodb://127.0.0.1:27017"), help="MongoDB connection URI")
    parser.add_argument("--db_name", type=str, default=config.get("db_name", ""), help="Target MongoDB database for cleanup")
    parser.add_argument("--collection", type=str, default=config.get("collection", None), help="Target collection (default: all)")
    parser.add_argument("--username", type=str, default=config.get("username", None), help="MongoDB authentication username")
    parser.add_argument("--password", type=str, default=config.get("password", ""), help="MongoDB authentication password (supports 'gcloud_secret:<secret_name>' for retrieval from Google Cloud Secret Manager)")
    parser.add_argument("--auth_db", type=str, default=config.get("auth_db", "admin"), help="MongoDB authentication database")
    parser.add_argument("--days_to_keep", type=int, default=int(config.get("days_to_keep", 30)), help="Days to retain data")
    parser.add_argument("--batch_size", type=int, default=int(config.get("batch_size", 5000)), help="Batch size for deletions")
    parser.add_argument("--sleep_time", type=float, default=float(config.get("sleep_time", 0.5)), help="Sleep time (seconds) between deletions")
    parser.add_argument("--log_file", type=str, default=config.get("log_file", None), help="Log file path")
    
    args = parser.parse_args()
    pt_delete(args)
