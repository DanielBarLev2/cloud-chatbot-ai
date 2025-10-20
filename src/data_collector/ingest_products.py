import os, sys, time
import pandas as pd
from dotenv import load_dotenv
from src.database.mongo_client import get_mongo_collection
from src.database.s3_client import upload_file
from src.utils.logger import get_logger
from pymongo import UpdateOne

load_dotenv()
logger = get_logger("ingestor")

def normalize_row(row: dict) -> dict:
    # Minimal schema hygiene
    return {
        "product_id": str(row.get("product_id", "")).strip(),
        "name": str(row.get("name", "")).strip(),
        "brand": str(row.get("brand", "")).strip(),
        "category": str(row.get("category", "")).strip(),
        "price": float(row.get("price", 0.0)),
        "currency": str(row.get("currency", "USD")).strip(),
        "stock": int(row.get("stock", 0)),
        "ingested_at": int(time.time()),
        "source": "sample_csv",
    }
    
    
def upsert_products(df: pd.DataFrame, batch_size: int = 100):
    coll = get_mongo_collection()

    # (optional but recommended) ensure unique index once
    try:
        coll.create_index("product_id", unique=True)
    except Exception as e:
        logger.warning(f"Index creation warning: {e}")

    ops, total = [], 0
    for _, r in df.iterrows():
        doc = normalize_row(r.to_dict())
        ops.append(
            UpdateOne(
                {"product_id": doc["product_id"]},
                {"$set": doc},
                upsert=True
            )
        )

        if len(ops) >= batch_size:
            res = coll.bulk_write(ops, ordered=False)
            total += res.upserted_count + res.modified_count
            logger.info(f"Upserted batch -> matched={res.matched_count}, "
                        f"modified={res.modified_count}, upserted={res.upserted_count}")
            ops = []

    if ops:
        res = coll.bulk_write(ops, ordered=False)
        total += res.upserted_count + res.modified_count
        logger.info(f"Upserted final batch -> matched={res.matched_count}, "
                    f"modified={res.modified_count}, upserted={res.upserted_count}")

    return total

def main():
    csv_path = os.getenv("CSV_PATH", "data/sample_products.csv")
    bucket = os.getenv("AWS_S3_BUCKET")
    region = os.getenv("AWS_REGION", "eu-west-1")

    if not os.path.exists(csv_path):
        logger.error(f"CSV not found: {csv_path}")
        sys.exit(1)

    # 1) Backup raw to S3 (versioned by timestamp)
    if bucket:
        key = f"raw/products/{int(time.time())}_{os.path.basename(csv_path)}"
        upload_file(csv_path, bucket, key)
        logger.info(f"Backed up raw to s3://{bucket}/{key}")
    else:
        logger.warning("AWS_S3_BUCKET not set — skipping S3 backup")

    # 2) Load → normalize → upsert into Mongo
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows from {csv_path}")
    total = upsert_products(df)
    logger.info(f"Ingestion complete. Upserted/updated: {total}")

if __name__ == "__main__":
    main()
