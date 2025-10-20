import os, boto3
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger("s3")

def get_s3():
    region = os.getenv("AWS_REGION", "eu-west-1")
    s3 = boto3.client("s3", region_name=region)
    return s3

def upload_file(local_path: str, bucket: str, key: str):
    s3 = get_s3()
    logger.info(f"Uploading {local_path} -> s3://{bucket}/{key}")
    s3.upload_file(local_path, bucket, key)