import os
from pymongo import MongoClient
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger("mongo")

def get_mongo_collection():
    """
    	Fetch MongoDB collection based on environment variables.
                Returns:
		pymongo.collection.Collection: MongoDB collection
	"""
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB", "product_db")
    coll_name = os.getenv("MONGO_COLLECTION", "products")
    
    if not uri:
        raise ValueError("MONGO_URI missing")
    
    client = MongoClient(uri,
                         tls=True, # encrypted connection
                         tlsAllowInvalidCertificates=True)
    db = client[db_name]
    
    logger.info(f"Connected to MongoDB db={db_name}, coll={coll_name}")
    
    return db[coll_name]
