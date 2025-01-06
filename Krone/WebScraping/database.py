import os
from pymongo import MongoClient

def get_db_connection(collection='Krone'):
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']
    return db[collection]
