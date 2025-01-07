from pymongo import MongoClient
import os

def get_db_connection(collection='derStandard'):
    # get env
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")

    # get client
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")

    # get db
    db = client['newspapers']
    db = db[collection]

    return db