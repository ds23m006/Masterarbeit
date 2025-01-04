import os
from pymongo import MongoClient

def get_db_connection(collection='Krone'):
    """
    Gibt eine MongoDB-Collection für Krone zurück.
    """
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")

    # Beispiel: MongoDB läuft auf Host 'BlackWidow' und Port 27017
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")

    db = client['newspapers']
    return db[collection]
