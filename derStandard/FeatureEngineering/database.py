# database.py
from pymongo import MongoClient
import os
import logging

logger = logging.getLogger(__name__)

def get_db_connection(collection='derStandard'):
    try:
        USERNAME = os.getenv("MONGODB_USER")
        PASSWORD = os.getenv("MONGODB_PWD")
        if not USERNAME or not PASSWORD:
            raise ValueError("MONGODB_USER und MONGODB_PWD müssen als Umgebungsvariablen gesetzt sein.")

        client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
        db = client['newspapers']
        db_collection = db[collection]
        logger.debug(f"Verbindung zur Collection '{collection}' erfolgreich hergestellt.")
        return db_collection
    except Exception as e:
        logger.error(f"Fehler bei der Datenbankverbindung: {e}", exc_info=True)
        raise
