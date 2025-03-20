import os
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient


def get_docs(collection):
    """
    Liefert die Dokumente einer Collection zurück.
    :param collection: Collection
    :return: Liste von Dokumenten
    """
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']

    # Zugriff auf die Collection 'derStandard'
    collection = db[collection]

    # Abfrage der Dokumente mit befülltem 'features.APA_OeNB_Sentiment'
    docs = collection.find({"features.APA_OeNB_Sentiment": {"$exists": True, "$ne": None}})
    
    return docs