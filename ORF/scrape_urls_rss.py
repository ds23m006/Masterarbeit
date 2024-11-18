import feedparser
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# load env vars
load_dotenv()

# Funktion zur Verbindung mit der MongoDB-Collection
def get_db_connection(collection='ORF'):
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    print(USERNAME)
    
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']
    collection = db[collection]
    return collection

# Verbindung zur Datenbank
collection = get_db_connection()

documents = []

# URL des RSS-Feeds
rss_list = ['https://rss.orf.at/oesterreich.xml',
            'https://rss.orf.at/burgenland.xml',
            'https://rss.orf.at/wien.xml',
            'https://rss.orf.at/kaernten.xml',
            'https://rss.orf.at/noe.xml',
            'https://rss.orf.at/ooe.xml',
            'https://rss.orf.at/vorarlberg.xml',
            'https://rss.orf.at/tirol.xml',
            'https://rss.orf.at/steiermark.xml',
            'https://rss.orf.at/salzburg.xml',
            'https://rss.orf.at/news.xml',
            'https://rss.orf.at/help.xml',
            'https://rss.orf.at/science.xml',
            ]

for rss_url in rss_list:

    # RSS-Feed parsen
    feed = feedparser.parse(rss_url)

    # Dokumente für das Batch-Insert vorbereiten
    for entry in feed.entries:

        if not collection.find_one({"scraping_info.url": entry.link}):
            document = {
                "scraping_info": {
                    "url": entry.link,
                    "status": None,
                    "download_datetime": None
                },
                "article": {
                    "category": entry.get("category", "Unknown")
                }
            }
            documents.append(document)

# Batch-Insert der Dokumente in die MongoDB
if documents:
    collection.insert_many(documents)
    print(f"{len(documents)} Dokumente wurden erfolgreich in die MongoDB eingefügt.")
else:
    print("Keine neuen Dokumente zum Einfügen gefunden.")
