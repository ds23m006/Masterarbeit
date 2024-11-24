import feedparser
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def get_db_connection(collection):
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']
    collection = db[collection]
    return collection

def get_orf_entry_info(entry):
    url = entry.link
    document = {
        "scraping_info": {
            "url": url,
            "status": None,
            "download_datetime": None
        },
        "article": {
            "category": entry.get("category", "Unknown")
        }
    }
    return {"url": url, "document": document}

def get_derstandard_entry_info(entry):
    if hasattr(entry, 'id_detail') and 'attributes' in entry.id_detail:
        if entry.id_detail.attributes.get('isPermaLink', 'false') == 'true':
            url = entry.id
            document = {
                "scraping_info": {
                    "url": url,
                    "status": None,
                    "download_datetime": None
                },
                "article": {
                    "title": entry.get("title", "No Title")
                }
            }
            return {"url": url, "document": document}
    return None

def process_feeds(rss_list, collection_name, get_entry_info):
    collection = get_db_connection(collection=collection_name)
    documents = []
    processed_urls = set()
    for rss_url in rss_list:
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            entry_info = get_entry_info(entry)
            if entry_info:
                url = entry_info['url']
                if url in processed_urls:
                    continue
                processed_urls.add(url)
                if not collection.find_one({"scraping_info.url": url}):
                    documents.append(entry_info['document'])
    if documents:
        collection.insert_many(documents)
        print(f"{len(documents)} Dokumente wurden erfolgreich in die Collection '{collection_name}' eingefügt.")
    else:
        print(f"Keine neuen Dokumente zum Einfügen für '{collection_name}' gefunden.")

# ORF RSS Feeds
orf_rss_list = [
    'https://rss.orf.at/oesterreich.xml',
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

# derStandard RSS Feeds
derstandard_rss_list = [
    'https://www.derstandard.at/rss',
    'https://www.derstandard.at/rss/international',
    'https://www.derstandard.at/rss/inland',
    'https://www.derstandard.at/rss/wirtschaft',
    'https://www.derstandard.at/rss/web',
    'https://www.derstandard.at/rss/sport',
    'https://www.derstandard.at/rss/panorama',
    'https://www.derstandard.at/rss/etat',
    'https://www.derstandard.at/rss/kultur',
    'https://www.derstandard.at/rss/wissenschaft',
    'https://www.derstandard.at/rss/gesundheit',
    'https://www.derstandard.at/rss/lifestyle',
    'https://www.derstandard.at/rss/karriere',
    'https://www.derstandard.at/rss/immobilien',
    'https://www.derstandard.at/rss/diskurs',
    'https://www.derstandard.at/rss/diestandard',
    'https://www.derstandard.at/rss/recht',
]

# Verarbeitung der ORF Feeds
process_feeds(orf_rss_list, collection_name='ORF', get_entry_info=get_orf_entry_info)

# Verarbeitung der derStandard Feeds
process_feeds(derstandard_rss_list, collection_name='derStandard', get_entry_info=get_derstandard_entry_info)
