import feedparser
import requests
from xml.etree import ElementTree
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import sys
from datetime import datetime

# Load environment variables
load_dotenv()

def get_db_connection(collection):
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']
    return db[collection]

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
    url = entry.link
    document = {
        "scraping_info": {
            "url": url,
            "status": None,
            "download_datetime": None
        }
    }
    return {"url": url, "document": document}

def get_kurier_entry_info(entry):
    url = entry.link
    document = {
        "scraping_info": {
            "url": url,
            "status": None,
            "download_datetime": None
        }
    }
    return {"url": url, "document": document}

def get_krone_entry_info(url):
    document = {
        "scraping_info": {
            "url": url,
            "status": None,
            "download_datetime": None
        }
    }
    return {"url": url, "document": document}

def process_feeds(rss_list, collection_name, get_entry_info):
    collection = get_db_connection(collection=collection_name)
    entry_info_list = []
    processed_urls = set()

    # Sammeln aller Einträge
    for rss_url in rss_list:
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            entry_info = get_entry_info(entry)
            if entry_info:
                url = entry_info['url']
                if url in processed_urls:
                    continue
                processed_urls.add(url)
                entry_info_list.append(entry_info)

    if not entry_info_list:
        print(f"Keine neuen Dokumente zum Einfügen für '{collection_name}' gefunden.")
        return

    # Abfrage, um vorhandene URLs zu ermitteln
    urls = [entry_info['url'] for entry_info in entry_info_list]
    existing_urls = set()
    if urls:
        existing_docs = collection.find({"scraping_info.url": {"$in": urls}}, {"scraping_info.url": 1})
        existing_urls = set(doc["scraping_info"]["url"] for doc in existing_docs)

    # Filtern der neuen Dokumente
    new_documents = [entry_info['document'] for entry_info in entry_info_list if entry_info['url'] not in existing_urls]

    if new_documents:
        collection.insert_many(new_documents)
        print(f"{len(new_documents)} Dokumente wurden erfolgreich in die Collection '{collection_name}' eingefügt.")
    else:
        print(f"Keine neuen Dokumente zum Einfügen für '{collection_name}' gefunden.")

def fetch_sitemaps(sitemap_index_url, all=False):
    """Fetch sitemaps from the sitemap index. Defaults to the latest sitemap unless all=True."""
    response = requests.get(sitemap_index_url)
    if response.status_code != 200:
        return []

    root = ElementTree.fromstring(response.content)
    sitemaps = []

    for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
        loc = elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if loc is not None:
            sitemaps.append(loc.text)

    if not all:
        # Return only the latest sitemap
        return [sitemaps[-1]] if sitemaps else []

    return sitemaps

def fetch_sitemap_urls(sitemap_url):
    """Fetch URLs from the given sitemap."""
    response = requests.get(sitemap_url)
    if response.status_code != 200:
        return []

    root = ElementTree.fromstring(response.content)
    urls = []
    for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
        loc = elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if loc is not None:
            urls.append(loc.text)
    return urls

def process_sitemaps(sitemap_list, collection_name):
    collection = get_db_connection(collection=collection_name)
    all_urls = []

    # Sammeln aller URLs aus den Sitemaps
    for sitemap_url in sitemap_list:
        urls = fetch_sitemap_urls(sitemap_url)
        all_urls.extend(urls)

    if not all_urls:
        print(f"Keine neuen Dokumente zum Einfügen für '{collection_name}' gefunden.")
        return

    # Abfrage, um vorhandene URLs zu ermitteln
    existing_urls = set(doc["scraping_info"]["url"] for doc in collection.find({}, {"scraping_info.url": 1}))
    new_documents = [get_krone_entry_info(url)['document'] for url in all_urls if url not in existing_urls]

    if new_documents:
        collection.insert_many(new_documents)
        print(f"{len(new_documents)} Dokumente wurden erfolgreich in die Collection '{collection_name}' eingefügt.")
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

# Kurier RSS
kurier_rss_list = ['https://kurier.at/xml/rss']

# Krone Sitemaps
krone_sitemap_index = "https://www.krone.at/sitemap-articles.xml"
krone_sitemap_list = fetch_sitemaps(krone_sitemap_index)

feeds_to_process = sys.argv[1:]  # Liste der Argumente nach dem Skriptnamen

if not feeds_to_process:
    feeds_to_process = ['ORF', 'derStandard', 'Kurier', 'Krone']

# Verarbeitung basierend auf den Argumenten
if 'ORF' in feeds_to_process:
    process_feeds(orf_rss_list, collection_name='ORF', get_entry_info=get_orf_entry_info)

if 'derStandard' in feeds_to_process:
    process_feeds(derstandard_rss_list, collection_name='derStandard', get_entry_info=get_derstandard_entry_info)

if 'Kurier' in feeds_to_process:
    process_feeds(kurier_rss_list, collection_name='Kurier', get_entry_info=get_kurier_entry_info)

if 'Krone' in feeds_to_process:
    process_sitemaps(krone_sitemap_list, collection_name='Krone')
