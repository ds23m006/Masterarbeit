import requests
from xml.etree import ElementTree
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

# Logger Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_db_connection(collection):
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']
    return db[collection]

def fetch_sitemaps(sitemap_index_url):
    """Fetch sitemaps from the sitemap index starting from 2020."""
    response = requests.get(sitemap_index_url)
    if response.status_code != 200:
        logger.error(f"Failed to fetch sitemap index: {sitemap_index_url}")
        return []

    root = ElementTree.fromstring(response.content)
    sitemaps = []

    for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
        loc = elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        lastmod = elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")

        if loc is not None and lastmod is not None:
            lastmod_date = datetime.fromisoformat(lastmod.text[:-6])  # Strip timezone
            if lastmod_date.year >= 2020:
                sitemaps.append(loc.text)

    return sitemaps

def fetch_sitemap_urls(sitemap_url):
    """Fetch URLs from the given sitemap."""
    response = requests.get(sitemap_url)
    if response.status_code != 200:
        logger.error(f"Failed to fetch sitemap: {sitemap_url}")
        return []

    root = ElementTree.fromstring(response.content)
    urls = []
    for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
        loc = elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if loc is not None:
            urls.append(loc.text)
    return urls

def process_sitemaps_from_2020(sitemap_index_url, collection_name):
    collection = get_db_connection(collection_name)
    sitemaps = fetch_sitemaps(sitemap_index_url)

    if not sitemaps:
        logger.error("No sitemaps found from 2020 onwards.")
        return

    total_new_documents = 0

    for sitemap_url in sitemaps:
        logger.info(f"Processing sitemap: {sitemap_url}")
        urls = fetch_sitemap_urls(sitemap_url)

        if not urls:
            logger.info(f"No URLs found in sitemap: {sitemap_url}")
            continue

        # Check for existing URLs in the database
        existing_urls = set(doc["scraping_info"]["url"] for doc in collection.find({}, {"scraping_info.url": 1}))
        new_documents = [{
            "scraping_info": {
                "url": url,
                "status": None,
                "download_datetime": None
            }
        } for url in urls if url not in existing_urls]

        if new_documents:
            collection.insert_many(new_documents)
            total_new_documents += len(new_documents)
            logger.info(f"Inserted {len(new_documents)} new documents from sitemap: {sitemap_url}")
        else:
            logger.info(f"No new documents to add from sitemap: {sitemap_url}")

    logger.info(f"Total new documents added: {total_new_documents}")

def main():
    krone_sitemap_index = "https://www.krone.at/sitemap-articles.xml"
    process_sitemaps_from_2020(krone_sitemap_index, collection_name="Krone")

if __name__ == "__main__":
    main()
