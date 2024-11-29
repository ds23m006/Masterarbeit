import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
import os
import logging

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB Verbindung
def get_db_connection(collection_name='ORF'):
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']
    collection = db[collection_name]
    return collection


def scrape_article(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Artikel-Div
    article = soup.find("div", id="ss-shunter")
    if not article:
        logger.warning("Artikelinhalt nicht gefunden.")
        return None

    # Titel
    title_tag = article.find('h1', class_='story-lead-headline')
    title = title_tag.get_text(strip=True) if title_tag else None

    # Untertitel
    subtitle_tag = article.find('p', class_='story-lead-text')
    if subtitle_tag:
        # Manchmal ist der Untertitel innerhalb eines <strong>-Tags
        strong_tag = subtitle_tag.find('strong')
        subtitle = strong_tag.get_text(strip=True) if strong_tag else subtitle_tag.get_text(strip=True)
    else:
        subtitle = None

    # Autor (Byline)
    byline_tag = article.find('div', class_='byline')
    autor = byline_tag.get_text(strip=True) if byline_tag else None

    # Pubdate
    pubdate = None
    pubdate_div = article.find('div', class_='story-meta-dates')
    if pubdate_div:
        print_only_div = pubdate_div.find('div', attrs={"aria-hidden": "true", "class": "print-only"})
        if print_only_div:
            pubdate_str = print_only_div.get_text(strip=True)
            try:
                pubdate = datetime.strptime(pubdate_str, "%d.%m.%Y %H.%M")
            except ValueError:
                logger.error(f"Fehler beim Parsen des Datums: {pubdate_str}")
                pubdate = None

    # Text (Liste von Paragraphen)
    text_paragraphs = []
    story_content = article.find('div', class_='story-story')
    if story_content:
        paragraphs = story_content.find_all('p')
        text_paragraphs = [p.get_text() for p in paragraphs]

    # Document erstellen
    article_data = {
        'autor': autor,
        'pubdate': pubdate,
        'title': title,
        'subtitle': subtitle,
        'text': text_paragraphs
    }

    return article_data

# Asynchrone Fetch-Funktion
async def fetch(session, url, collection):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                html_content = await response.text()
                article_data = scrape_article(html_content)
                if article_data:
                    # Update das Dokument
                    result = collection.update_one(
                        {'scraping_info.url': url},
                        {
                            '$set': {
                                'article': article_data,
                                'scraping_info.status': 'success',
                                'scraping_info.download_datetime': datetime.now()
                            }
                        }
                    )
                    if result.modified_count > 0:
                        logger.info(f"Artikel erfolgreich gescraped und aktualisiert: {url}")
                    else:
                        logger.warning(f"Dokument nicht gefunden oder nicht aktualisiert: {url}")
                else:
                    # scraping error
                    collection.update_one(
                        {'scraping_info.url': url},
                        {
                            '$set': {
                                'scraping_info.status': 'error',
                                'scraping_info.download_datetime': datetime.now()
                            }
                        }
                    )
                    logger.error(f"Scraping fehlgeschlagen für {url}")
                return
            else:
                # Update den Status
                collection.update_one(
                    {'scraping_info.url': url},
                    {
                        '$set': {
                            'scraping_info.status': f'error',
                            'scraping_info.download_datetime': datetime.now()
                        }
                    }
                )
                logger.error(f"Fehler beim Abrufen von {url}: HTTP {response.status}")
                return
    except Exception as e:
        # Update den Status bei anderen Fehlern
        collection.update_one(
            {'scraping_info.url': url},
            {
                '$set': {
                    'scraping_info.status': f'error',
                    'scraping_info.download_datetime': datetime.now()
                }
            }
        )
        logger.error(f"Fehler bei {url}: {e}")
        return


# Hauptfunktion
async def main():
    # Verbindung zur MongoDB herstellen
    collection = get_db_connection('ORF')

    # get URLs
    urls_to_scrape = list(collection.find({
        '$or': [
            {'scraping_info.status': {'$in': ['', None]}},
            {'scraping_info.status': {'$exists': False}}
        ]
        }, {'scraping_info.url': 1}))
    urls = [doc['scraping_info']['url'] for doc in urls_to_scrape if 'scraping_info' in doc and 'url' in doc['scraping_info']]

    if not urls:
        logger.info("Keine URLs zum Scrapen gefunden.")
        return
    logger.info(f"{len(urls)} URLs zum Scrapen gefunden.")

    # Anzahl gleichzeitiger Verbindungen begrenzen
    max_conns=20
    semaphore = asyncio.Semaphore(max_conns)

    # Asynchrone HTTP-Session
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_semaphore(semaphore, session, url, collection) for url in urls]
        await asyncio.gather(*tasks)


# Hilfsfunktion Semaphores
async def fetch_with_semaphore(semaphore, session, url, collection):
    async with semaphore:
        await fetch(session, url, collection)


# Main Funktion
if __name__ == "__main__":
    asyncio.run(main())
