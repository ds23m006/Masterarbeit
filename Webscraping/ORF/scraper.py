import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
import os
import logging

# Logger konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection(collection_name='ORF'):
    """
    Stellt die Verbindung zur MongoDB-Datenbank her und gibt die gewünschte Collection zurück.
    Die Umgebungsvariablen MONGODB_USER und MONGODB_PWD müssen gesetzt sein.
    """
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    if not USERNAME or not PASSWORD:
        logger.error("Umgebungsvariablen MONGODB_USER und/oder MONGODB_PWD nicht gesetzt.")
        raise EnvironmentError("MONGODB_USER und MONGODB_PWD müssen als Umgebungsvariablen gesetzt sein.")
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']
    collection = db[collection_name]
    return collection

def scrape_article(html_content):
    """
    Hauptfunktion zum Scrapen von ORF-Artikeln (alte Version).
    Extrahiert Titel, Untertitel, Autor, Veröffentlichungsdatum und Artikeltext.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Artikel-Div in der alten Struktur
    article = soup.find("div", id="ss-shunter")
    if not article:
        logger.warning("Hauptfunktion: Artikelinhalt nicht gefunden.")
        return None

    # Titel
    title_tag = article.find('h1', class_='story-lead-headline')
    title = title_tag.get_text(strip=True) if title_tag else None

    # Untertitel
    subtitle_tag = article.find('p', class_='story-lead-text')
    if subtitle_tag:
        strong_tag = subtitle_tag.find('strong')
        subtitle = strong_tag.get_text(strip=True) if strong_tag else subtitle_tag.get_text(strip=True)
    else:
        subtitle = None

    # Autor (Byline)
    byline_tag = article.find('div', class_='byline')
    autor = byline_tag.get_text(strip=True) if byline_tag else None

    # Veröffentlichungsdatum
    pubdate = None
    pubdate_div = article.find('div', class_='story-meta-dates')
    if pubdate_div:
        print_only_div = pubdate_div.find('div', attrs={"aria-hidden": "true", "class": "print-only"})
        if print_only_div:
            pubdate_str = print_only_div.get_text(strip=True)
            try:
                pubdate = datetime.strptime(pubdate_str, "%d.%m.%Y %H.%M")
            except ValueError:
                logger.error(f"Hauptfunktion: Fehler beim Parsen des Datums: {pubdate_str}")
                pubdate = None

    # Artikeltext: Liste von Paragraphen
    text_paragraphs = []
    story_content = article.find('div', class_='story-story')
    if story_content:
        paragraphs = story_content.find_all('p')
        text_paragraphs = [p.get_text(strip=True) for p in paragraphs]

    article_data = {
        'autor': autor,
        'pubdate': pubdate,
        'title': title,
        'subtitle': subtitle,
        'text': text_paragraphs
    }
    return article_data

def scrape_article_alternative(html_content):
    """
    Alternative Scraping-Funktion für die neue Version der Seite.
    Extrahiert dieselben Felder wie die Hauptfunktion:
    - Titel, Untertitel, Autor, Veröffentlichungsdatum und Artikeltext.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Artikel-Container in der neuen Struktur
    article = soup.find("div", id="ss-storyText")
    if not article:
        logger.warning("Alternative: Artikelinhalt nicht gefunden.")
        return None

    # Titel
    title_tag = article.find('h1')
    title = title_tag.get_text(strip=True) if title_tag else None

    # Untertitel
    subtitle_tag = article.find('p', class_='teaser')
    if subtitle_tag:
        strong_tag = subtitle_tag.find('strong')
        subtitle = strong_tag.get_text(strip=True) if strong_tag else subtitle_tag.get_text(strip=True)
    else:
        subtitle = None

    # Autor: Alternative Version liefert keinen Autor, daher None
    autor = None

    # Veröffentlichungsdatum
    pubdate = None
    pubdate_tag = article.find('p', class_='date')
    if pubdate_tag:
        date_text = pubdate_tag.get_text(strip=True)
        if date_text.startswith("Publiziert am"):
            date_text = date_text.replace("Publiziert am", "").strip()
        try:
            pubdate = datetime.strptime(date_text, "%d.%m.%Y")
        except ValueError:
            logger.error(f"Alternative: Fehler beim Parsen des Datums: {date_text}")
            pubdate = None

    # Artikeltext: Alle Paragraphen (ohne "teaser" und "date")
    text_paragraphs = []
    paragraphs = article.find_all('p')
    for p in paragraphs:
        if p.has_attr("class"):
            if "teaser" in p["class"] or "date" in p["class"]:
                continue
        text = p.get_text(strip=True)
        if text:
            text_paragraphs.append(text)

    article_data = {
        'autor': autor,
        'pubdate': pubdate,
        'title': title,
        'subtitle': subtitle,
        'text': text_paragraphs
    }
    return article_data

async def fetch(session, url, collection):
    """
    Asynchrone Funktion, um den HTML-Inhalt einer URL abzurufen,
    mittels der Haupt- bzw. alternativen Scraping-Funktion auszuwerten
    und das Ergebnis in der MongoDB zu speichern.
    """
    try:
        async with session.get(url) as response:
            if response.status == 200:
                html_content = await response.text()

                # Zuerst die Haupt-Scraping-Funktion verwenden
                article_data = scrape_article(html_content)
                if not article_data:
                    logger.warning(f"Scraping fehlgeschlagen für {url} mit Hauptfunktion, versuche alternative Methode.")
                    article_data = scrape_article_alternative(html_content)

                if article_data:
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
                    collection.update_one(
                        {'scraping_info.url': url},
                        {
                            '$set': {
                                'scraping_info.status': 'error',
                                'scraping_info.download_datetime': datetime.now()
                            }
                        }
                    )
                    logger.error(f"Scraping fehlgeschlagen für {url}, auch alternative Methode ohne Erfolg.")
            else:
                collection.update_one(
                    {'scraping_info.url': url},
                    {
                        '$set': {
                            'scraping_info.status': 'error',
                            'scraping_info.download_datetime': datetime.now()
                        }
                    }
                )
                logger.error(f"Fehler beim Abrufen von {url}: HTTP {response.status}")
    except Exception as e:
        collection.update_one(
            {'scraping_info.url': url},
            {
                '$set': {
                    'scraping_info.status': 'error',
                    'scraping_info.download_datetime': datetime.now()
                }
            }
        )
        logger.error(f"Fehler bei {url}: {e}")

async def fetch_with_semaphore(semaphore, session, url, collection):
    async with semaphore:
        await fetch(session, url, collection)

async def main():
    # Verbindung zur MongoDB herstellen
    collection = get_db_connection('ORF')

    # Abrufen der URLs, die noch nicht gescraped wurden
    urls_to_scrape = list(collection.find({
        '$or': [
            {'scraping_info.status': {'$in': ['', None, 'error']}},
            {'scraping_info.status': {'$exists': False}}
        ]
    }, {'scraping_info.url': 1}))
    urls = [doc['scraping_info']['url'] for doc in urls_to_scrape if 'scraping_info' in doc and 'url' in doc['scraping_info']]

    if not urls:
        logger.info("Keine URLs zum Scrapen gefunden.")
        return

    logger.info(f"{len(urls)} URLs zum Scrapen gefunden.")

    max_conns = 20
    semaphore = asyncio.Semaphore(max_conns)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_semaphore(semaphore, session, url, collection) for url in urls]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
