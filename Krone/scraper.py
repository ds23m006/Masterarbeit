import logging
import time
import os
import datetime
import multiprocessing
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from database import get_db_connection
from driver import configure_driver
from logger_setup import setup_logger, close_logger
from parsers import parse_krone_article, parse_krone_comment_section
from utils import scraping_status

def scrape_articles(logger, n=10):
    """
    Liest aus der MongoDB alle URLs, deren status nicht 'success' ist, 
    teilt sie auf n Prozesse auf und ruft 'scrape_articles_chunk' parallel auf.
    """
    krone_collection = get_db_connection('Krone')

    # URLs selektieren
    # (Hier ein Beispiel-Filter: alles != success; 
    #  passe das an deine Logik an, z.B. 'error', None, oder 'skipped')
    urls_to_scrape = list(krone_collection.find(
        {'scraping_info.status': {'$ne': 'success'}},
        {'scraping_info.url': 1}
    ))
    if len(urls_to_scrape) == 0:
        logger.info("Keine neuen oder fehlerhaften URLs zu verarbeiten.")
        return

    logger.info(f"Anzahl der zu scrapenden URLs: {len(urls_to_scrape)}")

    # Gleichmäßig auf n Prozesse verteilen
    chunks = [urls_to_scrape[i::n] for i in range(n)]

    with multiprocessing.Pool(processes=n) as pool:
        pool.map(scrape_articles_chunk, chunks)


def scrape_articles_chunk(urls_chunk):
    """
    Wird in einem Subprozess ausgeführt und verarbeitet eine Teilmenge von URLs.
    """
    pid = os.getpid()
    log_file = f'scraper_krone_{pid}.log'
    logger = setup_logger(log_file=log_file)
    logger.info(f"Prozess {pid} gestartet und verarbeitet {len(urls_chunk)} Artikel.")

    krone_collection = get_db_connection('Krone')
    driver = configure_driver(headless=True)

    try:
        for url_entry in urls_chunk:
            full_url = url_entry['scraping_info']['url']
            logger.info(f"Prozess {pid} verarbeitet URL: {full_url}")

            # Lade die Seite
            try:
                driver.set_page_load_timeout(15)
                driver.get(full_url)
            except TimeoutException:
                scraping_status(krone_collection, "error", full_url, "Timeout nach 15 Sekunden", logger)
                continue
            except Exception as e:
                scraping_status(krone_collection, "error", full_url, f"Fehler: {e}", logger)
                continue

            time.sleep(3)

            # Parsen
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            article_data = parse_krone_article(soup, logger)
            if not article_data.get('article.title') or not article_data.get('article.pubdate'):
                scraping_status(krone_collection, "error", full_url, "Kein Titel oder Datum gefunden", logger)
                continue

            # Kommentare parsen
            forum_comments = parse_krone_comment_section(driver, logger)

            # Weitere Features, z.B. Count der Kommentare
            comments_count = len(forum_comments) if forum_comments else 0

            # Status bestimmen
            if comments_count == 0:
                status = "warning (comments)"
            else:
                status = "success"

            # Zusammenstellen und in DB updaten
            article_data.update({
                'article.comments': forum_comments,
                'features.posting_count': comments_count,
                'scraping_info.status': status,
                'scraping_info.download_datetime': datetime.datetime.now()
            })

            krone_collection.update_one(
                {'scraping_info.url': full_url},
                {'$set': article_data}
            )

            logger.info(f"Scraping OK (Status '{status}') für {full_url}")
    finally:
        driver.quit()
        logger.info("Browser im Subprozess geschlossen.")
        close_logger(logger)
