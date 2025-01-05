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
    Liest aus der MongoDB alle URLs, deren Status != 'success' ist,
    teilt sie auf n Prozesse auf und ruft 'scrape_articles_chunk' parallel auf.
    """
    krone_collection = get_db_connection('Krone')

    # URLs selektieren
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

            # 1) Seite laden
            try:
                driver.set_page_load_timeout(20)
                driver.get(full_url)
            except TimeoutException:
                scraping_status(krone_collection, "error", full_url, "Timeout nach 20 Sekunden", logger)
                continue
            except Exception as e:
                scraping_status(krone_collection, "error", full_url, f"Fehler beim Laden: {e}", logger)
                logger.error(f"Fehler beim Laden der Seite: {e}", exc_info=True)
                continue

            time.sleep(3)

            # 2) HTML parsen (BeautifulSoup)
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # 3) Artikel parsen (Titel, Kicker, Autor, Premium etc.)
            try:
                article_data = parse_krone_article(soup, logger)
            except Exception as e:
                scraping_status(krone_collection, "error", full_url, f"Fehler beim Artikel-Parsing: {e}", logger)
                logger.error(f"Fehler beim Artikel-Parsing: {e}", exc_info=True)
                continue

            # 4) Posting Count ermitteln (Kommentaranzahl)
            try:
                posting_count_elem = soup.find('span', class_='stb__comment-count js-krn-comments-count')
                if posting_count_elem:
                    posting_count = int(posting_count_elem.text.strip())
                else:
                    posting_count = 0
            except Exception as e:
                logger.warning(f"Fehler beim Auslesen des posting_count: {e}", exc_info=True)
                posting_count = 0

            # posting_count im Artikel-Dict speichern
            article_data['features.posting_count'] = posting_count

            # 5) Kommentare parsen, nur wenn posting_count > 0 und kein Premium-Artikel
            if posting_count > 0 and not article_data.get('features.premium'):
                try:
                    forum_comments = parse_krone_comment_section(driver, logger)
                except Exception as e:
                    scraping_status(krone_collection, "warning (comments)", full_url, f"Fehler beim Kommentar-Parsing: {e}", logger)
                    logger.error(f"Fehler beim Kommentar-Parsing: {e}", exc_info=True)
                    forum_comments = []
                comments_count = len(forum_comments)
            else:
                # Entweder 0 Kommentare oder Premium => kein Kommentar-Parsing
                forum_comments = []
                comments_count = 0
                if posting_count == 0:
                    logger.info("Posting Count = 0 -> Keine Kommentare zum Parsen.")
                elif article_data.get('features.premium'):
                    logger.info("Premium-Artikel -> Keine Kommentare zugänglich.")

            # 6) Status bestimmen
            if not article_data.get('article.title') or not article_data.get('article.pubdate'):
                status = "error"
            else:
                status = "success"

            # 7) DB-Update
            article_data.update({
                'article.comments': forum_comments,
                'scraping_info.status': status,
                'scraping_info.download_datetime': datetime.datetime.now()
            })

            krone_collection.update_one(
                {'scraping_info.url': full_url},
                {'$set': article_data}
            )

            logger.info(f"Scraping abgeschlossen (Status '{status}') für {full_url}")
    finally:
        driver.quit()
        logger.info(f"Prozess {pid}: Browser geschlossen.")
        close_logger(logger)

