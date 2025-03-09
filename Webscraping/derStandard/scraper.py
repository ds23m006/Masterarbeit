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
from parsers import (
    get_article_byline,
    get_article_datetime,
    get_posting_count,
    get_paragraph_texts,
    extract_reactions,
    extract_forum_comments_normal,
    extract_forum_comments_alternative,
)
from utils import expand_shadow_element

def scrape_articles(logger, n=10):
    collection = get_db_connection()
    
    # get URLs
    urls_to_scrape = list(collection.find({
        '$and': [
            { 'features.APA_OeNB_Sentiment': { '$exists': True, '$nin': [None, ""] } },
            {'scraping_info.status': {'$nin': ['success']}},
            #{ '$or': [
            #    {'scraping_info.status': {'$nin': ['success']}},
            #    {'scraping_info.status': {'$exists': False}}
            #]}
        ]
    }, {'scraping_info.url': 1}))
    
    if len(urls_to_scrape)==0:
        logger.info(f"Alle Files bereits gescraped... Files mit status=error werden erneut gescraped")
        urls_to_scrape = list(collection.find({'scraping_info.status': 'error'}, {'scraping_info.url': 1}))
    
    logger.info(f"Anzahl der zu scrapenden URLs: {len(urls_to_scrape)}")

    # URLs gleichmäßig auf n Prozesse verteilen
    chunks = [urls_to_scrape[i::n] for i in range(n)]

    # Multiprocessing Pool verwenden
    with multiprocessing.Pool(processes=n) as pool:
        pool.map(scrape_articles_chunk, chunks)

def scrape_articles_chunk(urls_chunk):
    pid = os.getpid()
    log_file = f'scraper_{pid}.log'
    logger = setup_logger(log_file=log_file)
    logger.info(f"Prozess {pid} gestartet und verarbeitet {len(urls_chunk)} Artikel.")

    collection = get_db_connection()
    driver = configure_driver(headless=True)

    try:
        for url_dict in urls_chunk:
            full_url = url_dict['scraping_info']['url']

            # liveticker
            if full_url.startswith("https://www.derstandard.at/jetzt"):
                scraping_status(collection, "skipped", full_url, "Skipping Liveticker", logger)
                continue

            if "kreuzwortraetsel" in full_url:
                scraping_status(collection, "skipped", full_url, "Skipping Kreuzworträtsel", logger)
                continue

            logger.info(f"Prozess {pid} verarbeitet URL: {full_url}")

            try:
                driver.set_page_load_timeout(10)
                # Seite laden
                try:
                    driver.get(full_url)
                except TimeoutException:
                    scraping_status(collection, "error", full_url, "Timeout nach 10 Sekunden", logger)
                    continue

                wait = WebDriverWait(driver, 10)
                time.sleep(5)

                # Warten, bis die Seite geladen ist
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                logger.debug(f"Seite {full_url} vollständig geladen.")

                # Seite mit BeautifulSoup parsen
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                logger.debug(f"HTML-Inhalt von {full_url} mit BeautifulSoup geparst.")

                # Rubrik/Kicker
                kicker_tag = soup.find('h2', class_='article-kicker')
                kicker = kicker_tag.get_text(strip=True) if kicker_tag else None

                # Titel
                title_tag = soup.find('h1', class_='article-title')
                title = title_tag.get_text(strip=True) if title_tag else None

                # Subtitel
                subtitle_tag = soup.find('p', class_='article-subtitle')
                subtitle = subtitle_tag.get_text(strip=True) if subtitle_tag else None

                # Artikel-Byline
                article_byline = get_article_byline(soup, logger)

                # Datum und Uhrzeit
                article_datetime = get_article_datetime(soup, logger)

                if article_datetime is None or title is None:
                    scraping_status(collection, "error", full_url, 'Fehlendes Datum oder Titel', logger)
                    continue

                # Anzahl der Postings
                posting_count = get_posting_count(soup, full_url, logger)

                # Reaktionen
                reactions, reactions_warning = extract_reactions(driver, logger)

                # Artikelinhalt
                paragraph_texts = get_paragraph_texts(soup, full_url, logger)

                # manchmal ist die Seite anders strukturiert
                old_design = soup.find("div", class_="forum use-unobtrusive-ajax visible")

                # Kommentare
                if old_design:
                    forum_comments, comments_warning = extract_forum_comments_alternative(driver, logger)
                else:
                    forum_comments, comments_warning = extract_forum_comments_normal(driver, logger)

                # Status bestimmen
                if reactions_warning and comments_warning:
                    status = 'warning'
                elif reactions_warning:
                    status = 'warning (reactions)'
                elif comments_warning:
                    status = 'warning (comments)'
                else:
                    status = 'success'

                # Daten vorbereiten
                article_data = {
                    'article.title': title,
                    'article.subtitle': subtitle,
                    'article.kicker': kicker,
                    'article.text': paragraph_texts,
                    'article.author': article_byline,
                    'article.pubdate': article_datetime,
                    'article.comments': forum_comments,
                    'features.posting_count': posting_count,
                    'features.reactions': reactions,
                    'scraping_info.status': status,
                    'scraping_info.download_datetime': datetime.datetime.now()
                }

                # Daten in die 'derStandard' Collection einfügen
                collection.update_one(
                    {'scraping_info.url': full_url},
                    {'$set': article_data}
                )

                logger.info(f"Erfolgreich gescraped mit Status '{status}': {full_url} am {article_datetime}")

            except TimeoutException:
                scraping_status(collection, "error", full_url,'Timeout nach 10 Sekunden', logger)
            except Exception as e:
                scraping_status(collection, "error", full_url, str(e), logger)
                logger.error(f"Fehler beim Verarbeiten von {full_url}: {e}", exc_info=True)
                continue
    finally:
        driver.quit()
        logger.info("Browser erfolgreich geschlossen.")
        close_logger(logger)



def scraping_status(collection, status, url, exception_message, logger):
    collection.update_one(
        {'scraping_info.url': url},
        {
            '$set': {
                'scraping_info.status': status,
                'scraping_info.download_datetime': datetime.datetime.now()
            }
        }
    )
    logger.warning(f"{exception_message}: {url}")
