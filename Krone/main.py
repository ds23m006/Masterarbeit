import time
from logger_setup import setup_logger
from scraper import scrape_articles

def main(n=3):
    """
    Startpunkt des Krone-Scrapers.
    n = Anzahl der parallelen Prozesse (z.B. 10).
    """
    logger = setup_logger(log_file='krone_scraper_main.log')
    while True:
        scrape_articles(logger, n)
        time.sleep(60)  # Warte eine Minute, bevor erneut gescraped wird

if __name__ == "__main__":
    main()
