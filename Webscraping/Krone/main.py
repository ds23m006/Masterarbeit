import time
from logger_setup import setup_logger
from scraper import scrape_articles

def main(n=8):
    """
    n = Anzahl der parallelen Prozesse
    """
    logger = setup_logger(log_file='krone_scraper_main.log')
    while True:
        scrape_articles(logger, n)
        time.sleep(60)  # Warte eine Minute, bevor erneut gescraped wird

if __name__ == "__main__":
    main()