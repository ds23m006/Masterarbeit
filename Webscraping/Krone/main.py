import time
from logger_setup import setup_logger
from scraper import scrape_articles

def main(n=4):
    """
    n = Anzahl der parallelen Prozesse
    """
    logger = setup_logger(log_file='krone_scraper_main.log')
    while True:
        scrape_articles(logger, n)
        time.sleep(60)

if __name__ == "__main__":
    main()