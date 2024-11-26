import time
from logger_setup import setup_logger
from scraper import scrape_articles

def main(n=8):
    logger = setup_logger()
    while True:
        scrape_articles(logger, n)
        time.sleep(60)

if __name__ == "__main__":
    main()
