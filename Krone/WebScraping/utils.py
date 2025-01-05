import datetime

def scraping_status(collection, status, url, exception_message, logger):
    """
    Schreibt den Status in die DB und loggt eine Warnung.
    """
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
