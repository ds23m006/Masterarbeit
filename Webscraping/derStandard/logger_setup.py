import logging
import os

def setup_logger(name=__name__, log_file='scraper.log', level=logging.DEBUG):
    # set log file path
    log_file_path = os.path.join('WebScraping/derStandard/logfiles', log_file)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Entferne vorhandene Handler, um doppelte Logs zu vermeiden
    logger.handlers = []

    # Log-Format definieren
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console-Handler hinzufügen
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File-Handler hinzufügen
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def close_logger(logger):
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)
