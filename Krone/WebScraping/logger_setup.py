import logging
import os

def setup_logger(name=__name__, log_file='scraper.log', level=logging.DEBUG):
    """
    Initialisiert den Logger für Krone. Schreibt Logdateien in krone/logfiles.
    """
    log_folder = 'Krone/Webscraping/logfiles'
    os.makedirs(log_folder, exist_ok=True)
    log_file_path = os.path.join(log_folder, log_file)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Doppelte Handler entfernen
    logger.handlers = []

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console-Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File-Handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def close_logger(logger):
    """
    Schließt alle Handler. Erlaubt eine saubere Beendigung ohne Datei-Locks.
    """
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)
