# logger_setup.py
import logging
import os

def setup_logger(name=__name__, log_file='feature_engineering.log', level=logging.DEBUG):
    """
    Erstellt und konfiguriert einen Logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Verhindert doppelte Logs, falls der Logger mehrfach konfiguriert wird
    if not logger.handlers:
        # Log-Verzeichnis erstellen
        log_folder = 'logs'
        os.makedirs(log_folder, exist_ok=True)
        log_path = os.path.join(log_folder, log_file)

        # Formatter definieren
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Console-Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # File-Handler
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

def close_logger(logger):
    """
    Schließt alle Handler des Loggers.
    """
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)
