# main.py
from logger_setup import setup_logger, close_logger
from database import get_db_connection
from feature_engineering import run_basic_feature_engineering
from keyword_extraction import run_keyword_extraction

def main():
    # Logger initialisieren
    logger = setup_logger(log_file='feature_engineering_main.log')
    logger.info("Feature Engineering Prozess gestartet.")

    try:
        # Verbindung zur MongoDB herstellen
        conn = get_db_connection(collection='derStandard')

        # 1. Basic Feature Engineering ausführen
        logger.info("Starte Basic Feature Engineering...")
        run_basic_feature_engineering(conn, batch_size=1000)

        # 2. Keyword Extraction und Autor-Artikel-Zählung ausführen
        logger.info("Starte Keyword Extraction und Autor-Artikel-Zählung...")
        run_keyword_extraction(conn, batch_size=1000)

        logger.info("Feature Engineering Prozess abgeschlossen.")
    except Exception as e:
        logger.error(f"Fehler im Hauptprozess: {e}", exc_info=True)
    finally:
        close_logger(logger)

if __name__ == "__main__":
    main()
