# main.py
from logger_setup import setup_logger, close_logger
from database import get_db_connection
from feature_engineering import run_basic_feature_engineering
from keyword_extraction import run_keyword_extraction
from sentiment_analysis import run_sentiment_analysis

def process_collection(collection_name, logger, batch_size=1000):
    """
    Führt Feature Engineering, Keyword Extraction
    UND Sentiment-Analyse für eine gegebene Collection durch.
    """
    logger.info(f"Starte Verarbeitung der Collection '{collection_name}'...")
    try:
        # Verbindung zur spezifischen Collection herstellen
        conn = get_db_connection(collection=collection_name)
        
        # 1. Basic Feature Engineering ausführen
        logger.info(f"Starte Basic Feature Engineering für '{collection_name}'...")
        run_basic_feature_engineering(conn, batch_size=batch_size)
        
        # 2. Keyword Extraction und Autor-Artikel-Zählung ausführen
        logger.info(f"Starte Keyword Extraction und Autor-Artikel-Zählung für '{collection_name}'...")
        run_keyword_extraction(conn, batch_size=batch_size)

        # 3. Sentiment Analysis pro Paragraph
        logger.info(f"Starte Sentiment-Analyse für '{collection_name}'...")
        run_sentiment_analysis(conn, logger, batch_size=batch_size)

        logger.info(f"Feature Engineering + Sentiment für '{collection_name}' abgeschlossen.")
    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten der Collection '{collection_name}': {e}", exc_info=True)

def main():
    # Logger initialisieren
    logger = setup_logger(log_file='feature_engineering_main.log')
    logger.info("Feature Engineering Prozess gestartet.")
    
    # Liste der zu verarbeitenden Collections
    collections = ['Krone', 'derStandard', 'ORF']
    
    try:
        for collection_name in collections:
            process_collection(collection_name, logger, batch_size=1000)
        
        logger.info("Feature Engineering Prozess abgeschlossen.")
    except Exception as e:
        logger.error(f"Ungefangener Fehler im Hauptprozess: {e}", exc_info=True)
    finally:
        close_logger(logger)

if __name__ == "__main__":
    main()
