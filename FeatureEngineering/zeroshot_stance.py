import logging
from transformers import AutoTokenizer, pipeline
from database import get_db_connection

# Logging konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Definition der Experimente ---
experiments = {
    "experiment1": {
        # Experiment 1: Erweiterte Kandidaten-Labels inkl. alternativer Schreibweisen (z. B. OeNB)
        "candidate_labels": [
            "überwiegend positiv - Oesterreichische Nationalbank OeNB",
            "überwiegend negativ - Oesterreichische Nationalbank OeNB",
            "überwiegend neutral - Oesterreichliche Nationalbank OeNB",
            "überwiegend positiv",
            "überwiegend negativ",
            "überwiegend neutral",
            "Sonstiges"
        ],
        "hypothesis_template": "Die Stimmung des Artikels ist {}."
    },
    "experiment2": {
        # Experiment 2: Kandidaten-Labels mit explizitem OeNB-Bezug
        "candidate_labels": [
            "Positivität bezüglich der österreichliche Nationalbank OeNB",
            "Negativität bezüglich der österreichliche Nationalbank OeNB",
            "Neutralität bezüglich der österreichliche Nationalbank OeNB",
            "Positivität",
            "Negativität",
            "Neutralität",
            "Sonstiges"
        ],
        "hypothesis_template": "Dieser Artikel drückt hauptsächlich {} aus."
    },
    "experiment3": {
        "candidate_labels": [
            "positiv",
            "negativ",
            "neutral",
            "nicht vorhanden"
        ],
        "hypothesis_template": "Die Haltung zur Oesterreichischen Nationalbank (OeNB) ist überwiegend {}."
    }
}

# --- Initialisierung des Classifiers ---
def initialize_classifier():
    """
    Zero-Shot Klassifikations-Pipeline mit "joeddav/xlm-roberta-large-xnli".
    """
    tokenizer = AutoTokenizer.from_pretrained("joeddav/xlm-roberta-large-xnli")
    classifier = pipeline("zero-shot-classification",
                          model="joeddav/xlm-roberta-large-xnli",
                          tokenizer=tokenizer)
    return classifier

# --- Funktion zur Verarbeitung eines Dokuments ---
def process_document(doc, classifier):
    """
    Führt alle definierten Experimente (Zero-Shot Klassifikation) auf den Artikeltext
    eines Dokuments aus, sofern ein APA_sentiment vorhanden ist.
    
    :param doc: Ein Dokument aus der Datenbank.
    :param classifier: Die initialisierte Zero-Shot Klassifikations-Pipeline.
    :return: Ein Dictionary, das für jedes Experiment das Mapping {Label: Score} enthält.
    """
    # Prüfe, ob im Dokument ein APA_sentiment vorhanden ist.
    if not doc.get("features", {}).get("APA_sentiment"):
        return None  # Überspringe das Dokument, wenn kein APA_sentiment vorhanden ist.
    
    # Artikeltext extrahieren (bei article.text handelt es sich um ein Array von Strings)
    article = doc.get("article", {})
    text_list = article.get("text", [])
    if not text_list:
        return None  # Überspringe, wenn kein Artikeltext vorhanden ist.
    
    article_text = " ".join(text_list).strip()
    if not article_text:
        return None

    # Ergebnisse für alle Experimente sammeln
    results = {}
    for exp_name, exp_config in experiments.items():
        candidate_labels = exp_config["candidate_labels"]
        hypothesis_template = exp_config["hypothesis_template"]
        try:
            output = classifier(article_text, candidate_labels, hypothesis_template=hypothesis_template)
            # Erstelle ein Dictionary: {Label: Score}
            exp_result = {label: score for label, score in zip(output["labels"], output["scores"])}
            results[exp_name] = exp_result
        except Exception as e:
            logger.error(f"Fehler im Experiment {exp_name} für Dokument mit _id {doc.get('_id')}: {e}", exc_info=True)
            results[exp_name] = None
    return results


# --- Hauptfunktion ---
def main():
    # Liste der Collections, die verarbeitet werden sollen
    collection_names = ["Krone", "Kurier", "ORF", "OTS", "derStandard"]
    
    # Zero-Shot Klassifikations-Pipeline initialisieren
    classifier = initialize_classifier()
    logger.info("Zero-Shot Klassifikations-Pipeline initialisiert.")
    
    total_processed = 0
    # Für jede Collection wird der Prozess durchgeführt
    for coll_name in collection_names:
        collection = get_db_connection(collection=coll_name)
        logger.info(f"Verbindung zur Collection '{coll_name}' erfolgreich hergestellt.")
        
        # Alle Dokumente auswählen, in denen ein APA_sentiment vorhanden ist.
        query = {"features.APA_sentiment": {"$exists": True}}
        docs_cursor = collection.find(query)
        
        processed_count = 0
        for doc in docs_cursor:
            # Falls das Dokument bereits ein ZeroShotStanceDetection-Feld besitzt, überspringen wir die Verarbeitung
            if doc.get("features", {}).get("ZeroShotStanceDetection") is not None:
                logger.info(f"Dokument {str(doc['_id'])} in Collection '{coll_name}' übersprungen, da ZeroShotStanceDetection bereits existiert.")
                continue

            stance_results = process_document(doc, classifier)
            if stance_results is None:
                continue  # Falls kein Artikeltext oder APA_sentiment vorhanden ist
            # Speichern der Ergebnisse im Feld features.ZeroShotStanceDetection
            update_field = {"features.ZeroShotStanceDetection": stance_results}
            collection.update_one({"_id": doc["_id"]}, {"$set": update_field})
            processed_count += 1
            logger.info(f"Dokument {str(doc['_id'])} in Collection '{coll_name}' aktualisiert.")
        
        logger.info(f"Verarbeitung in Collection '{coll_name}' abgeschlossen: {processed_count} Dokument(e) aktualisiert.")
        total_processed += processed_count
    
    logger.info(f"Gesamtverarbeitung abgeschlossen: {total_processed} Dokument(e) in allen Collections aktualisiert.")


if __name__ == "__main__":
    main()
