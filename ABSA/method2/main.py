import os
import sys
import logging
sys.path.append(r"Z:\Technikum\Masterarbeit")
from ABSA.helper import get_docs, classify_sentiment_value, ENTITY_RULER_PATTERNS
from pymongo import MongoClient

# Log-Verzeichnis definieren (relativer Pfad)
log_dir = os.path.join("ABSA", "method2")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "logfile.log")

# Logging konfigurieren: Log-Level INFO, Ausgabe in Konsole und in Datei
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, mode="a", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)
logger.info("Logging startet – Logfile wird abgelegt unter %s", log_file)

# Importiere das Zero-Shot-Klassifikations-Pipeline-Modul
from transformers import pipeline
import spacy

# Initialisiere Zero-Shot-Klassifikation
MODEL_NAME = "joeddav/xlm-roberta-large-xnli"
zero_shot_classifier = pipeline("zero-shot-classification", model=MODEL_NAME)
logger.info("Zero-Shot-Klassifikation initialisiert mit Modell: %s", MODEL_NAME)

# Lade spaCy-Modell
nlp = spacy.load("de_core_news_lg")
logger.info("spaCy-Modell 'de_core_news_lg' geladen.")

# Entity Ruler erstellen und vor dem Standard-NER platzieren
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.add_patterns(ENTITY_RULER_PATTERNS)
logger.info("Entity Ruler konfiguriert mit %d Patterns.", len(ENTITY_RULER_PATTERNS))

# --- Parameter für Zero-Shot ---
CANDIDATE_SENTIMENTS = ["positiv", "neutral", "negativ"]
logger.info("Verwendete Kandidatenlabels: %s", CANDIDATE_SENTIMENTS)

def classify_paragraph_with_aspects(paragraph_text, aspect_label="OeNB", candidate_sentiments=CANDIDATE_SENTIMENTS):    
    """
    Nutzt das Zero-Shot-Klassifikationsmodell, um den Sentiment-Status eines Paragraphen zu bestimmen.
    Dabei werden zuerst alle Entitäten mit dem Label `aspect_label` extrahiert. 
    Falls genau eine eindeutige Entität erkannt wird, wird deren Text für die Hypothesenformulierung verwendet;
    andernfalls wird das allgemeine Label genutzt.
    
    :param paragraph_text: String, der zu klassifizierende Paragraph
    :param aspect_label: z. B. "OeNB"
    :param candidate_sentiments: Liste der Kandidaten-Sentiment-Klassen
    :return: (best_label, numeric_score) oder (None, None) falls der Aspekt nicht erkannt wurde
    """
    doc_spacy = nlp(paragraph_text)
    aspect_ents = [ent for ent in doc_spacy.ents if ent.label_ == aspect_label]
    if not aspect_ents:
        logger.debug("Kein Aspekt mit Label %s im Paragraph gefunden.", aspect_label)
        return None, None

    unique_aspect_texts = set(ent.text for ent in aspect_ents)
    if len(unique_aspect_texts) == 1:
        aspect_text = unique_aspect_texts.pop()
    else:
        aspect_text = aspect_label

    hypotheses = [f"In diesem Artikel wird {aspect_text} {sentiment} erwähnt." for sentiment in candidate_sentiments]
    logger.debug("Hypothesen: %s", hypotheses)
    
    result = zero_shot_classifier(paragraph_text, candidate_labels=hypotheses, multi_label=False)
    best_label = result["labels"][0]
    best_confidence = result["scores"][0]
    logger.debug("Zero-Shot-Ergebnis: best_label=%s, confidence=%.3f", best_label, best_confidence)

    if aspect_text in best_label:
        best_label = best_label.replace(aspect_text, aspect_label)
        logger.debug("Best Label angepasst: %s", best_label)
    
    if "positiv" in best_label.lower():
        numeric = best_confidence
    elif "negativ" in best_label.lower():
        numeric = -best_confidence
    else:
        numeric = 0.0
        
    return best_label, numeric


def process_documents_for_aspect_method2(collection_name, aspect="OeNB"):
    """
    Für jedes Dokument:
      - Iteriert über alle Paragraphen.
      - Führt für jeden Paragraphen eine Zero-Shot-Klassifikation durch,
        allerdings nur, wenn der Aspekt (z. B. "OeNB") mittels Entity Ruler erkannt wurde.
      - Aggregiert die numerischen Scores (positive Scores als +, negative als -).
      - Leitet eine Gesamtklasse ab und speichert das Ergebnis in features.absa.method2.
    
    :param collection_name: Name der Collection (z. B. "derStandard")
    :param aspect: Der Aspekt, z. B. "OeNB"
    """
    docs_cursor, collection = get_docs(collection_name)
    docs_list = list(docs_cursor)
    logger.info("Verarbeite %d Dokumente in Collection %s.", len(docs_list), collection_name)

    for doc in docs_list:
        doc_id = doc.get("_id")

        if doc.get("features", {}).get("absa", {}).get("method2", {}).get("overall_sentiment"):
            logger.info("Dokument %s bereits analysiert (überspringe).", doc_id)
            continue

        doc_text = doc.get("article", {}).get("text", [])
        paragraphs_output = []
        overall_score = 0.0
        paragraphs_count = 0

        for j, paragraph in enumerate(doc_text, start=1):
            label, score = classify_paragraph_with_aspects(paragraph, aspect_label=aspect, candidate_sentiments=CANDIDATE_SENTIMENTS)
            if label is None:
                continue

            overall_score += score
            paragraphs_count += 1

            paragraph_info = {
                "index": j,
                "text": paragraph,
                "sentiments": [
                    {
                        "aspect": aspect,
                        "label": label,
                        "numeric_score": score
                    }
                ]
            }
            paragraphs_output.append(paragraph_info)
            logger.debug("Dokument %s, Paragraph %d: %s (Score: %.3f)", doc["_id"], j, label, score)
        
        if paragraphs_count == 0:
            logger.info("Document %s: Kein Paragraph mit Aspekt %s gefunden, übersprungen.", doc["_id"], aspect)
            continue
        
        overall_score /= paragraphs_count
        overall_class = classify_sentiment_value(overall_score, threshold=0.5)
        absa_method2_data = {
            "paragraphs": paragraphs_output,
            "overall_score": overall_score,
            "overall_sentiment": {
                aspect: overall_class
            }
        }

        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"features.absa.method2": absa_method2_data}}
        )
        logger.info("Document %s processed: overall_score=%.3f, overall_sentiment=%s",
                    doc["_id"], overall_score, overall_class)

    logger.info("%d Dokumente in %s verarbeitet (Methode 2).", len(docs_list), collection_name)


if __name__ == "__main__":
    collections = ["derStandard", "Krone", "ORF"]
    
    for c in collections:
        logger.info("Verarbeite Collection: %s", c)
        process_documents_for_aspect_method2(collection_name=c, aspect="OeNB")
