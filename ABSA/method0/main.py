import os
import sys
import logging
sys.path.append(r"Z:\Technikum\Masterarbeit")
from ABSA.helper import get_docs, classify_sentiment_value, ENTITY_RULER_PATTERNS
from pymongo import MongoClient
from transformers import pipeline
import spacy

# Logging konfigurieren
log_dir = os.path.join("ABSA", "method0")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "method0.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, mode="a", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)
logger.info("Logging gestartet für Methode 0. Logfile: %s", log_file)

# Lade spaCy-Modell
nlp = spacy.load("de_core_news_lg")
logger.info("spaCy-Modell 'de_core_news_lg' geladen.")

# Entity Ruler erstellen
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.add_patterns(ENTITY_RULER_PATTERNS)
logger.info("Entity Ruler konfiguriert mit %d Patterns.", len(ENTITY_RULER_PATTERNS))

# Sentiment-Modell laden
MODEL_NAME = "oliverguhr/german-sentiment-bert"
sentiment_classifier = pipeline("sentiment-analysis", model=MODEL_NAME)
logger.info("Sentiment-Modell geladen: %s", MODEL_NAME)

# Mapping der Labels auf numerische Werte
LABEL_MAPPING = {"positive": 1, "neutral": 0, "negative": -1}


def classify_paragraph(paragraph):
    result = sentiment_classifier(paragraph)[0]
    label = result['label'].lower()
    score = LABEL_MAPPING.get(label, 0)
    return label, score


def process_documents_for_aspect_method0(collection_name, aspect="OeNB"):
    docs_cursor, collection = get_docs(collection_name)
    docs_list = list(docs_cursor)
    logger.info("Verarbeite %d Dokumente in Collection %s.", len(docs_list), collection_name)

    for doc in docs_list:
        doc_id = doc.get("_id")

        if doc.get("features", {}).get("absa", {}).get("method0", {}).get("overall_sentiment"):
            logger.info("Dokument %s bereits analysiert (überspringe).", doc_id)
            continue

        doc_text = doc.get("article", {}).get("text", [])
        paragraphs_output = []
        sentiment_scores = []

        for j, paragraph in enumerate(doc_text, start=1):
            doc_spacy = nlp(paragraph)

            # Paragraph überspringen, wenn Aspekt nicht vorkommt
            if not any(ent.label_ == aspect for ent in doc_spacy.ents):
                continue

            label, score = classify_paragraph(paragraph)
            sentiment_scores.append(score)

            paragraphs_output.append({
                "index": j,
                "text": paragraph,
                "sentiments": [{
                    "aspect": aspect,
                    "label": label,
                    "numeric_score": score
                }]
            })
            logger.debug("Dokument %s, Paragraph %d: %s (Score: %d)", doc_id, j, label, score)

        if not sentiment_scores:
            logger.info("Dokument %s enthält keine relevanten Paragraphen (überspringe).", doc_id)
            continue

        overall_score = sum(sentiment_scores) / len(sentiment_scores)
        overall_class = classify_sentiment_value(overall_score, threshold=0.3)

        absa_method0_data = {
            "paragraphs": paragraphs_output,
            "overall_score": overall_score,
            "overall_sentiment": {aspect: overall_class}
        }

        collection.update_one(
            {"_id": doc_id},
            {"$set": {"features.absa.method0": absa_method0_data}}
        )
        logger.info("Dokument %s verarbeitet: overall_score=%.3f, overall_sentiment=%s",
                    doc_id, overall_score, overall_class)

    logger.info("%d Dokumente in %s verarbeitet (Methode 0).", len(docs_list), collection_name)


if __name__ == "__main__":
    collections = ["derStandard", "Krone", "ORF"]
    for c in collections:
        logger.info("Starte Verarbeitung der Collection: %s", c)
        process_documents_for_aspect_method0(collection_name=c, aspect="OeNB")
