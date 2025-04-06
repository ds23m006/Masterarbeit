import os
import sys
import logging
from transformers import pipeline
import spacy

sys.path.append(r"Z:\Technikum\Masterarbeit")
from ABSA.helper import get_docs, ENTITY_RULER_PATTERNS

# Logging konfigurieren
log_dir = os.path.join("ABSA", "method2_3")
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "logfile.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, mode="a", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)
logger.info("Logging startet – Logfile unter %s", log_file)

# Modell initialisieren
MODEL_NAME = "joeddav/xlm-roberta-large-xnli"
zero_shot_classifier = pipeline("zero-shot-classification", model=MODEL_NAME)
logger.info("Zero-Shot-Modell initialisiert: %s", MODEL_NAME)

# spaCy-Modell laden
nlp = spacy.load("de_core_news_lg")
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.add_patterns(ENTITY_RULER_PATTERNS)

# Kandidatenlabels (binär)
CANDIDATE_SENTIMENTS = ["positiv", "negativ"]

def classify_sentence_binary(sentence_text, aspect_label="OeNB"):
    doc_spacy = nlp(sentence_text)
    aspect_ents = [ent for ent in doc_spacy.ents if ent.label_ == aspect_label]

    if not aspect_ents:
        return None, 0.0

    unique_aspect_texts = set(ent.text for ent in aspect_ents)
    aspect_text = unique_aspect_texts.pop() if len(unique_aspect_texts) == 1 else aspect_label

    hypotheses = [f"In diesem Satz wird {aspect_text} {sentiment} erwähnt." for sentiment in CANDIDATE_SENTIMENTS]

    result = zero_shot_classifier(sentence_text, candidate_labels=hypotheses, multi_label=True)

    scores_dict = dict(zip(result["labels"], result["scores"]))
    pos_score = scores_dict.get(hypotheses[0], 0.0)
    neg_score = scores_dict.get(hypotheses[1], 0.0)

    if pos_score > 0.8 and pos_score > neg_score + 0.5:
        return "positiv", pos_score
    elif neg_score > 0.8 and neg_score > pos_score + 0.5:
        return "negativ", -neg_score
    else:
        return "neutral", 0.0

def process_documents_method2_3(collection_name, aspect="OeNB"):
    docs_cursor, collection = get_docs(collection_name)
    docs_list = list(docs_cursor)
    logger.info("Verarbeite %d Dokumente in Collection %s.", len(docs_list), collection_name)

    for doc in docs_list:
        doc_id = doc.get("_id")
        
        doc_text = doc.get("article", {}).get("text", [])
        overall_score = 0.0
        sentence_count = 0

        for paragraph in doc_text:
            doc_spacy = nlp(paragraph)
            sentences = list(doc_spacy.sents)

            for sentence in sentences:
                label, score = classify_sentence_binary(sentence.text, aspect_label=aspect)
                if label is None:
                    continue

                overall_score += score
                sentence_count += 1

        if sentence_count == 0:
            logger.info("Dokument %s enthält keine relevanten Sätze (überspringe).", doc_id)
            continue

        overall_score /= sentence_count
        overall_class = "neutral"
        if overall_score >= 0.3:
            overall_class = "positiv"
        elif overall_score <= -0.3:
            overall_class = "negativ"

        collection.update_one(
            {"_id": doc_id},
            {"$set": {"features.absa.method2_3.overall_sentiment": {aspect: overall_class}}}
        )

        logger.info("Dokument %s verarbeitet: %s (Score: %.3f)", doc_id, overall_class, overall_score)


if __name__ == "__main__":
    collections = ["derStandard", "Krone", "ORF"]
    for collection_name in collections:
        logger.info("Starte Verarbeitung der Collection: %s", collection_name)
        process_documents_method2_3(collection_name, aspect="OeNB")
