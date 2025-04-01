import os
import sys
import logging
from collections import deque

sys.path.append(r"Z:\Technikum\Masterarbeit")
from ABSA.helper import get_docs, load_sentiws, classify_sentiment_value, ENTITY_RULER_PATTERNS
from pymongo import MongoClient

# Logging konfigurieren: Ausgabe in Konsole und Datei (optional)
log_dir = os.path.join("ABSA", "method1")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, "method1.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, mode="a", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)
logger.info("Logging gestartet für Methode 1. Logfile: %s", log_file)

# -------------------------------
# spaCy, Entity Ruler, SentiWS und Extensions
# -------------------------------
import spacy
from spacy.pipeline import EntityRuler
from spacy.tokens import Token, Doc

# Lade spaCy-Modell
nlp = spacy.load("de_core_news_lg")
logger.info("spaCy-Modell 'de_core_news_lg' geladen.")

# Entity Ruler erstellen und vor dem Standard-NER platzieren
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.add_patterns(ENTITY_RULER_PATTERNS)
logger.info("Entity Ruler konfiguriert mit %d Patterns.", len(ENTITY_RULER_PATTERNS))

# SentiWS-Lexikon laden
sentiws_lexicon = load_sentiws()
logger.info("SentiWS-Lexikon geladen: %d Einträge.", len(sentiws_lexicon))

# Token-Extension: SentiWS-Score
if not Token.has_extension("sentiws_score"):
    Token.set_extension("sentiws_score", default=0.0)

def sentiws_score_getter(token):
    return sentiws_lexicon.get(token.lemma_.lower(), 0.0)

# Getter registrieren
Token.set_extension("sentiws_score", getter=sentiws_score_getter, force=True)
logger.info("Token-Extension 'sentiws_score' registriert.")

# Definition der Negations- und Booster-Wörter
NEGATION_WORDS = {"nicht", "kein", "keine", "keinen", "keinem", "ohne", "nie"}
BOOSTER_WORDS = {
    "sehr": 1.5,
    "extrem": 2.0,
    "leicht": 1.2,
    "ein bisschen": 1.1,
    "wirklich": 1.3,
    "kaum": 0.8,
    "nur": 0.8
}

def refine_score_with_modifiers(token, base_score):
    """
    Prüft die direkten Kinder eines Tokens, um etwaige Negationen oder Booster zu berücksichtigen.
      - Bei Negation (dep_ == "neg" oder Wort in NEGATION_WORDS) wird der Score mit -0.5 multipliziert.
      - Bei Booster (dep_ in ("advmod", "amod") und Wort in BOOSTER_WORDS) wird der Score mit dem jeweiligen Faktor multipliziert.
    """
    final_score = base_score

    # prüfen der direkten children:
    for child in token.children:
        if child.dep_ == "neg" or (child.lower_ in NEGATION_WORDS):
            final_score *= -0.5
        elif (child.dep_ in ("advmod", "amod")) and (child.lower_ in BOOSTER_WORDS):
            factor = BOOSTER_WORDS[child.lower_]
            final_score *= factor
    return final_score



####################################################
# Prüfung, ob Token per Dependency auf Aspect verweist
####################################################
def is_token_linked_to_aspect(token, aspect_tokens, max_depth=3):
    """
    Führt eine BFS im Dependency-Baum durch (bis zu max_depth Kanten),
    um zu prüfen, ob 'token' mit einem der 'aspect_tokens' verknüpft ist.
    """
    if token in aspect_tokens:
        return True

    visited = set([token])
    queue = deque([(token, 0)])

    while queue:
        current, depth = queue.popleft()
        if depth >= max_depth:
            continue  # Tiefer wollen wir nicht suchen

        # Nachbarn: Head und Children
        neighbors = []
        # Head (wenn es nicht root ist und nicht schon besucht)
        if current.head != current and current.head not in visited:
            neighbors.append(current.head)
        # Children
        for child in current.children:
            if child not in visited:
                neighbors.append(child)

        # Prüfen, ob einer dieser Nachbarn im aspect_tokens ist
        for nb in neighbors:
            if nb in aspect_tokens:
                return True
            visited.add(nb)
            queue.append((nb, depth + 1))

    return False

def compute_sentiment_for_aspect_method1(doc, aspect_label="OeNB", max_depth=3):
    """
    Durchläuft alle Sätze des spaCy-Dokuments und summiert die Sentimentwerte aller Tokens,
    die per Dependency-Check (BFS, max_depth) dem Aspekt zugeordnet werden können.
    Sentiment-Werte werden über die Booster/Negationslogik angepasst.
    Sätze ohne Aspekt oder Sentiment-Token werden übersprungen.
    """
    total_score = 0.0
    for sent in doc.sents:
        # 1) Prüfen, ob Aspect im Satz existiert
        aspect_ents = [ent for ent in sent.ents if ent.label_ == aspect_label]
        if not aspect_ents:
            continue
        sentiment_tokens = [t for t in sent if t._.sentiws_score != 0.0]
        if not sentiment_tokens:
            continue
        aspect_tokens = set()
        for ent in aspect_ents:
            for t in ent:
                aspect_tokens.add(t)

        # 4) Aggregation der relevanten Sentimentwerte
        sent_score = 0.0
        for token in sentiment_tokens:
            if is_token_linked_to_aspect(token, aspect_tokens, max_depth=max_depth):
                base_score = token._.sentiws_score
                refined_score = refine_score_with_modifiers(token, base_score)
                sent_score += refined_score

        total_score += sent_score
    return total_score

# Doc-Extension zur Speicherung der ABSA-Ergebnisse
if not Doc.has_extension("aspect_sentiment"):
    Doc.set_extension("aspect_sentiment", default=dict)

def perform_absa(text, aspects=["OeNB"], max_depth=3):
    """
    Erzeugt ein spaCy-Dokument aus dem Text, berechnet für jeden Aspekt in 'aspects'
    den aggregierten Sentiment-Wert (Methode 1) und speichert ihn in doc._.aspect_sentiment.
    """
    doc = nlp(text)
    
    # Leeres Dictionary in Extension
    doc._.aspect_sentiment = {}

    # Für jeden Aspekt Label
    for aspect_label in aspects:
        sentiment_value = compute_sentiment_for_aspect_method1(doc, aspect_label=aspect_label, max_depth=max_depth)
        doc._.aspect_sentiment[aspect_label] = sentiment_value
        logger.debug("Perform ABSA: Aspekt %s, Score: %.3f", aspect_label, sentiment_value)
    return doc

def process_documents_for_aspect(collection_name="derStandard", aspects=["OeNB"]):
    """
    Lädt Dokumente aus der angegebenen Collection und verarbeitet jedes Dokument.
    Für jedes Dokument werden die Paragraphen aus dem Feld article.text durchlaufen,
    für jeden Paragraphen wird die ABSA (Methode 1) durchgeführt, und die Ergebnisse
    (paragraphweise und aggregiert) werden in der Datenbank unter features.absa.method1 gespeichert.
    
    :param collection_name: Name der Collection (z. B. "derStandard")
    :param aspects: Liste der zu verarbeitenden Aspekte, z. B. ["OeNB"]
    """
    docs_cursor, collection = get_docs(collection_name)
    docs_list = list(docs_cursor)
    logger.info("Verarbeite %d Dokumente in Collection '%s'.", len(docs_list), collection_name)

    for doc in docs_list:
        # doc_text ist eine Liste von Paragraph-Strings
        doc_text = doc.get("article", {}).get("text", [])
        
        paragraphs_output = []
        overall_scores = {aspect: 0.0 for aspect in aspects}

        # ---- 2) Über Paragraphen iterieren ----
        for j, paragraph_j in enumerate(doc_text, start=1):
            doc_spacy = nlp(paragraph_j)
            paragraph_sentiments = []

            # Prüfe ob Paragraph überhaupt relevante Entitäten enthält
            has_aspect = any(ent.label_ in aspects for ent in doc_spacy.ents)
            if not has_aspect:
                logger.debug("Überspringe Paragraph %d (kein Aspekt gefunden)", j)
                continue

            for aspect in aspects:
                aspect_ents = [ent for ent in doc_spacy.ents if ent.label_ == aspect]
                if not aspect_ents:
                    continue

                # Führe ABSA durch
                score = compute_sentiment_for_aspect_method1(doc_spacy, aspect_label=aspect, max_depth=3)
                sentiment_class = classify_sentiment_value(score)

                paragraph_sentiments.append({
                    "aspect": aspect,
                    "score": score,
                    "class": sentiment_class
                })
                overall_scores[aspect] += score

            if paragraph_sentiments:
                paragraph_info = {
                    "index": j,
                    "text": paragraph_j,
                    "sentiments": paragraph_sentiments
                }
                paragraphs_output.append(paragraph_info)
                logger.debug("Dokument %s, Paragraph %d verarbeitet.", doc["_id"], j)

        if not paragraphs_output:
            logger.info("Dokument %s hat keine relevanten Paragraphen, übersprungen.", doc["_id"])
            continue  # Optional: Dokument überspringen, wenn kein Paragraph gespeichert wurde

        overall_sentiment = {aspect: classify_sentiment_value(overall_scores[aspect]) for aspect in aspects}
        absa_method1_data = {
            "paragraphs": paragraphs_output,
            "overall_scores": overall_scores,
            "overall_sentiment": overall_sentiment
        }
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"features.absa.method1": absa_method1_data}}
        )
        logger.info("Document %s processed: Overall Score: %s, Overall Sentiment: %s",
                    doc["_id"], overall_scores, overall_sentiment)
    logger.info("%d Dokumente in Collection '%s' verarbeitet (Methode 1).", len(docs_list), collection_name)


if __name__ == "__main__":
    # Liste der Collections, die verarbeitet werden sollen
    collections = ["derStandard", "Krone", "ORF"]
    for c in collections:
        logger.info("Starte Verarbeitung der Collection: %s", c)
        process_documents_for_aspect(collection_name=c, aspects=["OeNB"])
