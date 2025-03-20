import sys
sys.path.append(r"Z:\Technikum\Masterarbeit")
from ABSA.helper import get_docs, load_sentiws, ENTITY_RULER_PATTERNS

import spacy
from spacy.pipeline import EntityRuler
from spacy.tokens import Token, Doc

from collections import deque  # für BFS


docs_cursor, collection = get_docs("derStandard")

# SpaCy-Modell laden (Deutsch)
nlp = spacy.load("de_core_news_lg")

# Entity Ruler erstellen und vor dem Standard-NER platzieren
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.add_patterns(ENTITY_RULER_PATTERNS)



# -------------------------------
# Token-Extension: SentiWS-Score
# -------------------------------

# Dictionary laden
sentiws_lexicon = load_sentiws()

if not Token.has_extension("sentiws_score"):
    Token.set_extension("sentiws_score", default=0.0)

def sentiws_score_getter(token):
    return sentiws_lexicon.get(token.lemma_.lower(), 0.0)

# Getter registrieren
Token.set_extension("sentiws_score", getter=sentiws_score_getter, force=True)


# diese Begriffe negieren das Sentiment (gedämpft)
negation_words = {"nicht", "kein", "keine", "keinen", "keinem", "ohne", "nie"}

# diese Begriffe verstärken oder schwächen das Sentiment
booster_words = {
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
    Prüft die direkten Kinder für:
     - Negation (dep_ == "neg" bzw. Wort in negation_words)
     - Booster (dep_ == "advmod" oder "amod", bzw. Wort in booster_words)
    und passt den Score an:
      - Negation => Multiplikation mit -0.5
      - Booster => Multiplikation mit dem jeweiligen Booster-Faktor
    """
    final_score = base_score

    # prüfen der direkten children:
    for child in token.children:
        # Negation?
        if child.dep_ == "neg" or (child.lower_ in negation_words):
            final_score *= -0.5
        # Booster?
        elif (child.dep_ in ("advmod", "amod")) and (child.lower_ in booster_words):
            factor = booster_words[child.lower_]
            final_score *= factor
    return final_score



####################################################
# Prüfung, ob Token per Dependency auf Aspect verweist
####################################################
def is_token_linked_to_aspect(token, aspect_tokens, max_depth=3):
    """
    Führt eine BFS im Dependency-Baum durch (bis zu max_depth Kanten),
    um herauszufinden, ob 'token' mit einem der 'aspect_tokens' verknüpft ist.
    
    Wir gehen von 'token' aus:
     - Fügen seinen Head und seine Kinder zur Warteschlange hinzu,
     - gehen pro Nachbar eine Ebene tiefer usw.
    Falls wir dabei auf einen Token aus 'aspect_tokens' treffen, return True.
    Ansonsten False.
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



####################################################
# compute_sentiment_for_aspect (mit Satz-Skip-Optimierung)
####################################################
def compute_sentiment_for_aspect(doc, aspect_label="OeNB", max_depth=3):
    """
    Durchläuft alle Sätze und summiert Sentiment nur für Tokens,
    die sich auf den aspect beziehen. (Dependency-Check)
    Engere Booster-/Negationsprüfung (Kinder).
    Überspringt Satz, wenn kein Aspect oder keine sentimenttragenden Tokens.
    
    max_depth: wie tief wir im Dependency-Baum für die Verknüpfung suchen (BFS).
    """
    total_score = 0.0
    for sent in doc.sents:
        # 1) Prüfen, ob Aspect im Satz existiert
        aspect_ents = [ent for ent in sent.ents if ent.label_ == aspect_label]
        if not aspect_ents:
            continue  # Kein aspect => Satz ignorieren

        # 2) Gibt es im Satz Sentiment-Token?
        sentiment_tokens = [t for t in sent if t._.sentiws_score != 0.0]
        if not sentiment_tokens:
            continue  # Keine Sentiments => Satz ignorieren

        # 3) Tokens, die zu den aspect-Entitäten gehören, erfassen
        aspect_tokens = set()
        for ent in aspect_ents:
            for t in ent:
                aspect_tokens.add(t)

        # 4) Aggregation der relevanten Sentimentwerte
        sent_score = 0.0
        for token in sentiment_tokens:
            if is_token_linked_to_aspect(token, aspect_tokens, max_depth=max_depth):
                base_score = token._.sentiws_score
                refined_score = refine_score_with_modifiers(token, base_score) # Booster/Negations-Logik
                sent_score += refined_score


        total_score += sent_score

    return total_score


####################################################
# Doc-Extension 
####################################################
#    Speichert in einem Dictionary {<aspekt_label>: <score>, ...}
if not Doc.has_extension("aspect_sentiment"):
    Doc.set_extension("aspect_sentiment", default=dict)

def perform_absa(text, aspects=["OeNB"], max_depth=3):
    """
    Nimmt einen Text, erstellt ein Doc, berechnet für jeden Aspekt in 'aspects'
    den Sentiment-Wert und legt ihn in doc._.aspect_sentiment[aspekt_label] ab.
    """
    doc = nlp(text)
    
    # Leeres Dictionary in Extension
    doc._.aspect_sentiment = {}

    # Für jeden Aspekt Label
    for aspect_label in aspects:
        sentiment_value = compute_sentiment_for_aspect(
            doc,
            aspect_label=aspect_label,
            max_depth=max_depth
        )
        doc._.aspect_sentiment[aspect_label] = sentiment_value

    return doc



def classify_sentiment_value(value, threshold=0.5):
    """
    Wandelt den Sentiment-Wert in eine einfache Klasse um:
      - > threshold => 'positiv'
      - < -threshold => 'negativ'
      - sonst => 'neutral'
    Du kannst die Schwellen anpassen, z.B. threshold=0.5 etc.
    """
    if value > threshold:
        return "positiv"
    elif value < -threshold:
        return "negativ"
    else:
        return "neutral"
    


def process_documents_for_aspect(collection_name="derStandard", aspects=["OeNB"]):
    """
    Lädt bis zu max_docs Dokumente aus der angegebenen Collection.
    Für jedes Dokument iteriert diese Funktion über alle Paragraphen,
    berechnet den Sentiment-Wert pro definiertem Aspekt (z. B. "OeNB") mittels perform_absa,
    speichert die Werte pro Paragraphen und aggregiert die Werte für das gesamte Dokument.
    
    Die Ergebnisse werden in der Struktur
    {
      "paragraphs": [
          {
             "index": <Paragraph-Nummer>,
             "text": <Paragraph-Text>,
             "sentiments": [
                 {
                   "aspect": <Aspekt>,
                   "score": <Wert>,
                   "class": <Klasse>
                 },
                 ...
             ]
          },
          ...
      ],
      "overall_scores": { <Aspekt>: <aggregierter Score>, ... },
      "overall_sentiment": { <Aspekt>: <Klasse>, ... }
    }
    unter features.absa.method1 in der Datenbank gespeichert.
    """
    docs_cursor, collection = get_docs(collection_name)
    docs_list = list(docs_cursor)

    for doc in docs_list:
        # doc_text ist eine Liste von Paragraph-Strings
        doc_text = doc.get("article", {}).get("text", [])
        
        paragraphs_output = []
        overall_scores = {aspect: 0.0 for aspect in aspects}

        # ---- 2) Über Paragraphen iterieren ----
        for j, paragraph_j in enumerate(doc_text, start=1):
            paragraph_sentiments = []
            # Für jeden Aspekt den Score berechnen
            for aspect in aspects:
                # Führe ABSA für den aktuellen Paragraphen für den jeweiligen Aspekt aus
                doc_spacy = perform_absa(paragraph_j, aspects=[aspect], max_depth=3)
                score = doc_spacy._.aspect_sentiment.get(aspect, 0.0)
                sentiment_class = classify_sentiment_value(score)

                paragraph_sentiments.append({
                    "aspect": aspect,
                    "score": score,
                    "class": sentiment_class
                })
                overall_scores[aspect] += score

            paragraph_info = {
                "index": j,
                "text": paragraph_j,
                "sentiments": paragraph_sentiments
            }
            paragraphs_output.append(paragraph_info)


        # Gesamtsentiment pro Aspekt ableiten
        overall_sentiment = {aspect: classify_sentiment_value(overall_scores[aspect])
                             for aspect in aspects}
        
        absa_method1_data = {
            "paragraphs": paragraphs_output,
            "overall_scores": overall_scores,
            "overall_sentiment": overall_sentiment
        }

        # ---- 4) Update in DB: features.absa.method1 ----
        # Update in der Datenbank unter features.absa.method1
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"features.absa.method1": absa_method1_data}}
        )
        print(f"Document {doc['_id']} processed and updated.")

    print(f"{len(docs_list)} Dokumente verarbeitet und aktualisiert.")


if __name__ == "__main__":
    process_documents_for_aspect(collection_name="derStandard", aspects=["OeNB"])