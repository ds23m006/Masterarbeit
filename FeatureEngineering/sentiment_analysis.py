# sentiment_analysis.py

import nltk
from nltk.tokenize import sent_tokenize
from transformers import pipeline, AutoTokenizer

nltk.download('punkt')

# Globale Variablen / Pipeline-Setup
sentiment_pipeline = pipeline("sentiment-analysis", model="oliverguhr/german-sentiment-bert")
tokenizer = sentiment_pipeline.tokenizer

def split_text_into_chunks_paragraphwise(paragraph, max_chunk_size=520):
    """
    Nimmt einen einzelnen Paragraphen und splittet ihn mithilfe von nltk-Satztokenisierung
    in Chunks (max. max_chunk_size Tokens). Jeder Chunk wird gesondert klassifiziert.
    """
    sentences = sent_tokenize(paragraph, language='german')
    chunks = []
    current_chunk = ""
    current_length = 0

    for sentence in sentences:
        tokens = tokenizer.encode(sentence, add_special_tokens=False)
        sentence_length = len(tokens)

        if sentence_length > max_chunk_size:
            # Falls Satz zu lang -> split
            split_indices = range(0, sentence_length, max_chunk_size)
            for i in split_indices:
                sub_tokens = tokens[i:i + max_chunk_size]
                sub_chunk = tokenizer.decode(sub_tokens, clean_up_tokenization_spaces=True)
                sub_length = len(sub_tokens)
                chunks.append((sub_chunk, sub_length))
        else:
            # Wenn aktueller Satz nicht mehr in den Chunk passt
            if current_length + sentence_length > max_chunk_size:
                if current_chunk:
                    chunks.append((current_chunk, current_length))
                current_chunk = sentence
                current_length = sentence_length
            else:
                if current_chunk:
                    current_chunk += " " + sentence
                else:
                    current_chunk = sentence
                current_length += sentence_length

    if current_chunk:
        chunks.append((current_chunk, current_length))

    return chunks


def analyze_sentiment_paragraph(paragraph, max_chunk_size=520):
    """
    Berechnet das Sentiment für einen einzelnen Paragraphen,
    indem er ihn in Chunks aufteilt und dann einen gewichteten
    Durchschnitt bildet.
    """
    chunks = split_text_into_chunks_paragraphwise(paragraph, max_chunk_size)
    if not chunks:
        return None, None  # Absatz war leer

    chunk_texts, chunk_lengths = zip(*chunks)
    results = sentiment_pipeline(list(chunk_texts))

    sentiments = []
    weights = []

    for result, length in zip(results, chunk_lengths):
        label = result['label'].lower()
        score = result['score']

        # Mappe Sentiment-Klassen auf numerische Werte
        if label == 'negative':
            sentiment_value = -1
        elif label == 'neutral':
            sentiment_value = 0
        elif label == 'positive':
            sentiment_value = 1
        else:
            sentiment_value = 0  # fallback

        # Gewichteter Sentiment-Score
        sentiment_score = sentiment_value * score
        sentiments.append(sentiment_score)
        weights.append(length)

    weighted_sum = sum(s * w for s, w in zip(sentiments, weights))
    total_weights = sum(weights)
    if total_weights == 0:
        avg_sentiment = 0.0
    else:
        avg_sentiment = weighted_sum / total_weights

    # Klassifiziere
    if avg_sentiment < -0.5:
        sentiment_class = 'negative'
    elif -0.5 <= avg_sentiment < 0.5:
        sentiment_class = 'neutral'
    else:
        sentiment_class = 'positive'

    return avg_sentiment, sentiment_class


def _token_weight(text: str):
    """
    Hilfsfunktion, um die Anzahl Tokens (nach BERT-Tokenizer) eines Absatzes zu ermitteln.
    """
    if not text:
        return 0
    tokens = tokenizer.encode(text, add_special_tokens=False)
    return len(tokens)


def run_sentiment_analysis(conn, logger, batch_size=1000):
    """
    Führt die Sentiment-Analyse pro Paragraph durch und speichert
    zusätzlich einen Dokument-Sentiment-Wert (token-gewichtet).

    Filterkriterien:
    0) features.paragraph_sentiments == null
    1) scraping_info.status = "success"
    2) features.paywall != True
    3) features.APA_sentiment != null

    Ergebnis:
     - features.paragraph_sentiments: array pro Absatz
       [
         {
           "paragraph_index": ...,
           "text": "...",
           "sentiment_value": float,
           "sentiment_class": str
         },
         ...
       ]

     - features.doc_sentiment_value: float
     - features.doc_sentiment_class: str
    """

    logger.info(f"Starte Sentiment-Analyse in Collection '{conn.name}'...")

    query = {
        "scraping_info.status": "success",
        "$or": [
            {"features.paywall": {"$exists": False}},
            {"features.paywall": {"$eq": False}},
            {"features.paywall": None}
        ],
        "features.APA_sentiment": {"$ne": None},
        # nur Dokumente, wo doc_sentiment_class noch nicht gesetzt:
        "features.doc_sentiment_class": None
    }

    doc_count = conn.count_documents(query)
    logger.info(f"{doc_count} Dokumente gefunden in '{conn.name}' gemäß Filter (Sentiment).")

    cursor = conn.find(query, no_cursor_timeout=True).batch_size(batch_size)

    counter = 0
    for doc in cursor:
        counter += 1
        doc_id = doc["_id"]
        article_obj = doc.get("article", {})
        paragraphs = article_obj.get("text", [])

        if not isinstance(paragraphs, list):
            logger.debug(f"Dokument {doc_id}: 'article.text' ist kein Array -> Überspringe.")
            continue

        paragraph_sentiments = []
        # Für die Dokumentberechnung
        doc_sentiment_sum = 0.0
        doc_token_sum = 0

        for i, paragraph in enumerate(paragraphs):
            p_str = paragraph.strip()
            if not p_str:
                paragraph_sentiments.append({
                    "paragraph_index": i,
                    "text": "",
                    "sentiment_value": None,
                    "sentiment_class": None
                })
                continue

            avg_sent, sent_class = analyze_sentiment_paragraph(p_str)
            paragraph_sentiments.append({
                "paragraph_index": i,
                "text": p_str,
                "sentiment_value": avg_sent,
                "sentiment_class": sent_class
            })

            # Nun Tokens im gesamten Absatz zählen (nicht nur in den Chunks)
            # um eine Weighted-Average-Berechnung auf Doku-Ebene zu ermöglichen:
            paragraph_token_count = _token_weight(p_str)
            if avg_sent is not None and paragraph_token_count > 0:
                doc_sentiment_sum += avg_sent * paragraph_token_count
                doc_token_sum += paragraph_token_count

        # Dokument-Level-Berechnung
        if doc_token_sum == 0:
            doc_sent_value = None
            doc_sent_class = None
        else:
            doc_avg = doc_sentiment_sum / doc_token_sum
            if doc_avg < -0.5:
                doc_sent_class = 'negative'
            elif -0.5 <= doc_avg < 0.5:
                doc_sent_class = 'neutral'
            else:
                doc_sent_class = 'positive'
            doc_sent_value = doc_avg

        # Speichere die Ergebnisse
        update_data = {
            "features.paragraph_sentiments": paragraph_sentiments,
            "features.doc_sentiment_value": doc_sent_value,
            "features.doc_sentiment_class": doc_sent_class
        }

        update_result = conn.update_one(
            {"_id": doc_id},
            {"$set": update_data}
        )
        logger.debug(
            f"[{conn.name}] Dokument {doc_id} aktualisiert. "
            f"Matched: {update_result.matched_count}, Modified: {update_result.modified_count}"
        )

    logger.info(f"Sentiment-Analyse abgeschlossen für {counter} Dokumente in '{conn.name}'.")
    cursor.close()
