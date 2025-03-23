import os
import json
import logging
from pymongo import MongoClient
import openai
import spacy
import re

import sys
sys.path.append(r"Z:\Technikum\Masterarbeit")
from ABSA.helper import get_docs, ENTITY_RULER_PATTERNS


def main(aspects=["OeNB"], collections_to_process=["derStandard", "Krone", "ORF"]):
    os.makedirs("ABSA/method3", exist_ok=True)
    logger = logging.getLogger("ABSA_method3")
    logger.setLevel(logging.INFO)

    log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler("ABSA/method3/method3.log", mode="a", encoding="utf-8")
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # set api key
    openai.api_ke = os.getenv("OPENAI_API_KEY")

    nlp = spacy.load("de_core_news_lg")
    ruler = nlp.add_pipe("entity_ruler", before="ner")
    ruler.add_patterns(ENTITY_RULER_PATTERNS)

    for collection_name in collections_to_process:
        logger.info("Verarbeite Collection: %s", collection_name)
        documents, collection = get_docs(collection_name)

        for doc in documents:
            doc_id = doc.get("_id")

            """
            if doc.get("features", {}).get("absa", {}).get("method3", {}).get("overall_sentiment"):
                logger.info("Dokument %s bereits analysiert (überspringe).", doc_id)
                continue
            """

            paragraphs = doc.get("article", {}).get("text", [])
            if not paragraphs:
                logger.warning("Dokument %s enthält keinen Text (überspringe).", doc_id)
                continue

            text = "\n\n".join(paragraphs)
            doc_spacy = nlp(text)

            aspects_found = set(ent.label_ for ent in doc_spacy.ents if ent.label_ in aspects)
            if not aspects_found:
                logger.info("Dokument %s enthält keinen Zielaspekt (überspringe).", doc_id)
                continue

            aspects_prompt = ", ".join(aspects_found)

            prompt_text = (
                f"Analysiere den folgenden Text hinsichtlich des Sentiments bezüglich der österreichischen Nationalbank (OeNB).\n"
                "Berücksichtige Sarkasmus, Ironie, Kontext und andere linguistische Merkmale.\n"
                "Gib das Ergebnis ausschließlich als JSON-Objekt mit folgendem Format zurück:\n"
                '{"OeNB": "positiv|neutral|negativ"}\n'
                "Zero-Shot-Prompt. Keine weiteren Erklärungen oder Texte, nur das JSON-Objekt.\n\n"
                f"Text:\n{text}"
            )

            client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

            try:
                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt_text}],
                    temperature=0.0
                )
                gpt_answer = response.choices[0].message.content.strip()

                # Markdown-Codeblöcke entfernen (```json ... ```)
                gpt_answer = re.sub(r'^```json\s*|\s*```$', '', gpt_answer, flags=re.MULTILINE).strip()

            except Exception as e:
                logger.error("OpenAI API Fehler bei Dokument %s: %s", doc_id, e)
                continue

            try:
                sentiment_result = json.loads(gpt_answer)
            except json.JSONDecodeError as e:
                logger.error("Ungültiges JSON bei Dokument %s: %s | Antwort: %s", doc_id, e, gpt_answer)
                continue

            if not all(aspect in sentiment_result for aspect in aspects_found):
                logger.error("Fehlende Aspekte in GPT-Antwort für Dokument %s: %s", doc_id, gpt_answer)
                continue

            try:
                collection.update_one(
                    {"_id": doc_id},
                    {"$set": {"features.absa.method3.overall_sentiment": sentiment_result}}
                )
                logger.info("Sentiment für Dokument %s gespeichert: %s", doc_id, sentiment_result)
            except Exception as e:
                logger.error("Fehler beim Speichern für Dokument %s: %s", doc_id, e)


if __name__ == "__main__":
    main()