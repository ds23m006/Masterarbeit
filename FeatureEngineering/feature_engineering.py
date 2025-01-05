# feature_engineering.py
from pymongo import UpdateOne
import logging

def run_basic_feature_engineering(conn, batch_size=1000):
    """
    Bestimmt features.paragraph_count und features.body_word_count für Dokumente in 'conn'.
    """
    logger = logging.getLogger(__name__)
    filter_query = {
        "$or": [
            {"features.paragraph_count": {"$exists": False}},
            {"features.body_word_count": {"$exists": False}}
        ]
    }

    try:
        cursor = conn.find(filter_query, no_cursor_timeout=True).batch_size(batch_size)
        bulk_updates = []
        counter = 0

        for doc in cursor:
            counter += 1

            text_list = doc.get('article', {}).get('text', [])
            if not isinstance(text_list, list):
                text_list = []

            paragraph_count = len(text_list)
            body_word_count = sum(len(paragraph.split()) for paragraph in text_list)

            # Für Bulk-Update sammeln
            update_op = UpdateOne(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "features.paragraph_count": paragraph_count,
                        "features.body_word_count": body_word_count
                    }
                }
            )
            bulk_updates.append(update_op)

            # Wenn BATCH_SIZE erreicht ist, alle Updates auf einmal ausführen
            if len(bulk_updates) == batch_size:
                conn.bulk_write(bulk_updates, ordered=False)
                logger.info(f"{counter} Dokumente verarbeitet und geschrieben.")
                bulk_updates.clear()

        # Reste schreiben
        if bulk_updates:
            conn.bulk_write(bulk_updates, ordered=False)
            logger.info(f"Abschließender Bulk-Write für {len(bulk_updates)} Dokumente.")

        logger.info(f"Fertig! Insgesamt {counter} Dokumente bearbeitet.")

    except Exception as e:
        logger.error(f"Fehler beim Basic Feature Engineering: {e}", exc_info=True)
    finally:
        cursor.close()
