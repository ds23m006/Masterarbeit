# keyword_extraction.py
import re
from collections import defaultdict
from pymongo import UpdateOne
import logging

def count_tags_in_texts(list_of_texts, tags_dict):
    """
    Durchsucht eine Liste von Strings nach den Tags in tags_dict.
    Gibt ein Dictionary { tag_name: gesamtanzahl_der_vorkommen } zurück.
    """
    tag_counts = defaultdict(int)

    for field_str in list_of_texts:
        if field_str is None:
            field_str = ""
        for tag_name, patterns in tags_dict.items():
            for pattern in patterns:
                matches = re.findall(pattern, field_str, flags=re.IGNORECASE)
                tag_counts[tag_name] += len(matches)

    return {k: v for k, v in tag_counts.items() if v > 0}

def count_tags_in_comments(comments, tags_dict):
    """
    Durchsucht rekursiv das Feld article.comments (und darin verschachtelte replies).
    comments ist eine Liste von Kommentar-Objekten:
        [
          {
            "content": "...",
            "replies": [ {...}, {...} ]
          },
          ...
        ]
    Gibt ein Dictionary { tag_name: anzahl_der_vorkommen } zurück.
    """
    aggregated_tags = defaultdict(int)

    for comment in comments:
        content = comment.get("content", "") or ""
        for tag_name, patterns in tags_dict.items():
            for pattern in patterns:
                matches = re.findall(pattern, content, flags=re.IGNORECASE)
                aggregated_tags[tag_name] += len(matches)

        replies = comment.get("replies", [])
        if isinstance(replies, list):
            child_counts = count_tags_in_comments(replies, tags_dict)
            for t_name, cnt in child_counts.items():
                aggregated_tags[t_name] += cnt

    return {k: v for k, v in aggregated_tags.items() if v > 0}

def build_author_to_docs(conn):
    """
    Baut ein Mapping author_name -> set(Dokument-IDs) auf.
    """
    author_to_docs = defaultdict(set)
    logger = logging.getLogger(__name__)

    try:
        author_path = 'article.author.article_origins' if conn.name == "derStandard" else 'article.author'
        cursor_all = conn.find(
            {},
            {"_id": 1, author_path: 1},
            no_cursor_timeout=True
        )

        for doc in cursor_all:
            doc_id = doc["_id"]
            if conn.name == 'derStandard':
                origins_value = doc.get("article", {}).get("author", {}).get("article_origins", [])
            else:
                origins_value = doc.get("article", {}).get("author", {})

            if isinstance(origins_value, str):
                origins_list = [origins_value]
            elif isinstance(origins_value, list):
                origins_list = origins_value
            else:
                continue

            for author_name in origins_list:
                if isinstance(author_name, str):
                    author_to_docs[author_name].add(doc_id)
    except Exception as e:
        logger.error(f"Fehler beim Aufbau des author_to_docs Mappings: {e}", exc_info=True)
    finally:
        cursor_all.close()

    return author_to_docs

def run_keyword_extraction(conn, batch_size=1000):
    """
    Führt die Keyword-Extraction und das Zählen der Artikel pro Autor durch.
    """
    logger = logging.getLogger(__name__)

    # 1. Aufbau des author_to_docs Mappings
    logger.info("Baue author_to_docs Mapping auf...")
    author_to_docs = build_author_to_docs(conn)
    logger.info(f"Anzahl unterschiedlicher Autoren: {len(author_to_docs)}")

    # 2. Definition der Tags und zugehörigen Regex-Patterns
    tags_dict = {
        "OeNB": [r"OeNB", r"Oesterreichische Nationalbank"],
        "digitaler Euro": [r"digitaler? Euro"],
        "Transparenzplattform": [r"Transparenzplattform"],
        "Sparzinsen": [r"Sparzinsen"],
        "Zinssenkung": [r"Zinssenkung"],
        "Zinsanhebung": [r"Zinsanhebung", r"Zinserhöhung"],
        "Inflation": [r"Inflation"],
        "Leitzinsen": [r"Einlagenzins", r"Hauptrefinanzierungssatz", r"Spitzenrefinanzierungssatz", r"Leitzins"],
        "EZB": [r"EZB", r"ECB", r"Europäische Zentralbank"],
    }

    # 3. Filtern der Dokumente, die noch nicht bearbeitet wurden
    filter_query = {
        "$or": [
            {"features.author_article_count": {"$exists": False}},
            {"features.tags_in_article": {"$exists": False}},
            {"features.tags_in_comments": {"$exists": False}}
        ]
    }

    try:
        cursor_main = conn.find(filter_query, no_cursor_timeout=True).batch_size(batch_size)
        bulk_ops = []
        counter = 0

        for doc in cursor_main:
            counter += 1
            doc_id = doc["_id"]
            article_data = doc.get("article", {})

            # (A) author_article_count
            if conn.name == 'derStandard':
                origins = article_data.get("author", {}).get("article_origins", [])
            else:
                origins = article_data.get("author", {})
                
            if isinstance(origins, str):
                origins = [origins]
            elif not isinstance(origins, list):
                origins = []

            author_article_count = []
            for author_name in origins:
                if not isinstance(author_name, str):
                    continue
                all_docs_for_author = author_to_docs.get(author_name, set())
                count_others = len(all_docs_for_author) - 1 if doc_id in all_docs_for_author else len(all_docs_for_author)
                author_article_count.append({
                    "author": author_name,
                    "count": max(0, count_others)
                })

            # (B) tags_in_article
            kicker = article_data.get("kicker", "") or ""
            subtitle = article_data.get("subtitle", "") or ""
            title = article_data.get("title", "") or ""
            text_list = article_data.get("text", [])
            if isinstance(text_list, str):
                text_list = [text_list]
            elif not isinstance(text_list, list):
                text_list = []

            combined_texts = [kicker, subtitle, title] + text_list
            tags_in_article_dict = count_tags_in_texts(combined_texts, tags_dict)

            # (C) tags_in_comments
            comments_list = article_data.get("comments", [])
            if not isinstance(comments_list, list):
                comments_list = []
            tags_in_comments_dict = count_tags_in_comments(comments_list, tags_dict)

            # (D) Bulk-Update vorbereiten
            update_op = UpdateOne(
                {"_id": doc_id},
                {
                    "$set": {
                        "features.author_article_count": author_article_count,
                        "features.tags_in_article": tags_in_article_dict,
                        "features.tags_in_comments": tags_in_comments_dict
                    }
                }
            )
            bulk_ops.append(update_op)

            # Bulk-Write, wenn batch_size erreicht
            if len(bulk_ops) >= batch_size:
                conn.bulk_write(bulk_ops, ordered=False)
                logger.info(f"{counter} Dokumente verarbeitet ...")
                bulk_ops.clear()

        # Restliche Updates
        if bulk_ops:
            conn.bulk_write(bulk_ops, ordered=False)
            logger.info(f"Abschließender Bulk-Write für {len(bulk_ops)} Dokumente.")

        logger.info(f"Fertig! Insgesamt {counter} Dokumente bearbeitet.")

    except Exception as e:
        logger.error(f"Fehler bei der Keyword-Extraction: {e}", exc_info=True)
    finally:
        cursor_main.close()
