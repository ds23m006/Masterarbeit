import os
from pymongo import MongoClient


def get_docs(collection):
    """
    Liefert die Dokumente einer Collection zurück.
    :param collection: Collection
    :return: Liste von Dokumenten
    """
    USERNAME = os.getenv("MONGODB_USER")
    PASSWORD = os.getenv("MONGODB_PWD")
    client = MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@BlackWidow:27017")
    db = client['newspapers']

    # Zugriff auf die Collection 'derStandard'
    collection = db[collection]

    # Abfrage der Dokumente mit befülltem 'features.APA_OeNB_Sentiment'
    docs_cursor = collection.find({"features.APA_OeNB_Sentiment": {"$exists": True, "$ne": None}})
    
    return docs_cursor, collection 


def load_sentiws():
    """
    Liest die beiden SentiWS-Dateien ein und gibt ein Dictionary form -> score zurück.
    """
    # pfade zu den SentiWS-Dateien
    path_positive = os.getenv("SENTIWS_PATH_POS")
    path_negative = os.getenv("SENTIWS_PATH_NEG")

    # initialisiere Dictionary
    sentiws = {}

    def process_file(file_path):
        with open(file_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                # Beispielzeile: fröhlich|ADJ    0.749    froh fröhlicher fröhlichste
                parts = line.split('\t')
                if len(parts) < 3:
                    continue

                # parts[0] -> LEMMA|POS, parts[1] -> SCORE, parts[2] -> Liste Wortformen
                lemma_str = parts[0].split("|")[0].strip()
                score_str = parts[1].strip()
                variants_str = parts[2].strip()

                try:
                    score = float(score_str.replace(',', '.'))
                except ValueError:
                    continue
                
                # Grundform
                sentiws[lemma_str.lower()] = score

                # Varianten
                variants = variants_str.split(",")
                for w in variants:
                    w_lower = w.lower()
                    sentiws[w_lower] = score

    # Positive und negative Datei einlesen
    process_file(path_positive)
    process_file(path_negative)

    return sentiws



ENTITY_RULER_PATTERNS = [
    #
    # 1) KURZFORMEN / ALLEINSTEHEND
    #
    # -----------------------------
    # (a) "OeNB" allein
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": "oenb"}
        ]
    },
    # (b) "Österreichische Nationalbank", "Österreichische Notenbank",
    #     "Oesterreichische Nationalbank", "oesterreichische notenbank" etc.
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": {"REGEX": r"^(ö|oe)sterreichische(n|r)?$"}},
            {"LOWER": {"REGEX": r"^(nationalbank|notenbank)$"}}
        ]
    },

    #
    # 2) GENERALRAT
    #
    # -----------------------------
    # (a) "Generalrat der OeNB"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": "generalrat"},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": "oenb"}
        ]
    },
    # (b) "Generalrat der Österreichischen Nationalbank" bzw. "Österreichische Notenbank"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": "generalrat"},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": {"REGEX": r"^(ö|oe)sterreichische(n|r)?$"}},
            {"LOWER": {"REGEX": r"^(nationalbank|notenbank)$"}}
        ]
    },

    #
    # 3) DIREKTORIUM
    #
    # -----------------------------
    # (a) "Direktorium der OeNB"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": "direktorium"},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": "oenb"}
        ]
    },
    # (b) "Direktorium der Österreichischen Nationalbank"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": "direktorium"},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": {"REGEX": r"^(ö|oe)sterreichische(n|r)?$"}},
            {"LOWER": {"REGEX": r"^(nationalbank|notenbank)$"}}
        ]
    },

    #
    # 4) DIREKTOR / DIREKTORIN
    #
    # -----------------------------
    # (a) "Direktor der OeNB"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": {"REGEX": r"^direktor(in)?$"}},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": "oenb"}
        ]
    },
    # (b) "Direktor der Österreichischen Nationalbank"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": {"REGEX": r"^direktor(in)?$"}},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": {"REGEX": r"^(ö|oe)sterreichische(n|r)?$"}},
            {"LOWER": {"REGEX": r"^(nationalbank|notenbank)$"}}
        ]
    },

    #
    # 5) GOUVERNEUR / GOUVERNEURIN
    #
    # -----------------------------
    # (a) "Gouverneur der OeNB"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": {"REGEX": r"^gouverneur(in)?$"}},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": "oenb"}
        ]
    },
    # (b) "Gouverneur der Österreichischen Nationalbank"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": {"REGEX": r"^gouverneur(in)?$"}},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": {"REGEX": r"^(ö|oe)sterreichische(n|r)?$"}},
            {"LOWER": {"REGEX": r"^(nationalbank|notenbank)$"}}
        ]
    },

    #
    # 6) VIZE-GOUVERNEUR / VIZE-GOUVERNEURIN
    #
    # -----------------------------
    # (a) "Vize-Gouverneur der OeNB"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": {"REGEX": r"^vize(\-)?gouverneur(in)?$"}},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": "oenb"}
        ]
    },
    # (b) "Vize-Gouverneur der Österreichischen Nationalbank"
    {
        "label": "OeNB",
        "pattern": [
            {"LOWER": {"REGEX": r"^vize(\-)?gouverneur(in)?$"}},
            {"LOWER": "der", "OP": "?"},
            {"LOWER": {"REGEX": r"^(ö|oe)sterreichische(n|r)?$"}},
            {"LOWER": {"REGEX": r"^(nationalbank|notenbank)$"}}
        ]
    }
]