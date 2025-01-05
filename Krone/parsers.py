# parsers.py
import re
import dateparser
import inspect
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

def parse_krone_article(soup, logger):
    """
    Parst die wichtigsten Meta-Infos aus einem Krone-Artikel:
      - Premium-Erkennung
      - (Pre)Title, Kicker, Subtitle, Autor, Datum
      - Artikeltext nur, falls kein Premium-Artikel

    Rückgabe: dict mit den Feldern:
      {
        'features.premium': bool,
        'article.title': str oder None,
        'article.kicker': str oder None,
        'article.subtitle': str oder None,
        'article.author': [str, str, ...] oder None,
        'article.pubdate': datetime-Objekt oder None,
        'article.text': [Absatz1, Absatz2, ...]
      }
    """
    data = {}

    # 1) Premium-Check
    try:
        premium_div = soup.find('div', id='paywall-content', attrs={'data-product': 'premium'})
        is_premium = bool(premium_div)
        data['features.premium'] = is_premium
    except Exception as e:
        logger.error(f"[parse_krone_article] Fehler bei Premium-Check: {e}", exc_info=True)
        data['features.premium'] = False

    # 2) Titel
    try:
        title_div = soup.find('div', {'data-nodeid': '10555-94f40e7b'})
        if title_div:
            data['article.title'] = title_div.get_text(strip=True)
        else:
            data['article.title'] = None
            logger.debug("[parse_krone_article] Kein Titel gefunden.")
    except Exception as e:
        logger.error(f"[parse_krone_article] Fehler beim Titel: {e}", exc_info=True)
        data['article.title'] = None

    # 3) Kicker + Datum
    try:
        kicker_pubdate_div = soup.find('div', {'data-nodeid': '10555-9b995933'})
        if kicker_pubdate_div:
            # Kicker
            kicker_el = kicker_pubdate_div.find(class_='bc__link bc__link--shortened')
            if kicker_el:
                data['article.kicker'] = kicker_el.get_text(strip=True)
            else:
                data['article.kicker'] = None

            # Datum
            pubdate_el = kicker_pubdate_div.find('div', class_='bc__date')
            if pubdate_el:
                data['article.pubdate'] = dateparser.parse(pubdate_el.get_text(strip=True), languages=['de'])
            else:
                data['article.pubdate'] = None
        else:
            data['article.kicker'] = None
            data['article.pubdate'] = None
    except Exception as e:
        logger.error(f"[parse_krone_article] Fehler bei Kicker/PubDate: {e}", exc_info=True)
        data['article.kicker'] = None
        data['article.pubdate'] = None

    # 4) Subtitle
    try:
        subtitle_div = soup.find('div', {'data-nodeid': '10555-a75a93ac'})
        if subtitle_div:
            data['article.subtitle'] = subtitle_div.get_text(strip=True)
        else:
            data['article.subtitle'] = None
    except Exception as e:
        logger.error(f"[parse_krone_article] Fehler beim Subtitle: {e}", exc_info=True)
        data['article.subtitle'] = None

    # 5) Autor(en)
    try:
        authors_div = soup.find('div', {'data-nodeid': '10555-ac9231da'})
        if authors_div:
            authors = []
            for author_el in authors_div.findAll('div', class_='al__author'):
                authors.append(author_el.get_text(strip=True))
            data['article.author'] = authors if authors else None
        else:
            data['article.author'] = None
    except Exception as e:
        logger.error(f"[parse_krone_article] Fehler beim Autor-Parsen: {e}", exc_info=True)
        data['article.author'] = None

    # 6) Artikel-Text (nur, wenn kein Premium-Artikel)
    if not data['features.premium']:
        try:
            content_div = soup.find('div', {'data-nodeid': '10555-8d883f15'})
            if content_div:
                paragraphs = []
                for box in content_div.findChildren(attrs={'class': 'box col-xs-12 c_tinymce'}):
                    # <br> ersetzen
                    for br in box.find_all('br'):
                        br.replace_with(". ")
                    paragraphs.append(box.get_text(separator=' ').strip())
                data['article.text'] = paragraphs
            else:
                data['article.text'] = []
                logger.debug("[parse_krone_article] Kein Artikel-Text gefunden.")
        except Exception as e:
            logger.error(f"[parse_krone_article] Fehler beim Parsen des Artikeltexts: {e}", exc_info=True)
            data['article.text'] = []
    else:
        data['article.text'] = []
        logger.info("[parse_krone_article] Premium-Artikel erkannt – Artikeltext wird nicht abgerufen.")

    return data


def parse_krone_comment_section(driver, logger):
    """
    Lädt mithilfe von Selenium die Kommentare (inkl. 'Mehr anzeigen'-Klicks) 
    und parst sie via BeautifulSoup.
    
    Gibt eine Liste (verschachtelter) Kommentare zurück oder [] wenn keine gefunden.
    """
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    comments = []
    try:
        # Warte auf das Element #coral-container (bis zu 30 Sek. z.B.)
        coral_container = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "coral-container"))
        )
        # Scrolle zum Element
        driver.execute_script("arguments[0].scrollIntoView(true);", coral_container)

        # Warte auf das iFrame in #coral-container
        iframe = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#coral-container iframe"))
        )

        # Wechsle ins iFrame
        driver.switch_to.frame(iframe)

        # Klicke ggf. mehrmals auf "Mehr anzeigen"
        more_button_xpath = '/html/body/div[1]/div/div[2]/div/div/div/div[3]/div[2]/div/div/div/div/div[3]/button'
        for _ in range(10):
            try:
                more_button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, more_button_xpath))
                )
                more_button.click()
                logger.debug("[parse_krone_comment_section] Mehr anzeigen geklickt.")
            except Exception:
                # Kein weiterer Button
                break

        # Jetzt HTML aus dem iFrame lesen
        soup_iframe = BeautifulSoup(driver.page_source, 'html.parser')
        # Zurück zum Haupt-Frame
        driver.switch_to.default_content()

        # Finde alle Kommentar-DIVs
        comment_divs = soup_iframe.find_all("div", class_=re.compile(r"talk-stream-comment-wrapper-level-\d+"))
        comments = parse_krone_nested_comments(comment_divs)
    except Exception as e:
        logger.error(f"Fehler beim Laden/Parsen der Krone-Kommentar-Sektion: {e}", exc_info=True)

    return comments


def parse_krone_nested_comments(comment_divs):
    """
    Bekommt eine Liste von <div class="talk-stream-comment-wrapper-level-X"> 
    und baut eine verschachtelte Struktur:
    [
      {
        "commentID": ..., "author": ..., "datetime": ..., 
        "content": ..., "upvotes": int, "downvotes": int,
        "reply_on_comment": None oder commentID, 
        "replies": [...]
      },
      ...
    ]
    """
    root_comments = []
    level_stack = {}

    for wrapper in comment_divs:
        level = get_comment_level(wrapper)
        comment_data = extract_comment_data(wrapper)

        # Stack-Logik
        if level == 0:
            root_comments.append(comment_data)
            level_stack[0] = comment_data
        else:
            parent_comment = level_stack.get(level - 1)
            if parent_comment:
                # setze Parent-Child-Beziehung
                comment_data["reply_on_comment"] = parent_comment["commentID"]
                parent_comment["replies"].append(comment_data)
            else:
                # falls DOM-Struktur nicht konsistent ist
                root_comments.append(comment_data)
            level_stack[level] = comment_data

    return root_comments


def get_comment_level(wrapper):
    """
    Liest die Nummer aus dem Klassennamen, z.B. talk-stream-comment-wrapper-level-2 -> 2
    """
    classes = wrapper.get("class", [])
    for c in classes:
        if c.startswith("talk-stream-comment-wrapper-level-"):
            try:
                return int(c.replace("talk-stream-comment-wrapper-level-", ""))
            except:
                return 0
    return 0


def extract_comment_data(wrapper):
    """
    Extrahiert ID, Autor, Datum, Up-/Downvotes und Kommentartext.
    """
    import dateparser

    # Kommentar-ID
    comment_id = wrapper.get("id", "").strip()

    # Autor
    author_tag = wrapper.find("span", class_="AuthorName__name___3O4jF")
    author = author_tag.get_text(strip=True) if author_tag else "Unbekannt"

    # Datum (title-Attribut in <span class="TimeAgo__timeago ...">)
    import re
    timestamp_tag = wrapper.find("span", class_=re.compile(r"TimeAgo__timeago"))
    comment_datetime_str = timestamp_tag.get("title", "").strip() if timestamp_tag else ""
    datetime_obj = dateparser.parse(comment_datetime_str, languages=['de'])

    # Up-/Downvotes
    upvote_span = wrapper.find("span", class_="talk-plugin-upvote-count")
    downvote_span = wrapper.find("span", class_="talk-plugin-downvote-count")
    upvotes = parse_vote_count(upvote_span)
    downvotes = parse_vote_count(downvote_span)

    # Inhalt (alle <span class="Linkify">)
    linkify_spans = wrapper.find_all("span", class_="Linkify")
    lines = [sp.get_text(strip=True) for sp in linkify_spans]
    content = "\n".join(filter(None, lines)).strip()

    return {
        "commentID": comment_id,
        "author": author,
        "datetime": datetime_obj,
        "content": content,
        "upvotes": upvotes,
        "downvotes": downvotes,
        "reply_on_comment": None,
        "replies": []
    }


def parse_vote_count(span_tag):
    if not span_tag or not span_tag.text.strip():
        return 0
    try:
        return int(span_tag.text.strip())
    except ValueError:
        return 0
