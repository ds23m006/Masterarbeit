import re
import dateparser
from bs4 import BeautifulSoup
import inspect
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

def parse_krone_article(soup, logger):
    """
    Parst die wichtigsten Artikelinformationen aus dem BeautifulSoup-Objekt.
    Gibt ein Dict zurück mit Titel, Kicker, Subtitle, Autor(en), Datum, Textblöcken etc.
    """
    data = {}

    # Beispiel: Titel, Kicker, Subtitle (data-nodeid)
    try:
        title_div = soup.find('div', {'data-nodeid': '10555-94f40e7b'})
        if title_div:
            data['article.title'] = title_div.get_text(strip=True)
        else:
            logger.debug(f"{inspect.currentframe().f_code.co_name} Kein Titel gefunden.")

        kicker_pubdate_div = soup.find('div', {'data-nodeid': '10555-9b995933'})
        if kicker_pubdate_div:
            # Kicker
            kicker_el = kicker_pubdate_div.find(class_='bc__link bc__link--shortened')
            if kicker_el:
                data['article.kicker'] = kicker_el.get_text(strip=True)

            # Datum
            pubdate_el = kicker_pubdate_div.find('div', class_='bc__date')
            if pubdate_el:
                data['article.pubdate'] = dateparser.parse(pubdate_el.get_text(strip=True), languages=['de'])

        # Subtitle
        subtitle_div = soup.find('div', {'data-nodeid': '10555-a75a93ac'})
        if subtitle_div:
            data['article.subtitle'] = subtitle_div.get_text(strip=True)

        # Autor(en)
        authors_div = soup.find('div', {'data-nodeid': '10555-ac9231da'})
        if authors_div:
            authors = []
            for author_el in authors_div.findAll('div', class_='al__author'):
                authors.append(author_el.get_text(strip=True))
            data['article.author'] = authors if authors else None

        # Article-Text
        content_div = soup.find('div', {'data-nodeid': '10555-8d883f15'})
        if content_div:
            # Sammeln aller Teil-Blöcke
            paragraphs = []
            for box in content_div.findChildren(attrs={'class': 'box col-xs-12 c_tinymce'}):
                # <br> durch Satzende ersetzen
                for br in box.find_all('br'):
                    br.replace_with(". ")
                paragraphs.append(box.get_text(separator=' ').strip())
            data['article.text'] = paragraphs
        else:
            logger.debug(f"{inspect.currentframe().f_code.co_name} Kein Artikel-Text gefunden.")

    except Exception as e:
        logger.error(f"Fehler beim Parsen des Artikels: {e}", exc_info=True)

    return data


def parse_krone_comment_section(driver, logger):
    """
    Lädt mithilfe von Selenium die Kommentare (inkl. 'Mehr anzeigen' Klicks) und parst sie.
    Gibt eine Liste verschachtelter Kommentar-Dictionaries zurück.
    """
    from bs4 import BeautifulSoup
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    # Versuche, den iFrame zu finden, in dem die Kommentare geladen werden
    try:
        coral_container = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "coral-container"))
        )
        # Scrolle zum Element
        driver.execute_script("arguments[0].scrollIntoView(true);", coral_container)

        # Warte auf das iFrame
        iframe = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#coral-container iframe"))
        )

        # Wechsel in das iFrame
        driver.switch_to.frame(iframe)

        # Wiederholt "Mehr anzeigen"-Button klicken (falls vorhanden)
        more_button_xpath = '/html/body/div[1]/div/div[2]/div/div/div/div[3]/div[2]/div/div/div/div/div[3]/button'
        for _ in range(10):
            try:
                more_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, more_button_xpath))
                )
                more_button.click()
                logger.debug("Mehr anzeigen geklickt.")
            except Exception:
                # Kein weiterer Button gefunden
                break

        # Lade das DOM in BeautifulSoup
        spicy_soup = BeautifulSoup(driver.page_source, 'html.parser')
        # Kehre anschließend zurück zum Haupt-Frame
        driver.switch_to.default_content()

        # Suche nach allen Kommentar-Wrappern
        all_comment_divs = spicy_soup.find_all(
            "div", 
            class_=re.compile(r"talk-stream-comment-wrapper-level-\d+")
        )
        comments = parse_krone_nested_comments(all_comment_divs)
        return comments

    except Exception as e:
        logger.error(f"Fehler beim Laden/Parsens der Krone-Kommentar-Sektion: {e}", exc_info=True)
        return []


def parse_krone_nested_comments(comment_divs):
    """
    Erwartet eine Liste von BeautifulSoup-Elementen (alle Level-Wrapper).
    Baut daraus mithilfe eines Stack-Ansatzes eine verschachtelte Struktur:
    [
      { "commentID": ..., "author": ..., "datetime": ..., "content": ..., "upvotes": ..., 
        "downvotes": ..., "reply_on_comment": None, "replies": [...] },
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
    Liest die Kommentar-Ebene (Level 0,1,2,...) aus einer Klasse
    wie 'talk-stream-comment-wrapper-level-1' aus.
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
    Extrahiert alle relevanten Informationen eines Einzel-Kommentars.
    """
    import re

    comment_id = wrapper.get("id", "").strip()

    # Autor
    author_tag = wrapper.find("span", class_="AuthorName__name___3O4jF")
    author = author_tag.get_text(strip=True) if author_tag else "Unbekannt"

    # Datum aus dem title-Attribut (z.B. title="4.1.2025, 22:56:59")
    timestamp_tag = wrapper.find("span", class_=re.compile(r"TimeAgo__timeago"))
    comment_datetime = timestamp_tag.get("title", "").strip() if timestamp_tag else ""
    datetime_obj = dateparser.parse(comment_datetime, languages=['de'])

    # Up-/Downvotes
    upvote_tag = wrapper.find("span", class_="talk-plugin-upvote-count")
    downvote_tag = wrapper.find("span", class_="talk-plugin-downvote-count")
    upvotes = parse_vote_count(upvote_tag)
    downvotes = parse_vote_count(downvote_tag)

    # Kommentartext 
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
    """
    Wandelt den Text eines <span> in eine int-Zahl um. (Up/Downvotes)
    """
    if not span_tag or not span_tag.text.strip():
        return 0
    try:
        return int(span_tag.text.strip())
    except ValueError:
        return 0
