import re
import datetime
import dateparser
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import inspect
from utils import expand_shadow_element


def parse_posting(posting_element, logger):
    # Parst ein einzelnes <dst-posting>-Element und extrahiert die relevanten Daten.
    try:
        # Extrahiere den Autor
        author = "Unbekannter Benutzer"
        try:
            usermenu = posting_element.find_element(By.CSS_SELECTOR, "dst-posting--user button")
            spans = usermenu.find_elements(By.CSS_SELECTOR, "span > span")
            if spans:
                author = spans[0].text.strip()
        except NoSuchElementException:
            pass

        # Extrahiere die Anzahl der Follower
        user_followers = 0
        try:
            followers_div = posting_element.find_element(By.CSS_SELECTOR, "dst-posting--user button div[title]")
            followers_text = followers_div.get_attribute("title")
            followers_match = re.search(r'\d+', followers_text)
            if followers_match:
                user_followers = int(followers_match.group())
        except NoSuchElementException:
            pass
        except ValueError:
            logger.warning(f"{inspect.currentframe().f_back.f_code.co_name} Ungültige Follower-Zahl: '{followers_text}'.")

        # Extrahiere das Datum und die Uhrzeit
        datetime_obj = None
        try:
            time_tag = posting_element.find_element(By.CSS_SELECTOR, "time[data-date]")
            datetime_str = time_tag.get_attribute("data-date")
            datetime_obj = dateparser.parse(datetime_str, languages=['de'])
        except NoSuchElementException:
            pass

        # Extrahiere den Inhalt des Postings
        content = ""
        try:
            content_div = posting_element.find_element(By.CSS_SELECTOR, "div.posting--content")
            headers = content_div.find_elements(By.TAG_NAME, "h1")
            paragraphs = content_div.find_elements(By.TAG_NAME, "p")
            header_text = "\n".join([h.text for h in headers]) if headers else ""
            paragraph_text = "\n".join([p.text for p in paragraphs]) if paragraphs else ""
            content = "\n".join([header_text, paragraph_text]).strip()
        except NoSuchElementException:
            pass

        # Extrahiere Upvotes und Downvotes
        upvotes = 0
        downvotes = 0
        try:
            ratinglog = posting_element.find_element(By.CSS_SELECTOR, "dst-posting--ratinglog")
            positiveratings = ratinglog.get_attribute("positiveratings")
            negativeratings = ratinglog.get_attribute("negativeratings")
            upvotes = int(positiveratings) if positiveratings and positiveratings.isdigit() else 0
            downvotes = int(negativeratings) if negativeratings and negativeratings.isdigit() else 0
        except NoSuchElementException:
            pass
        except ValueError:
            logger.warning(f"{inspect.currentframe().f_back.f_code.co_name} Ungültige Upvote/Downvote-Zahlen gefunden.")

        # Extrahiere Parent-Kommentar-ID (falls Antwort)
        parent_id = posting_element.get_attribute("data-parentpostingid")
        reply_on_comment = int(parent_id) if parent_id and parent_id.isdigit() else None

        # Extrahiere Kommentar-ID
        commentID = posting_element.get_attribute("data-postingid")
        commentID = int(commentID) if commentID and commentID.isdigit() else None

        # Erstelle das Kommentar-Dictionary
        comment = {
            'commentID': commentID,
            'author': author,
            'user_followers': user_followers,
            'datetime': datetime_obj,
            'content': content,
            'upvotes': upvotes,
            'downvotes': downvotes,
            'reply_on_comment': reply_on_comment,
            'replies': []
        }

        return comment

    except Exception as e:
        logger.error(f"{inspect.currentframe().f_back.f_code.co_name} Fehler beim Parsen eines Postings: {e}", exc_info=True)
        return None

def parse_comment_datetime(datetime_str):
    return dateparser.parse(datetime_str, languages=['de'])

def get_article_byline(soup, logger):
    article_byline = {}
    article_byline_tag = soup.find('div', class_='article-byline')
    if article_byline_tag:
        # Storylabels extrahieren
        storylabels_tag = article_byline_tag.find('div', class_='storylabels')
        if storylabels_tag:
            storylabels = storylabels_tag.get_text(strip=True)
            article_byline['storylabels'] = storylabels

        # Article origins extrahieren
        article_origins_tag = article_byline_tag.find('div', class_='article-origins')
        if article_origins_tag:
            article_origins = article_origins_tag.get_text(strip=True)
            article_byline['article_origins'] = article_origins
        else:
            # Fallback für einfachen Autorentext
            author_simple = article_byline_tag.find('span', class_='simple')
            if author_simple:
                article_byline['article_origins'] = author_simple.get_text(strip=True)
    else:
        article_byline = None
        logger.debug(f"{inspect.currentframe().f_back.f_code.co_name} Keine Artikel-Byline gefunden.")
    return article_byline





def get_article_datetime(soup, logger):
    
    time_elements = [
        soup.select_one("p.article-pubdate time"),
        soup.find('time', class_='article-pubdate')
    ]

    for time_element in time_elements:
        if time_element:
            # Try to get the 'datetime' attribute
            datetime_str = time_element.get("datetime", "").strip() or time_element.get_text(strip=True)
            datetime_str = datetime_str.replace('\n', '').strip()

            # parse the datetime string
            try:
                return datetime.datetime.fromisoformat(datetime_str)
            except (TypeError, ValueError):
                article_datetime = dateparser.parse(datetime_str, languages=['de'])
                if article_datetime:
                    return article_datetime

    # Log if no datetime was found
    logger.debug(f"{inspect.currentframe().f_back.f_code.co_name} Kein Datum gefunden.")
    return None





def get_posting_count(soup, full_url, logger):
    posting_count = None
    try:
        posting_count_tag = soup.find('span', class_='js-forum-postingcount')
        if posting_count_tag:
            posting_count_text = posting_count_tag.contents[0].strip()
            posting_count = int(posting_count_text)
            return posting_count
    except (AttributeError, ValueError):
        posting_count = None
    try:
        community_section = soup.find('section', id='story-community')
        header_div = community_section.find('div', class_='story-community-header')
        h1_tag = header_div.find('h1')
        h1_text = h1_tag.get_text(strip=True)
        match = re.search(r'Forum:\s*(\d+)\s*Postings', h1_text)
        if match:
            posting_count = int(match.group(1))
            return posting_count
    except (AttributeError, ValueError):
        posting_count = None
        logger.warning(f"{inspect.currentframe().f_back.f_code.co_name} Ungültige Posting-Anzahl in {full_url}")
    return posting_count

def get_paragraph_texts(soup, full_url, logger):
    # Artikelinhalt extrahieren
    paragraph_texts = None
    try:
        article_body = soup.find('div', class_='article-body')
        if article_body:

            logger.debug(f"{inspect.currentframe().f_back.f_code.co_name} Alle 'href'-Attribute aus Artikeltext entfernt.")

            # Unerwünschte Elemente entfernen
            for ad in article_body.find_all(['ad-container', 'ad-slot', 'ad', 'native-ad']):
                ad.decompose()
            for ad in article_body.find_all("div", class_="native-ad"):
                ad.decompose()
            for figure in article_body.find_all('figure'):
                figure.decompose()
            for unwanted in article_body.find_all(['aside', 'nav', 'div'], attrs={'data-section-type': 'supplemental'}):
                unwanted.decompose()

            # Alle 'href'-Attribute entfernen
            for a_tag in article_body.find_all('a'):
                a_tag['href'] = ''

            logger.debug(f"{inspect.currentframe().f_back.f_code.co_name} Unerwünschte Elemente aus Artikeltext entfernt.")

            # Paragraphen extrahieren und in Liste umwandeln
            paragraphs = article_body.find_all('p')
            paragraph_texts = [p.get_text() for p in paragraphs]
            logger.debug(f"{inspect.currentframe().f_back.f_code.co_name} Extrahierte Paragraphen: {len(paragraph_texts)} in {full_url}")
        else:
            logger.debug(f"{inspect.currentframe().f_back.f_code.co_name} Kein Artikelinhalt gefunden in {full_url}")
    except Exception as e:
        logger.error(f"{inspect.currentframe().f_back.f_code.co_name} Fehler beim Extrahieren des Artikelinhalts: {e}", exc_info=True)
    return paragraph_texts


def extract_reactions(driver, logger):
    """
    Extrahiert die Reaktionen aus dem Shadow DOM der aktuellen Seite.
    Gibt ein Tuple zurück: (reactions_dict, warning_flag)
    """
    try:
        shadow_host = driver.find_element(By.CSS_SELECTOR, "dst-community-reactions")
        shadow_root = expand_shadow_element(driver, shadow_host)
        reactions_buttons = shadow_root.find_elements(By.CSS_SELECTOR, "aside.reactions div.reactions--buttons button")
        reactions = {}
        for button in reactions_buttons:
            try:
                count_element = button.find_element(By.TAG_NAME, "strong")
                count = int(count_element.text.strip())
            except (NoSuchElementException, ValueError):
                count = 0
            try:
                sr_only = button.find_element(By.CSS_SELECTOR, "span.sr-only")
                reaction_name = sr_only.text.strip()
            except NoSuchElementException:
                reaction_name = button.text.replace(str(count), '').strip()
            reactions[reaction_name] = count
        return reactions, False  # Kein Warnhinweis notwendig
    except NoSuchElementException:
        logger.warning("Reaktionen konnten nicht extrahiert werden.")
        return None, True  # Warnhinweis setzen
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren der Reaktionen: {e}", exc_info=True)
        return None, True  # Warnhinweis setzen
    


def extract_forum_comments_normal(driver, logger, max_comments=70):
    """
    Extrahiert Benutzerkommentare aus dem Shadow DOM der aktuellen Seite und
    bildet verschachtelte Antworten ab.
    Gibt ein Tuple zurück: (comments_list, warning_flag)
    """
    comments = []
    count = 0
    try:
        forum_host = driver.find_element(By.CSS_SELECTOR, "dst-forum")
        forum_shadow = expand_shadow_element(driver, forum_host)
        main_content = forum_shadow.find_element(By.CSS_SELECTOR, "main.forum--main")
        children = main_content.find_elements(By.CSS_SELECTOR, ":scope > *")
        current_parent = None

        for child in children:
            if count >= max_comments:
                break
            tag_name = child.tag_name.lower()
            if tag_name == "dst-posting":
                comment = parse_posting(child, logger)
                if comment:
                    comments.append(comment)
                    current_parent = comment
                    count += 1
            elif tag_name == "section":
                classes = child.get_attribute("class")
                if classes and "thread" in classes:
                    if not current_parent:
                        logger.warning("Thread-Sektion gefunden, aber kein aktueller Parent.")
                        continue
                    reply_postings = child.find_elements(By.CSS_SELECTOR, "dst-posting")
                    for reply in reply_postings:
                        if count >= max_comments:
                            break
                        reply_comment = parse_posting(reply, logger)
                        if reply_comment:
                            current_parent['replies'].append(reply_comment)
                            count += 1
        return comments, False  # Kein Warnhinweis notwendig
    except NoSuchElementException:
        logger.warning("Forum-Elemente nicht gefunden.")
        return [], True  # Warnhinweis setzen
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren der Forenkommentare: {e}", exc_info=True)
        return [], True  # Warnhinweis setzen
    

def extract_forum_comments_alternative(driver, logger, max_comments=70):
    """
    Extrahiert Benutzerkommentare aus der aktuellen Seite unter Verwendung von BeautifulSoup
    und bildet verschachtelte Antworten ab.
    """
    comments_data = []
    comment_map = {}
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    postings = soup.find_all('div', class_='posting', attrs={'data-postingid': True})

    if not postings:
        logger.warning("Forum-Elemente nicht gefunden.")
        return comments_data, True  # Warnhinweis setzen


    for posting in postings[:max_comments]:
        try:
            commentID = posting.get('data-postingid')
            if not commentID or not commentID.isdigit():
                continue
            commentID = int(commentID)

            username = posting.get('data-communityname') or 'gelöschtes Profil'

            reply_on_comment = posting.get('data-parentpostingid')
            reply_on_comment = int(reply_on_comment) if reply_on_comment and reply_on_comment.isdigit() else None

            # Datum und Uhrzeit des Kommentars extrahieren
            datetime_tag = posting.find('span', class_='js-timestamp')
            if datetime_tag and datetime_tag.text:
                datetime_str = datetime_tag.text.strip()
                datetime_obj = parse_comment_datetime(datetime_str)
            else:
                datetime_obj = None 

            # Kommentarüberschrift extrahieren
            comment_header_tag = posting.find('h4', class_='upost-title')
            comment_header = comment_header_tag.text.strip() if comment_header_tag else ""

            # Kommentartext extrahieren
            comment_body = posting.find('div', class_='upost-text')
            comment_text = comment_body.get_text(separator=' ', strip=True) if comment_body else ""

            # Upvotes extrahieren
            upvotes_tag = posting.find('span', class_='js-ratings-positive-count')
            upvotes = int(upvotes_tag.text.strip()) if upvotes_tag and upvotes_tag.text.isdigit() else 0

            # Downvotes extrahieren
            downvotes_tag = posting.find('span', class_='js-ratings-negative-count')
            downvotes = int(downvotes_tag.text.strip()) if downvotes_tag and downvotes_tag.text.isdigit() else 0

            # Anzahl der Follower des Nutzers extrahieren
            user_followers_tag = posting.find('span', class_='upost-follower')
            user_followers = int(user_followers_tag.text.strip()) if user_followers_tag and user_followers_tag.text.isdigit() else 0

            comment_data = {
                'commentID': commentID,
                'author': username,
                'user_followers': user_followers,
                'datetime': datetime_obj,
                'content': f"{comment_header}\n{comment_text}".strip(),
                'upvotes': upvotes,
                'downvotes': downvotes,
                'reply_on_comment': reply_on_comment,
                'replies': []
            }

            # In die Map einfügen
            comment_map[commentID] = comment_data

        except Exception as e:
            logger.error(f"Fehler beim Verarbeiten eines Kommentars: {e}", exc_info=True)
            continue 

    # Jetzt die verschachtelte Struktur aufbauen
    for comment in comment_map.values():
        parent_id = comment['reply_on_comment']
        if parent_id and parent_id in comment_map:
            parent_comment = comment_map[parent_id]
            parent_comment['replies'].append(comment)
        else:
            comments_data.append(comment)

    return comments_data, False  
