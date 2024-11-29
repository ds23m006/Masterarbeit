# README: Web Scraping für Masterarbeit

## Projektübersicht

Dieses Projekt dient der Implementierung und Ausführung von Web-Scraping-Mechanismen für die Masterarbeit **"Media Coverage on Financial Innovations: An NLP-Based Analysis"**. Ziel ist es, Artikel und Diskussionen aus Medienquellen zu extrahieren, die öffentliche Meinungen und Emotionen zu Finanzinnovationen widerspiegeln. Die gewonnenen Daten werden in einer MongoDB-Datenbank gespeichert und später für Natural Language Processing (NLP)-Analysen wie Sentiment-, Stance- und Emotion-Analysen genutzt.

---

## Allgemeines

Web-Scraping lässt sich in zwei Hauptmethoden unterteilen [1]:

1. **Screen Scraping:** Daten werden aus HTML-Quelltexten mithilfe eines Parsers (z. B. BeautifulSoup) oder regulären Ausdrücken extrahiert. Diese Methode ist besonders bei unstrukturierten oder semistrukturierten Daten notwendig.
2. **API-Scraping:** Hierbei werden strukturierte Daten (z. B. JSON oder XML) über eine API abgefragt. APIs ermöglichen einen standardisierten und effizienten Zugriff auf Inhalte.

In der Praxis wird API-Scraping bevorzugt, sofern APIs verfügbar sind. Für dynamische Inhalte, die über JavaScript geladen werden, kommen Tools wie Selenium oder Scrapy zum Einsatz, da herkömmliche HTTP-Bibliotheken wie `requests` hier nicht ausreichen. Aufgrund der höheren Performance wird dynamisches Scraping nur als letzter Ausweg genutzt.

### URL-Ermittlung

Der erste Schritt im Web-Scraping besteht darin, relevante URLs zu finden. Dafür werden folgende Ansätze verwendet:

1. **Sitemap-Erkennung:** Überprüfung der `robots.txt` (z. B. [orf.at/robots.txt](https://orf.at/robots.txt)), um Verweise auf Sitemaps zu identifizieren. Sitemaps enthalten oft strukturierte Informationen über alle verfügbaren Seiten einer Website.
2. **Archivsuche:** Wenn keine Sitemap vorhanden ist, werden Archive durchsucht. Dabei können URL-Parameter helfen, um Inhalte effizient zu iterieren.
3. **Dynamisches Crawling:** Für Inhalte, die nur durch Scrollen geladen werden, wird Selenium eingesetzt.

### Datenbankintegration

Die extrahierten Daten werden in einer **MongoDB** gespeichert. Für jede Quelle wird eine eigene Collection in der Datenbank `newspapers` erstellt, um unterschiedliche Strukturen abzubilden. URLs werden mit den Attributen `scraping_info.status=None` und `scraping_info.download_datetime=None` initialisiert. Während des Scraping-Prozesses werden diese Felder aktualisiert, um den Fortschritt und Erfolg zu dokumentieren.

---

## Hauptbestandteile

### 1. **Code-Struktur**

- **`config.py`**  
  Enthält Konfigurationsparameter wie den Pfad zum Chromedriver, User-Agent-Strings und Umgebungsvariablen.

- **`database.py`**  
  Verbindet das Projekt sicher mit der MongoDB-Datenbank über Umgebungsvariablen.

- **`driver.py`**  
  Initialisiert einen Selenium-Webdriver mit Optionen wie headless Browsing, User-Agent-Spoofing und Popup-Handling.

- **`logger_setup.py`**  
  Richtet ein flexibles Logger-System ein, das Logs sowohl in der Konsole als auch in Dateien speichert (z. B. `scraper.log`).

- **`main.py`**  
  Hauptskript, das den Scraping-Prozess zyklisch ausführt. URLs werden aus der Datenbank geladen, verarbeitet und die Ergebnisse gespeichert.

- **`scraper.py`**  
  Beinhaltet die Kernlogik für das Scraping. Nutzt `aiohttp` für asynchrone HTTP-Anfragen und BeautifulSoup für die HTML-Analyse.

- **`parsers.py`**  
  Enthält spezifische Parser für Inhalte wie Artikel, Kommentare und Metadaten. Unterstützt die Verarbeitung verschachtelter Forenstrukturen.

---

### 2. **Datenpipeline**

Der gesamte ETL-Prozess umfasst:

1. **Extraktion:**  
   URLs werden aus Sitemaps, Archiven oder der Datenbank abgerufen. Dynamische Inhalte werden durch Selenium geladen.
   
2. **Transformation:**  
   Inhalte wie Titel, Veröffentlichungsdatum und Kommentare werden geparst, bereinigt und in ein einheitliches Format gebracht.

3. **Laden:**  
   Die transformierten Daten werden in der MongoDB gespeichert. Der Status des Scraping-Vorgangs wird dokumentiert (`success`, `error`).

---

## Herausforderungen

### Kein Nachrichtenarchiv (z. B. ORF)

Eine besondere Herausforderung ergibt sich beim ORF. Laut dem **Angebotskonzept für news.ORF.at** gibt es kein Nachrichtenarchiv:

> *„Die Berichterstattung gibt insgesamt einen Überblick über das aktuelle Nachrichtengeschehen, ohne dabei vertiefend zu sein oder ein Nachrichtenarchiv zu beinhalten. \[...\] Ein Nachrichtenarchiv wird nicht angeboten.“*  
> (Quelle: [ORF Angebotskonzept](https://zukunft.orf.at/rte/upload/2023/veroeffentlichungen/veroeffentlichungen_010124/angebotskonzept_news_vom_15-10-2023.pdf))

Das bedeutet, dass kontinuierliches Scraping erforderlich ist, um Artikel und Inhalte rechtzeitig zu erfassen, bevor sie von der Website entfernt werden.

Für historische Artikel kann man über die URL die Parameter variieren, um unterschiedliche Artikel zu bekommen. Auch wenn auf diese Weise nicht alle Artikel erfasst werden können, lässt sich dennoch ein großer Teil abdecken.

### Dynamische Inhalte

Viele Websites laden Inhalte nur bei Benutzerinteraktion. Selenium wird hier eingesetzt, obwohl es ressourcenintensiver ist.

### Rate-Limiting und Blocker

Um Sperren zu vermeiden, werden User-Agents gewechselt, Requests gedrosselt und Proxys verwendet.

---

## Literaturverzeichnis
[1] M. Dogucu und M. Çetinkaya-Rundel, „Web Scraping in the Statistics and Data Science Curriculum: Challenges and Opportunities“, Journal of Statistics and Data Science Education, Bd. 29, Nr. sup1, S. S112–S122, Jan. 2021, doi: 10.1080/10691898.2020.1787116.
