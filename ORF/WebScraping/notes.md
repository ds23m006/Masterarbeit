# Herausforderungen
- Suchfunktion fast nirgends verfügbar
    https://oesterreich.orf.at/elasticsearch?query=test (nicht verfügbar)
    
    https://help.orf.at/elasticsearch?query=*&from=0&sort=date (verfügbar)
    https://science.orf.at/elasticsearch?query=*&from=0&sort=date (verfügbar)


- Kein Archiv
    siehe https://zukunft.orf.at/rte/upload/2023/veroeffentlichungen/veroeffentlichungen_010124/angebotskonzept_news_vom_15-10-2023.pdf

- Daher kontinuierliches Scraping notwendig
    https://rss.orf.at/

Historie ist nur sehr eingeschränkt verfügbar

Daher folgendes vorgehen:
1. Für 'Help' und 'Science': Suche mit elasticsearch nach q=* und scraped diese Seite.
2. Für news.orf: 
    scrape alle Artikel von XXXXXX bis YYYYYY.
    https://science.orf.at/stories/XXXXXX/
    https://science.orf.at/stories/YYYYYY/
    Suche händisch nach passenden XXXXXX und YYYYYY

    






