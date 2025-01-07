### Url Scraping

Beim ersten Durchlauf werden alle URLs ab 2020 über die Sitemap heruntergeladen. Nach dem Deployment werden neue URLs täglich heruntergeladen (cron-job)

### Scraping Prozess

Nachdem die Seite wieder relativ dynamisch geladen wird, muss mit Selenium gescraped werden. BeautifulSoup wird für das parsing verwendet.

- Zugriff auf Elemente via `data-nodeid`.
