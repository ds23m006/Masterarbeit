# URLs scrapen

## Sitemap Variante

1. robots.txt nach sitemap durchsuchen
2. wenn sitemap gefunden, dann über sitemap iterieren und URLs scrapen

## Archiv Variante

Wenn keine sitemap gefunden werden konnte, dann muss man im Archiv über alle Seiten iterieren und von jedem Eintrag die URL abspeichern

# Scrape pages

for url in URLs:
    scrape(page(url))
