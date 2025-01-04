import time
import datetime
from selenium import webdriver as wd
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from config import CHROMEDRIVER_PATH, USER_AGENT, FRONTPAGE_URL

def configure_driver(headless=True):
    """
    Erstellt und konfiguriert den WebDriver für krone.at.
    Klickt ggf. das Cookie-Popup weg.
    """
    chrome_options = wd.ChromeOptions()
    if headless:
        # In neueren Chrome-Versionen: "--headless=new" 
        chrome_options.add_argument("--headless")
    chrome_options.add_argument(f"--user-agent={USER_AGENT}")
    chrome_options.add_argument('--allow-running-insecure-content')
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # Bilder deaktivieren
    chrome_prefs = {
        "profile.default_content_settings.images": 2,
        "profile.managed_default_content_settings.images": 2
    }
    chrome_options.experimental_options["prefs"] = chrome_prefs

    service = ChromeService(executable_path=CHROMEDRIVER_PATH)
    driver = wd.Chrome(service=service, options=chrome_options)

    # Besuche die Startseite, um das Cookie-Popup zu schließen
    driver.get(FRONTPAGE_URL)
    time.sleep(5)
    try:
        # Beispiel-XPath für Cookie-Popup
        driver.find_element(By.XPATH, "/html/body/div[1]/div/div/div/div/div/div[2]/button[3]").click()
    except NoSuchElementException:
        print("Cookie-Popup wurde nicht gefunden oder bereits ausgeblendet.")
        pass

    return driver
