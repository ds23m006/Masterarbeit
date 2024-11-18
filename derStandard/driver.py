import time
import datetime
from selenium import webdriver as wd
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from config import CHROMEDRIVER_PATH, USER_AGENT, FRONTPAGE_URL

def configure_driver(headless=True):
    chrome_options = wd.ChromeOptions()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument(f"--user-agent={USER_AGENT}")
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-running-insecure-content')
    chrome_options.add_argument('--disable-webgl')

    chrome_prefs = {
        "profile.default_content_settings.images": 2,
        "profile.managed_default_content_settings.images": 2
    }
    chrome_options.experimental_options["prefs"] = chrome_prefs

    service = ChromeService(executable_path=CHROMEDRIVER_PATH)
    driver = wd.Chrome(service=service, options=chrome_options)

    # POPUP WEGKLICKEN
    driver.get(FRONTPAGE_URL + datetime.date.today().strftime("%Y/%m/%d"))
    time.sleep(5)
    try:
        driver.switch_to.frame(driver.find_element(By.XPATH, "/html/body/div/iframe"))
        driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[3]/div[1]/button").click()
        driver.switch_to.parent_frame()
    except NoSuchElementException:
        pass  # Popup nicht gefunden, nichts zu tun

    return driver
