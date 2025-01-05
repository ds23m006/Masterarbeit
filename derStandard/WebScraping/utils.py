from selenium.webdriver.remote.webelement import WebElement

def expand_shadow_element(driver, element: WebElement):
    """Erweitert ein Shadow DOM-Element und gibt das Shadow Root zurück."""
    shadow_root = driver.execute_script('return arguments[0].shadowRoot', element)
    return shadow_root
