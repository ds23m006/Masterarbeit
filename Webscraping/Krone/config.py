import socket

# Konfigurationen
CHROMEDRIVER_PATH = "/usr/bin/chromedriver" if socket.gethostname() == "raspberrypi" else "chromedriver.exe"
FRONTPAGE_URL = "https://www.krone.at/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/109.0.0.0 Safari/537.36"
)
