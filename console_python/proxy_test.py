from selenium import webdriver
from selenium.webdriver.chrome.options import Options
 
 
PROXY = "193.149.225.228:80"
driverpath = "C:\Soccer_betting\chromedriver.exe"
chrome_options = Options()
# chrome_options.add_argument('headless')
chrome_options.add_experimental_option("excludeSwitches", ['enable-logging']);
chrome_options.add_argument('ignore-certificate-errors')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.add_argument("--disable-extensions")
chrome_options.page_load_strategy = 'eager'
chrome_options.add_argument('--proxy-server=%s' % PROXY)
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.binary_location = 'C:\Program Files\Google\Chrome\Application\chrome.exe'
driver = webdriver.Chrome(driverpath,options=chrome_options)
driver.delete_all_cookies()
driver.maximize_window()
 
driver.get("https://oddsportal.com")
print(driver.page_source)
driver.quit()