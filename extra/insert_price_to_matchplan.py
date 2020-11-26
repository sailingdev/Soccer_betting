import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3
import requests
from bs4 import BeautifulSoup, Tag

from selenium import webdriver 
from selenium.webdriver.chrome.options import Options
#from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time
import sys

http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="soccer"
)

mycursor = mydb.cursor()

driverpath = "H:\workstation\Soccer+betting\Soccer_scrapping\driver_exe\chromedriver.exe"
chrome_options = Options()
chrome_options.add_argument('headless')
chrome_options.add_argument('ignore-certificate-errors')
chrome_options.add_argument('--disable-gpu')
chrome_options.page_load_strategy = 'none'
#chrome_options.add_argument("--proxy-server=xxx.xxx.xxx.xxx");
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.binary_location = 'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe'

def switch_season(argument):
    switcher = {
      "2019-2020": 12,
      "2020" : 64,
       
    }
    return switcher.get(argument, "null")
def switch_league(argument):
    switcher = {
      
      "england/premier-league-": 6,   #England
      "esp-primera-division": 16,  #spain
      "bundesliga": 8,   #Germany
      "ita-serie-a" : 11,  #italy
      "fra-ligue-1" : 7,   #france
      "ned-eredivisie": 12,  #Netherland
      "aut-bundesliga": 1,  #Austria
        "por-primeira-liga": 14,  #portugal
        "por-liga-sagres": 14,
        "por-liga-zon-sagres":14,
        "gre-superleague": 9,   #Greece
        "tur-sueperlig": 19,   #Turkey
        "nor-eliteserien": 13,  #Norway
        "nor-tippeligaen":13,
        "swe-allsvenskan": 17,  #Sweden
        "sui-super-league": 18,   #Swiztland
        "den-superliga": 5,     #Denmark
        "den-sas-ligaen":5,
        "ukr-premyer-liga": 20,     #Ukraine
        "bul-a-grupa": 2,       #bulgaria
        "cze-1-fotbalova-liga": 3,      #Chezch
        "cze-gambrinus-liga": 3,
        "cro-1-hnl": 4 ,          #Croatia
        "hun-nb-i": 10,     #Hungary
        "hun-nb1": 10,
        "hun-otp-liga":10,
        "srb-super-liga": 15    #Serbia
    }
    return switcher.get(argument, "null")

def do_price_to_matchplan(basic_match_href_url):
    #basic_match_href_url = "https://www.oddsportal.com/soccer/austria/tipico-bundesliga-2018-2019/admira-st-polten-rZjCogp6/"
    odd_price = []
    first_two_url = basic_match_href_url + "#1X2;2" 
    OU_url  =  basic_match_href_url + "#over-under;2"
    AH_url = basic_match_href_url + "#ah;2"
    print("--------- start scraping 1X2 data --------------------")
    WD_value = get_1X2data(first_two_url)
    if len(WD_value) == 3:
      odd_price.append(WD_value)
    else:
      print("  Average counts is smaller than 3")
      return
    print("--------- start scraping Over Under data --------------------")
    odd_price.append(get_Over_Underdata(OU_url))
    print("--------- start scraping Asian Handicap data --------------------")
    odd_price.append(get_AH_Data(AH_url))
    print("--------- End scraping  data --------------------")
    print(odd_price)

def get_1X2data(url):
    return_val = []

    ################################ driver setting part start############################
    driver1 = webdriver.Chrome(driverpath,options=chrome_options)
    #driver.maximize_window()
    ################################ driver setting part End #############################`
    driver1.get(url)
   
    tfoot = driver1.find_elements_by_tag_name('tfoot')
    aver_element = tfoot[0].find_element_by_class_name("aver")
    if aver_element:
       av_values = aver_element.find_elements_by_class_name("right")
       if len(av_values) > 2:
          return_val.append(av_values[0].text)
          return_val.append(av_values[1].text)
          return_val.append(av_values[2].text)
       else:
         print("  Average counts is smaller than 3")
    else: 
       print(" Not Found aver elements")
    #driver1.quit()
    
    return return_val  

def get_Over_Underdata(url):
    return_val = ['','','','','','','','','','']
    ################################ driver setting part start############################
    driver2 = webdriver.Chrome(driverpath,options=chrome_options)
    #driver.maximize_window()
    ################################ driver setting part End #############################
    driver2.get(url)
    root_data = driver2.find_element_by_id("odds-data-table")
    
    containers = root_data.find_elements_by_class_name('table-container')
    for container in containers:
      #print(container.text)
      strong_element = container.find_elements_by_tag_name('strong')
      if len(strong_element):
        if strong_element[0].text == "Over/Under +2.5":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[5] = span_elements[0].text
            return_val[4] = span_elements[1].text

        if strong_element[0].text == "Over/Under +3.5":
          
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[9] = span_elements[0].text
            return_val[8] = span_elements[1].text
      
        if strong_element[0].text == "Over/Under +2":
                  
                  span_elements = container.find_elements_by_class_name('nowrp')
                  if len(span_elements):
                    return_val[1] = span_elements[0].text
                    return_val[0] = span_elements[1].text
                    
        if strong_element[0].text == "Over/Under +2.25":
              
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[3] = span_elements[0].text
            return_val[2] = span_elements[1].text

        if strong_element[0].text == "Over/Under +3":
              
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[7] = span_elements[0].text
            return_val[6] = span_elements[1].text


    #print(return_val)
    #driver2.quit()
    return return_val

def get_AH_Data(url):
    return_val = ['','','','','','','','','','','','','','','','','','','','','','','','','','','','','','']
    ################################ driver setting part start############################
    driver3 = webdriver.Chrome(driverpath,options=chrome_options)
    #driver.maximize_window()
    ################################ driver setting part End #############################
    driver3.get(url)
    root_data = driver3.find_element_by_id("odds-data-table")
    containers = root_data.find_elements_by_class_name('table-container')
    for container in containers:
      
      strong_element = container.find_elements_by_tag_name('strong')
      if len(strong_element):
        if strong_element[0].text == "Asian handicap -2.5":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[1] = span_elements[0].text
            return_val[0] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -2.25":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[3] = span_elements[0].text
            return_val[2] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -2":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[5] = span_elements[0].text
            return_val[4] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -1.75":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[7] = span_elements[0].text
            return_val[6] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -1.5":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[9] = span_elements[0].text
            return_val[8] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -1.25":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[11] = span_elements[0].text
            return_val[10] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -1":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[13] = span_elements[0].text
            return_val[12] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -0.75":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[15] = span_elements[0].text
            return_val[14] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -0.5":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[17] = span_elements[0].text
            return_val[16] = span_elements[1].text
        if strong_element[0].text == "Asian handicap -0.25":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[19] = span_elements[0].text
            return_val[18] = span_elements[1].text
          
        if strong_element[0].text == "Asian handicap 0":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[21] = span_elements[0].text
            return_val[20] = span_elements[1].text
        if strong_element[0].text == "Asian handicap +0.25":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[23] = span_elements[0].text
            return_val[22] = span_elements[1].text

        if strong_element[0].text == "Asian handicap +0.5":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[25] = span_elements[0].text
            return_val[24] = span_elements[1].text

        if strong_element[0].text == "Asian handicap +0.75":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[27] = span_elements[0].text
            return_val[26] = span_elements[1].text

        if strong_element[0].text == "Asian handicap +1":
          span_elements = container.find_elements_by_class_name('nowrp')
          if len(span_elements):
            return_val[29] = span_elements[0].text
            return_val[28] = span_elements[1].text
    
    #driver3.quit()
    #print(return_val)
    return return_val

def insert_Price_To_Matchplan(league, season, pagenumber):
  site_url = "https://www.oddsportal.com/soccer/"
  search_url = site_url + league + season + "/results/#/page/" + str(pagenumber)
  
  print(f"----------------{league} - {season} {pagenumber}page start--------------------------------")
  ################################ driver setting part start ############################
  driver = webdriver.Chrome(driverpath,options=chrome_options)
  #driver.maximize_window()
  ################################ driver setting part End #############################`
  print(search_url)
  wait = WebDriverWait(driver, 20)
  driver.get(search_url)

  wait.until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
  driver.execute_script("window.stop();")

  tbody = driver.find_element_by_tag_name('tbody')
  #print(tbody.text)
  index = 0
  match_info_tr = tbody.find_elements_by_xpath("tr[@xeid != '']")
  for everymatch in match_info_tr:  
    #print(match_info_tr[0].text)
    print(f"    -------------------- {pagenumber}page {index}th match start----------------------")
    hrefUrl = everymatch.find_elements_by_tag_name("a")[0].get_attribute('href')
    print(hrefUrl)
    do_price_to_matchplan(hrefUrl)   #get every match information
    print(f"    -------------------- {pagenumber}page {index}th match End----------------------")
    index += 1

  print(f"---------------- {league} - {season} {pagenumber}page End--------------------------------")

insert_Price_To_Matchplan("england/premier-league-", "2019-2020", 1)
