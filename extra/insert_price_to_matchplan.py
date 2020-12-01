import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3
import requests
from bs4 import BeautifulSoup, Tag
from datetime import datetime , timedelta
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

driverpath = "C:\Soccer_betting\chromedriver.exe"
chrome_options = Options()
chrome_options.add_argument('headless')
chrome_options.add_experimental_option("excludeSwitches", ['enable-logging']);
chrome_options.add_argument('ignore-certificate-errors')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.add_argument("--disable-extensions")
chrome_options.page_load_strategy = 'eager'
#chrome_options.add_argument("--proxy-server=xxx.xxx.xxx.xxx");
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.binary_location = 'C:\Program Files\Google\Chrome\Application\chrome.exe'


site_url = "https://www.oddsportal.com/soccer/"
def switch_month(argument):
    switcher = {
      "Jan": "01",
      "Feb" : "02",
      "Mar" : "03",
      "Apr" : "04",
      "May" : "05",
      "Jun" : "06",
      "Jul" : "07",
      "Aug" : "08",
      "Sep" : "09",
      "Oct" : "10",
      "Nov" : "11",
      "Dec" : "12"
       
    }
    return switcher.get(argument, "null")

def switch_season(argument):
    switcher = {
      "2019-2020": 12,
      "2020" : 64,
       
    }
    return switcher.get(argument, "null")
def switch_league(argument):
    switcher = {
      
      "england/premier-league-": 6,   #England
      "spain/laliga": 16,  #spain
      "germany/bundesliga": 8,   #Germany
      "italy/serie-a" : 11,  #italy
      "france/ligue-1" : 7,   #france
      "netherlands/eredivisie": 12,  #Netherland
      "austria/tipico-bundesliga": 1,  #Austria
      
        "portugal/primeira-liga": 14,  #portugal
        "greece/super-league": 9,   #Greece
        "turkey/super-lig": 19,   #Turkey
        "norway/eliteserien": 13,  #Norway

        "sweden/allsvenskan": 17,  #Sweden
        "switzerland/super-league": 18,   #Swiztland
        "denmark/superliga": 5,     #Denmark

        "ukraine/premier-league": 20,     #Ukraine
        "bulgaria/parva-liga": 2,       #bulgaria
        "czech-republic/1-liga": 3,      #Chezch
       
        "croatia/1-hnl": 4 ,          #Croatia
        "hungary/otp-bank-liga": 10,     #Hungary
        "serbia/super-liga": 15    #Serbia
    }
    return switcher.get(argument, "null")

def insert_odds(basic_match_href_url, match_date, team_text):

    three_way_url = basic_match_href_url + "#1X2;2" 
    OU_url  =  basic_match_href_url + "#over-under;2"
    #AH_url = basic_match_href_url + "#ah;2"  
    home_team_name = team_text.split(' - ')[0]
    away_team_name = team_text.split(' - ')[1]
    sql = f"SELECT team_id from team_list where team_name_odd = '{home_team_name}'"
    #print(sql)
    mycursor.execute(sql)
    result = mycursor.fetchall()
    if result:
        home_team_id = result[0][0]
        sql = f"SELECT match_id from season_match_plan where date = '{match_date}' and home_team_id = {home_team_id}"
        mycursor.execute(sql)
        result  =  mycursor.fetchall()
        if result:
            match_id = result[0][0]
            print("        match_id is ", match_id)
            sql = f"select * from odds where match_id = {match_id} and (bookmaker_id = 10 or bookmaker_id = 11)"
            mycursor.execute(sql)
            result = mycursor.fetchall()
            if result:
                print("         # No need to insert")
            else:
                odd_price = get_odds(three_way_url, OU_url)
                print("        " , odd_price)
                sql = f"INSERT INTO odds (match_id, bookmaker_id, Home, Draw, Away, Over2d5, Under2d5 ) " \
				f"VALUES ({match_id}, 10, {odd_price['3way']['aver'][0]}, {odd_price['3way']['aver'][1]}, {odd_price['3way']['aver'][2]}, {odd_price['O/U']['aver'][0]}, {odd_price['O/U']['aver'][1]})"
                mycursor.execute(sql)
                mydb.commit()
                sql = f"INSERT INTO odds (match_id, bookmaker_id, Home, Draw, Away, Over2d5, Under2d5 ) " \
				f"VALUES ({match_id}, 11, {odd_price['3way']['highest'][0]}, {odd_price['3way']['highest'][1]}, {odd_price['3way']['highest'][2]}, {odd_price['O/U']['highest'][0]}, {odd_price['O/U']['highest'][1]})"
                mycursor.execute(sql)
                mydb.commit()
                print("        # insert successful! ")
def get_odds(turl, OU_url):
    odd_price = {"3way": {}, "O/U": {}}
    average_list = []
    highest_list = [] 
   
    ################################ driver setting part start############################
    driver1 = webdriver.Chrome(driverpath,options=chrome_options)
    
   
    ################################ driver setting part End #############################`
    print("        * start scraping 1X2 data --------------------")
    #first = datetime.now()
    driver1.get(turl)
    
    time.sleep(2.5)
    driver1.execute_script("changeOddsFormat(1);")
    time.sleep(2.5)

    #################################################################################
    tfoot = driver1.find_elements_by_tag_name('tfoot')
    aver_element = tfoot[0].find_element_by_class_name("aver")
    high_elemnet = tfoot[0].find_element_by_class_name("highest")
    if aver_element:
       av_values = aver_element.find_elements_by_class_name("right")
       if len(av_values) > 2:
            average_list.append(av_values[0].text)
            average_list.append(av_values[1].text)
            average_list.append(av_values[2].text)
    if high_elemnet:
        av_values = high_elemnet.find_elements_by_class_name("right")
        if len(av_values) > 2:
          highest_list.append(av_values[0].text)
          highest_list.append(av_values[1].text)
          highest_list.append(av_values[2].text)

    three_way = {"aver": average_list , "highest": highest_list}
    odd_price['3way'] = three_way
    
    ###########################################################################

    print("        * start scraping Over Under data --------------------")
    driver1.execute_script("uid(5)._onClick();")
    
    time.sleep(1.5)
    # wait = WebDriverWait(driver1, 20)
    # wait.until(EC.presence_of_element_located((By.ID, 'odds-data-table')))
    aver_list = []
    highest_list = []

    driver1.execute_script("page.togleTableContent('P-2.50-0-0',this)")
    time.sleep(0.5)
    tfoot = driver1.find_elements_by_tag_name('tfoot')
    if len(tfoot):
        aver_element = tfoot[0].find_element_by_class_name("aver")
        high_elemnet = tfoot[0].find_element_by_class_name("highest")
        if aver_element:
            av_values = aver_element.find_elements_by_class_name("right")
            if len(av_values) > 2:
                aver_list.append(av_values[1].text)
                aver_list.append(av_values[2].text)     
        if high_elemnet:
            av_values = high_elemnet.find_elements_by_class_name("right")
            if len(av_values) > 2:
                highest_list.append(av_values[1].text)
                highest_list.append(av_values[2].text)
        
      
    O_U = {"aver": aver_list , "highest": highest_list}
    
    odd_price['O/U'] = O_U
    #second = datetime.now();
    #print("time gape ", second-first)
    driver1.quit()
    
    return odd_price

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
 
def getDate_from_trTxt(date_txt):
  if 'Today' in date_txt:
      return datetime.today().strftime('%Y-%m-%d')
  elif 'Yesterday' in date_txt:
      yesterday = datetime.now() - timedelta(1)
      return datetime.strftime(yesterday, '%Y-%m-%d')
  else:
     date_part = date_txt.split(' ');
     return date_part[2] + "-" +switch_month(date_part[1]) + '-' + date_part[0]

def insert_Price_To_Matchplan(league, season):
    driver = webdriver.Chrome(driverpath,options=chrome_options)
    if season == "":
        page_url = site_url + league + season + "/results/"
    else:
        page_url = site_url + league + "-" + season + "/results/"
    driver.get(page_url)
    pagination = driver.find_elements_by_id("pagination")
    if len(pagination):
        pagenumber = len(pagination[0].find_elements_by_tag_name("a")) - 3
    else:
        pagenumber = 1
    print("whole page count", pagenumber)
    for page in range(1, pagenumber+1):
        search_url = site_url + league + season + "/results/#/page/" + str(page)
        #print(search_url)
    
        print(f"----------------{league} - {season} {page}page start--------------------------------")
      
        driver.get(search_url)    
        time.sleep(1.5)
        tbody = driver.find_element_by_tag_name('tbody')                # get tobody of all matches
        #print(tbody.text)
        index = 0
        match_date = ""
        all_tr_array = tbody.find_elements_by_tag_name("tr")

        for each_tr in all_tr_array:
            classField = each_tr.get_attribute('class')
            if 'nob-border' in classField:                                # it means date tr
                date_th = each_tr.find_elements_by_tag_name('th')[0]
                date_txt = date_th.text
                match_date =  getDate_from_trTxt(date_txt)
            

            if "deactivate" in classField:                                # means match tr
                print(f"    --- {league} {season} {page} page {index}th match start---")
                team_text = each_tr.find_elements_by_tag_name("a")[0].text
                score_field = each_tr.find_elements_by_tag_name('td')[2].text
                if (" - " in team_text) & (':' in score_field):
                    print(f"        {match_date} , {team_text} ")
                    hrefUrl = each_tr.find_elements_by_tag_name("a")[0].get_attribute('href')
                    insert_odds(hrefUrl, match_date, team_text)                                  # get every match information
                    #print(f"    --- {page} page {index}th match End---")
                    index += 1
                else:
                    print("        * not correct Ended match")
        print(f"---------------- {league} - {season} {page}page End--------------------------------")

# insert_Price_To_Matchplan("england/premier-league", "")
# insert_Price_To_Matchplan("spain/laliga", "")
# insert_Price_To_Matchplan("germany/bundesliga", "")
# insert_Price_To_Matchplan("italy/serie-a", "")
# insert_Price_To_Matchplan("france/ligue-1", "")
# insert_Price_To_Matchplan("netherlands/eredivisie", "")
# insert_Price_To_Matchplan("austria/tipico-bundesliga", "")
# insert_Price_To_Matchplan("portugal/primeira-liga", "")
# insert_Price_To_Matchplan("greece/super-league", "")
# insert_Price_To_Matchplan("turkey/super-lig", "")
# insert_Price_To_Matchplan("norway/eliteserien", "")
# insert_Price_To_Matchplan("sweden/allsvenskan", "")
# insert_Price_To_Matchplan("switzerland/super-league", "")
# insert_Price_To_Matchplan("denmark/superliga", "")
# insert_Price_To_Matchplan("ukraine/premier-league", "")
# insert_Price_To_Matchplan("bulgaria/parva-liga", "")
# insert_Price_To_Matchplan("czech-republic/1-liga", "")
# insert_Price_To_Matchplan("croatia/1-hnl", "")
# insert_Price_To_Matchplan("hungary/otp-bank-liga", "")
# insert_Price_To_Matchplan("serbia/super-liga", "")
insert_Price_To_Matchplan("england/premier-league", "")