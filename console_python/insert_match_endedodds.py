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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
import time
import sys
import os

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
# PROXY = "188.138.1.126:443" # IP:PORT or HOST:PORT

# chrome_options.add_argument('--proxy-server=%s' % PROXY)
chrome_options.binary_location = 'C:\Program Files\Google\Chrome\Application\chrome.exe'
#chrome_options.binary = 'C:\Program Files\Mozilla Firefox\firefox.exe'

profile = webdriver.FirefoxProfile()
profile.set_preference('browser.download.folderList', 2)
profile.set_preference('browser.download.manager.showWhenStarting', False)
profile.set_preference('browser.download.dir', os.getcwd())
profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ('application/vnd.ms-excel'))
profile.set_preference('general.warnOnAboutConfig', False)
profile.update_preferences()
gecko_path = "C:\Soccer_betting\geckodriver.exe"
path = "C:\Program Files\Mozilla Firefox\firefox.exe"
binary = FirefoxBinary(path)


site_url = "https://www.oddsportal.com/"
tfoot_index = 0
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
        "2020" : 64,
        "2021" : 844,
        "2019-2020": 12,
        "2020-2021": 799,
        "2021-2022" : 857
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

total_added_count = 0
def insert_odds(basic_match_href_url, match_date, team_text, current_season):
    global total_added_count
    three_way_url = basic_match_href_url + "#1X2;2" 
    OU_url  =  basic_match_href_url + "#over-under;2"
    AH_url = basic_match_href_url + "#ah;2"  
    
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
            sql = f"select * from odds where match_id = {match_id} and bookmaker_id = 11"
            mycursor.execute(sql)
            result = mycursor.fetchall()
            ################## inserting new odds data #######################################################################
           
            if (result and result[0][3] != 0) and (result and str(result[0][42]) >= match_date):                # that wasnt resch game's odd
                print("         # No need to insert")
                return "No update"
            else:
                if result:
                        sql = f"delete from odds where match_id = {match_id} and bookmaker_id = 11"
                        mycursor.execute(sql)
                        mydb.commit()
                        print("        * deleted existing one row !")
                odd_price = get_odds(three_way_url, OU_url, AH_url)
                print("        " , odd_price)
                updated_at = datetime.today().strftime('%Y-%m-%d')
                print(f"       inserted at {updated_at}")
                sql = f"INSERT INTO odds (match_id, bookmaker_id, Home, Draw, Away, Over2d5, Under2d5, AH2_1, AH2_2, AH1d75_1, AH1d75_2, AH1d5_1, AH1d5_2 , AH1d25_1, AH1d25_2, AH1_1, AH1_2, AH0d75_1, AH0d75_2, AH0d5_1, AH0d5_2, AH0d25_1, AH0d25_2, AH0_1, AH0_2 , AH_p0d25_1 , AH_p0d25_2, AH_p0d5_1, AH_p0d5_2, AH_p0d75_1 , AH_p0d75_2, AH_p1_1, AH_p1_2, AH_p1d25_1, AH_p1d25_2, AH_p1d5_1, AH_p1d5_2, AH_p1d75_1, AH_p1d75_2, AH_p2_1, AH_p2_2 , updated_at)"  \
			    f"VALUES ({match_id}, 11, {odd_price['3way']['highest'][0]}, {odd_price['3way']['highest'][1]}, {odd_price['3way']['highest'][2]}, {odd_price['O/U']['highest'][0]}, {odd_price['O/U']['highest'][1]}, " \
                f"{odd_price['AH']['AH_2']['highest'][0]} , {odd_price['AH']['AH_2']['highest'][1]} ,{odd_price['AH']['AH_1.75']['highest'][0]} , {odd_price['AH']['AH_1.75']['highest'][1]} , " \
                f"{odd_price['AH']['AH_1.5']['highest'][0]} , {odd_price['AH']['AH_1.5']['highest'][1]} ,{odd_price['AH']['AH_1.25']['highest'][0]} , {odd_price['AH']['AH_1.25']['highest'][1]} , " \
                f"{odd_price['AH']['AH_1']['highest'][0]} , {odd_price['AH']['AH_1']['highest'][1]} ,{odd_price['AH']['AH_0.75']['highest'][0]} , {odd_price['AH']['AH_0.75']['highest'][1]} , " \
                f"{odd_price['AH']['AH_0.5']['highest'][0]} , {odd_price['AH']['AH_0.5']['highest'][1]} ,{odd_price['AH']['AH_0.25']['highest'][0]} , {odd_price['AH']['AH_0.25']['highest'][1]} , " \
                f"{odd_price['AH']['AH_0']['highest'][0]} , {odd_price['AH']['AH_0']['highest'][1]} , {odd_price['AH']['AH_p0.25']['highest'][0]} , {odd_price['AH']['AH_p0.25']['highest'][1]} , " \
                f"{odd_price['AH']['AH_p0.5']['highest'][0]} , {odd_price['AH']['AH_p0.5']['highest'][1]},{odd_price['AH']['AH_p0.75']['highest'][0]} , {odd_price['AH']['AH_p0.75']['highest'][1]} , "  \
                f"{odd_price['AH']['AH_p1']['highest'][0]} , {odd_price['AH']['AH_p1']['highest'][1]},{odd_price['AH']['AH_p1.25']['highest'][0]} , {odd_price['AH']['AH_p1.25']['highest'][1]} , "  \
                f"{odd_price['AH']['AH_p1.5']['highest'][0]} , {odd_price['AH']['AH_p1.5']['highest'][1]},{odd_price['AH']['AH_p1.75']['highest'][0]} , {odd_price['AH']['AH_p1.75']['highest'][1]} , "  \
                f"{odd_price['AH']['AH_p2']['highest'][0]} , {odd_price['AH']['AH_p2']['highest'][1]} , '{updated_at}') "
                mycursor.execute(sql)
                mydb.commit()

                total_added_count += 1
                print("        # insert successful! ")
                return "update"
            ######################### updating Asian Handicap on existing rows #################################################################

            # odd_price = get_odds(three_way_url, OU_url, AH_url)
            # print("        " , odd_price)

            # sql = f"UPDATE odds set  AH2_1 = {odd_price['AH']['AH_2']['highest'][0]} , AH2_2 = {odd_price['AH']['AH_2']['highest'][1]} , " \
            # f"AH1d75_1 = {odd_price['AH']['AH_1.75']['highest'][0]} , AH1d75_2 = {odd_price['AH']['AH_1.75']['highest'][1]} ," \
            # f"AH1d5_1 = {odd_price['AH']['AH_1.5']['highest'][0]} , AH1d5_2 = {odd_price['AH']['AH_1.5']['highest'][1]} ," \
            # f"AH1d25_1 = {odd_price['AH']['AH_1.25']['highest'][0]} , AH1d25_2 = {odd_price['AH']['AH_1.25']['highest'][1]} ," \
            # f"AH1_1 = {odd_price['AH']['AH_1']['highest'][0]} , AH1_2 = {odd_price['AH']['AH_1']['highest'][1]} ," \
            # f"AH0d75_1 = {odd_price['AH']['AH_0.75']['highest'][0]} , AH0d75_2 = {odd_price['AH']['AH_0.75']['highest'][1]} ," \
            # f"AH0d5_1 = {odd_price['AH']['AH_0.5']['highest'][0]} , AH0d5_2 = {odd_price['AH']['AH_0.5']['highest'][1]} ," \
            # f"AH0d25_1 = {odd_price['AH']['AH_0.25']['highest'][0]} , AH0d25_2 = {odd_price['AH']['AH_0.25']['highest'][1]} ," \
            # f"AH0_1 = {odd_price['AH']['AH_0']['highest'][0]} , AH0_2 = {odd_price['AH']['AH_0']['highest'][1]} ," \
            # f"AH_p0d25_1 = {odd_price['AH']['AH_p0.25']['highest'][0]} , AH_p0d25_2 = {odd_price['AH']['AH_p0.25']['highest'][1]} ," \
            # f"AH_p0d5_1 = {odd_price['AH']['AH_p0.5']['highest'][0]} , AH_p0d5_2 = {odd_price['AH']['AH_p0.5']['highest'][1]} ," \
            # f"AH_p0d75_1 = {odd_price['AH']['AH_p0.75']['highest'][0]} , AH_p0d75_2 = {odd_price['AH']['AH_p0.75']['highest'][1]} ," \
            # f"AH_p1_1 = {odd_price['AH']['AH_p1']['highest'][0]} , AH_p1_2 = {odd_price['AH']['AH_p1']['highest'][1]} ," \
            # f"AH_p1d25_1 = {odd_price['AH']['AH_p1.25']['highest'][0]} , AH_p1d25_2 = {odd_price['AH']['AH_p1.25']['highest'][1]} ," \
            # f"AH_p1d5_1 = {odd_price['AH']['AH_p1.5']['highest'][0]} , AH_p1d5_2 = {odd_price['AH']['AH_p1.5']['highest'][1]} ," \
            # f"AH_p1d75_1 = {odd_price['AH']['AH_p1.75']['highest'][0]} , AH_p1d75_2 = {odd_price['AH']['AH_p1.75']['highest'][1]} ," \
            # f"AH_p2_1 = {odd_price['AH']['AH_p2']['highest'][0]} , AH_p2_2 = {odd_price['AH']['AH_p2']['highest'][1]} " \
            # f"WHERE match_id = {match_id} and bookmaker_id = 11"
            # mycursor.execute(sql)
            # mydb.commit()

            # print("        # Asian Handicap added on existing columns! ")

        else:
            print("        # Can't find match id in season_match_plan table.")
    else:
        print("        # Can't find team_id in team_list.")
    
def get_odds(turl, OU_url , AH_url):
    odd_price = {"3way": {}, "O/U": {}, "AH": {}}
   
    highest_list = [] 
   
    ################################ driver setting part start############################
    driver1 = webdriver.Chrome(driverpath,options=chrome_options)
    driver1.get(turl)
    time.sleep(0.5)
    ################################ driver setting part End #############################`
    print("        * start scraping 1X2 data --------------------")
   
    
    # driver1.execute_script("ElementSelect.expand( 'user-header-oddsformat' , 'user-header-oddsformat-expander' )")
    # time.sleep(1)
    # #driver1.find_element_by_xpath("//a[text()='EU Odds']").click()
    # driver1.execute_script("changeOddsFormat(1);")
    # time.sleep(3)
    
    ############## 3 way result ###################################################################
    tfoot = driver1.find_elements_by_tag_name('tfoot')
    
    high_elemnet = tfoot[0].find_element_by_class_name("highest")   
    if high_elemnet:
        av_values = high_elemnet.find_elements_by_class_name("right")
        if len(av_values) > 2:
          for i in range(0, 3):
            if av_values[i].text == "-":
              highest_list.append("0")
            else: 
              highest_list.append(av_values[i].text)
          

    three_way = {"highest": highest_list}
    odd_price['3way'] = three_way
    
    ############### Over / Under result ############################################################

    print("        * start scraping Over Under data --------------------")
    driver1.execute_script("uid(5)._onClick();")
    
    time.sleep(1)
    # wait = WebDriverWait(driver1, 20)
    # wait.until(EC.presence_of_element_located((By.ID, 'odds-data-table')))
    
    highest_list = []
    tfoot_OU = []
    element_OU = driver1.find_elements_by_xpath("//a[text()='Over/Under +2.5 ']")
    if len(element_OU) == 0:
        print("    Couldn't find Over 2.5 values !")
    else:
        driver1.execute_script("page.togleTableContent('P-2.50-0-0',this)")
        time.sleep(1)
        tfoot_OU = driver1.find_elements_by_tag_name('tfoot')

    if len(tfoot_OU):
   
        high_elemnet = tfoot_OU[0].find_element_by_class_name("highest")
       
        if high_elemnet:
            av_values = high_elemnet.find_elements_by_class_name("right")
            if len(av_values) > 1:
                for i in  range(0, 2):
                  if av_values[i+1].text == "-":
                    highest_list.append("0")
                  else:
                    highest_list.append(av_values[i+1].text)
    else:

       highest_list = ['0', '0']
        
      
    O_U = {"highest": highest_list}
    
    odd_price['O/U'] = O_U
    ############### Asian Handicap result ############################################################
    print("        * start scraping Asian Handicap data --------------------")
    AH_odds = {"AH_2":{}, "AH_1.75":{}, "AH_1.5":{}, "AH_1.25":{}, "AH_1":{}, "AH_0.75":{}, "AH_0.5":{}, "AH_0.25":{}, "AH_0":{} , 
    "AH_p2":{}, "AH_p1.75":{}, "AH_p1.5":{}, "AH_p1.25":{}, "AH_p1":{}, "AH_p0.75":{}, "AH_p0.5":{}, "AH_p0.25":{}}
    driver1.execute_script("uid(4)._onClick();")
    time.sleep(1)

    tfoot_OU = []
    
    tfoot_index = 0
    ##################= Asian Handicap 2.0 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -2 ']")
    
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -2 values !")
        AH_odds["AH_2"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--2.00-0-0',this)")
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_2"] = AH
            driver1.execute_script("page.togleTableContent('P--2.00-0-0',this)")
        else:
            AH_odds["AH_2"] = { "highest": ['0', '0']}
        
    ##################= Asian Handicap 1.75 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -1.75 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -1.75 values !")
        AH_odds["AH_1.75"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--1.75-0-0',this)")
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_1.75"] = AH
            driver1.execute_script("page.togleTableContent('P--1.75-0-0',this)")
        else:
            AH_odds["AH_1.75"] = { "highest": ['0', '0']}
        
    ##################= Asian Handicap 1.5 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -1.5 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -1.5 values !")
        AH_odds["AH_1.5"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":  
            driver1.execute_script("page.togleTableContent('P--1.50-0-0',this)")
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_1.5"] = AH
            driver1.execute_script("page.togleTableContent('P--1.50-0-0',this)")
        else:
            AH_odds["AH_1.5"] = { "highest": ['0', '0']}
    ##################= Asian Handicap 1.25 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -1.25 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -1.25 values !")
        AH_odds["AH_1.25"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--1.25-0-0',this)")
            #time.sleep(0.2)
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_1.25"] = AH
            driver1.execute_script("page.togleTableContent('P--1.25-0-0',this)")
        else:
            AH_odds["AH_1.25"] = { "highest": ['0', '0']} 
    ##################= Asian Handicap 1 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -1 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -1 values !")
        AH_odds["AH_1"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--1.00-0-0',this)")
            #time.sleep(0.2)
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_1"] = AH
            driver1.execute_script("page.togleTableContent('P--1.00-0-0',this)")
        else:
            AH_odds["AH_1"] = { "highest": ['0', '0']}
    ##################= Asian Handicap 0.75 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -0.75 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -0.75 values !")
        AH_odds["AH_0.75"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--0.75-0-0',this)")
            #time.sleep(0.2)
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_0.75"] = AH
            driver1.execute_script("page.togleTableContent('P--0.75-0-0',this)")
        else:
            AH_odds["AH_0.75"] = { "highest": ['0', '0']}
    ##################= Asian Handicap 0.5 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -0.5 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -0.5 values !")
        AH_odds["AH_0.5"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--0.50-0-0',this)")
            
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_0.5"] = AH
            driver1.execute_script("page.togleTableContent('P--0.50-0-0',this)")
        else:
            AH_odds["AH_0.5"] = { "highest": ['0', '0']}  
    ##################= Asian Handicap 0.25 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -0.25 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -0.25 values !")
        AH_odds["AH_0.25"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P--0.25-0-0',this)")
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_0.25"] = AH
          driver1.execute_script("page.togleTableContent('P--0.25-0-0',this)")
        else:
          AH_odds["AH_0.25"] = { "highest": ['0', '0']}
    ##################= Asian Handicap 0 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap 0 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -0.00 values !")
        AH_odds["AH_0"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-0.00-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_0"] = AH
          driver1.execute_script("page.togleTableContent('P-0.00-0-0',this)")
        else:
          AH_odds["AH_0"] = { "highest": ['0', '0']}
    ##################= Asian Handicap +0.25 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap +0.25 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap +0.25 values !")
        AH_odds["AH_p0.25"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-0.25-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_p0.25"] = AH
          driver1.execute_script("page.togleTableContent('P-0.25-0-0',this)")
        else:
          AH_odds["AH_p0.25"] = { "highest": ['0', '0']}
    ##################= Asian Handicap +0.5 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap +0.5 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap +0.5 values !")
        AH_odds["AH_p0.5"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-0.50-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_p0.5"] = AH
          driver1.execute_script("page.togleTableContent('P-0.50-0-0',this)")
        else:
          AH_odds["AH_p0.5"] = { "highest": ['0', '0']}
    ##################= Asian Handicap +0.75 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap +0.75 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap +0.75 values !")
        AH_odds["AH_p0.75"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-0.75-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_p0.75"] = AH
          driver1.execute_script("page.togleTableContent('P-0.75-0-0',this)")
        else:
          AH_odds["AH_p0.75"] = { "highest": ['0', '0']}
    ##################= Asian Handicap +1 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap +1 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap +1 values !")
        AH_odds["AH_p1"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-1.00-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_p1"] = AH
          driver1.execute_script("page.togleTableContent('P-1.00-0-0',this)")
        else:
          AH_odds["AH_p1"] = { "highest": ['0', '0']}
    ##################= Asian Handicap +1.25 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap +1.25 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap +1.25 values !")
        AH_odds["AH_p1.25"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-1.25-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_p1.25"] = AH
          driver1.execute_script("page.togleTableContent('P-1.25-0-0',this)")
        else:
          AH_odds["AH_p1.25"] = { "highest": ['0', '0']}
    ##################= Asian Handicap +1.5 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap +1.5 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap +1.5 values !")
        AH_odds["AH_p1.5"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-1.50-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_p1.5"] = AH
          driver1.execute_script("page.togleTableContent('P-1.50-0-0',this)")
        else:
          AH_odds["AH_p1.5"] = { "highest": ['0', '0']}
    ##################= Asian Handicap +1.75 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap +1.75 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap +1.75 values !")
        AH_odds["AH_p1.75"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-1.75-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_p1.75"] = AH
          driver1.execute_script("page.togleTableContent('P-1.75-0-0',this)")
        else:
          AH_odds["AH_p1.75"] = { "highest": ['0', '0']}
    ##################= Asian Handicap +2 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap +2 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap +2 values !")
        AH_odds["AH_p2"] = { "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
          driver1.execute_script("page.togleTableContent('P-2.00-0-0',this)")
          
          tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
          AH = get_various_AsianHandicap(tfoot_OU)
          AH_odds["AH_p2"] = AH
          driver1.execute_script("page.togleTableContent('P-2.00-0-0',this)")
        else:
          AH_odds["AH_p2"] = { "highest": ['0', '0']}
    
    
    odd_price['AH'] = AH_odds
    driver1.quit()
    return odd_price

def get_various_AsianHandicap(tfoot_OU):
    global tfoot_index
  
    highest_list = []
    if len(tfoot_OU):
        #aver_element = tfoot_OU[0].find_element_by_class_name("aver")
        high_elemnet = tfoot_OU[0].find_element_by_class_name("highest")
        # if aver_element:
        #     av_values = aver_element.find_elements_by_class_name("right")
        #     if len(av_values) > 1:
        #         for i in  range(0, 2):
                      
        #           if (av_values[i+1].text == "-" ) or (av_values[i+1].text == "" ):
                    
        #             aver_list.append("0")
        #           else:
        #             aver_list.append(av_values[i+1].text)     
        if high_elemnet:
            av_values = high_elemnet.find_elements_by_class_name("right")
            if len(av_values) > 1:
                for i in  range(0, 2):
                  if (av_values[i+1].text == "-") or  (av_values[i+1].text == ""):
                    highest_list.append("0")
                  else:
                    highest_list.append(av_values[i+1].text)
        
        #tfoot_index += 2
    else:
       
       highest_list = ['0', '0']
        
      
    AH = {"highest": highest_list}
    return AH

def getDate_from_trTxt(date_txt):
    if 'Today' in date_txt:
        return datetime.today().strftime('%Y-%m-%d')
    elif 'Yesterday' in date_txt:
        yesterday = datetime.now() - timedelta(1)
        return datetime.strftime(yesterday, '%Y-%m-%d')
    else:
        date_part = date_txt.split(' ');
        return date_part[2] + "-" +switch_month(date_part[1]) + '-' + date_part[0]

def insert_Price_To_Matchplan(league, season, breakFlag = True, startPage = None):
      
    driver = webdriver.Chrome(driverpath, options=chrome_options)
    current_season = False
    
    
    
    ####################### going to result page ###############################
    if season == "":
        page_url = site_url + "soccer/" + league + season + "/results/"
        current_season = True
    else:
        page_url = site_url +"soccer/" + league + "-" + season + "/results/"
    driver.get(page_url)
    pagination = driver.find_elements_by_id("pagination")
    if len(pagination):
        pagenumber = len(pagination[0].find_elements_by_tag_name("a")) - 3
    else:
        pagenumber = 1
    print("whole page count", pagenumber)
    breakflag = 0
    if startPage ==  None:
          startPage = 1
    for page in range(startPage, pagenumber+1):
        search_url = page_url + "#/page/" + str(page)
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
                #if (" - " in team_text) & ( ':' in score_field ):
                if (" - " in team_text):
                    print(f"        {match_date} , {team_text} ")
                    hrefUrl = each_tr.find_elements_by_tag_name("a")[0].get_attribute('href')
                    
                    status = insert_odds(hrefUrl, match_date, team_text, current_season)              # get every match information
                    if current_season & (status == "No update"):
                            print("     * No need to update , this is already inserted!")
                            breakflag = 1
                            if breakFlag:
                                break
                    
                    index += 1
                else:
                    print("        * not correct Ended match")
        if breakflag:
            breakflag = 0
            break
        print(f"---------------- {league} - {season} {page}page End--------------------------------")
    driver.quit()

insert_Price_To_Matchplan("england/premier-league",   "2021-2022")
insert_Price_To_Matchplan("spain/laliga",             "2021-2022")
insert_Price_To_Matchplan("germany/bundesliga",       "2021-2022")
insert_Price_To_Matchplan("italy/serie-a",            "2021-2022")
insert_Price_To_Matchplan("france/ligue-1",           "2021-2022")
insert_Price_To_Matchplan("netherlands/eredivisie",   "2021-2022")
insert_Price_To_Matchplan("austria/tipico-bundesliga","2021-2022")
insert_Price_To_Matchplan("portugal/primeira-liga",   "2021-2022")
insert_Price_To_Matchplan("greece/super-league",      "2021-2022")
insert_Price_To_Matchplan("turkey/super-lig",         "2021-2022")
insert_Price_To_Matchplan("norway/eliteserien",       "2021-2022")
insert_Price_To_Matchplan("sweden/allsvenskan",       "2021-2022")
insert_Price_To_Matchplan("switzerland/super-league", "2021-2022")
insert_Price_To_Matchplan("denmark/superliga",        "2021-2022")
insert_Price_To_Matchplan("ukraine/premier-league",   "2021-2022")
insert_Price_To_Matchplan("bulgaria/parva-liga",      "2021-2022")
insert_Price_To_Matchplan("czech-republic/1-liga",    "2021-2022")
insert_Price_To_Matchplan("croatia/1-hnl",            "2021-2022")
insert_Price_To_Matchplan("hungary/otp-bank-liga",    "2021-2022")
insert_Price_To_Matchplan("serbia/super-liga",        "2021-2022")

# insert_Price_To_Matchplan("serbia/super-liga", "2019-2020")
# insert_Price_To_Matchplan("serbia/super-liga", "2018-2019")
# insert_Price_To_Matchplan("serbia/super-liga", "2017-2018")
# insert_Price_To_Matchplan("serbia/super-liga", "2016-2017")
# insert_Price_To_Matchplan("serbia/super-liga", "2015-2016")





print(" Total added count is : ", total_added_count)