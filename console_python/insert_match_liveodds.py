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

PROXY = "193.149.225.224:80"
driverpath = "C:\Soccer_betting\chromedriver.exe"
chrome_options = Options()
chrome_options.add_argument('headless')
chrome_options.add_experimental_option("excludeSwitches", ['enable-logging']);
chrome_options.add_argument('ignore-certificate-errors')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.add_argument("--disable-extensions")
chrome_options.page_load_strategy = 'eager'
# chrome_options.add_argument('--proxy-server=%s' % PROXY)
#chrome_options.add_argument("--proxy-server=xxx.xxx.xxx.xxx");
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.binary_location = 'C:\Program Files\Google\Chrome\Application\chrome.exe'


site_url = "https://www.oddsportal.com/soccer/"

def switch_month(argument):
    switcher = {
        "Jan" : "01",
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
        "2021" : 844 ,
        "2019-2020": 12,
        "2020-2021": 799,
        "2021-2022": 857
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
def insert_update_odds(basic_match_href_url, match_date, team_text):
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
            odd_price = get_odds(three_way_url, OU_url, AH_url)
            print("        " , odd_price)
            sql = f"select * from odds where match_id = {match_id} and bookmaker_id = 13"
            mycursor.execute(sql)
            result = mycursor.fetchall()
            if result:                  # alrady exist in odds table
                # sql = f"UPDATE odds set  Home = {odd_price['3way']['aver'][0]}, Draw = {odd_price['3way']['aver'][1]}, Away = {odd_price['3way']['aver'][2]} " \
                #   f", Over2d5 = {odd_price['O/U']['aver'][0]} , Under2d5 = {odd_price['O/U']['aver'][1]}, AH2_1 = {odd_price['AH']['AH_2']['aver'][0]} , AH2_2 = {odd_price['AH']['AH_2']['aver'][1]} , " \
                #   f"AH1d75_1 = {odd_price['AH']['AH_1.75']['aver'][0]} , AH1d75_2 = {odd_price['AH']['AH_1.75']['aver'][1]} ," \
                #   f"AH1d5_1 = {odd_price['AH']['AH_1.5']['aver'][0]} , AH1d5_2 = {odd_price['AH']['AH_1.5']['aver'][1]} ," \
                #   f"AH1d25_1 = {odd_price['AH']['AH_1.25']['aver'][0]} , AH1d25_2 = {odd_price['AH']['AH_1.25']['aver'][1]} ," \
                #   f"AH1_1 = {odd_price['AH']['AH_1']['aver'][0]} , AH1_2 = {odd_price['AH']['AH_1']['aver'][1]} ," \
                #   f"AH0d75_1 = {odd_price['AH']['AH_0.75']['aver'][0]} , AH0d75_2 = {odd_price['AH']['AH_0.75']['aver'][1]} ," \
                #   f"AH0d5_1 = {odd_price['AH']['AH_0.5']['aver'][0]} , AH0d5_2 = {odd_price['AH']['AH_0.5']['aver'][1]} ," \
                #   f"AH0d25_1 = {odd_price['AH']['AH_0.25']['aver'][0]} , AH0d25_2 = {odd_price['AH']['AH_0.25']['aver'][1]} ," \
                #   f"AH0_1 = {odd_price['AH']['AH_0']['aver'][0]} , AH0_2 = {odd_price['AH']['AH_0']['aver'][1]} where match_id = {match_id} and bookmaker_id = 12" \
                # mycursor.execute(sql)
                # mydb.commit()

                sql = f"UPDATE odds set Home = {odd_price['3way']['highest'][0]}, Draw =  {odd_price['3way']['highest'][1]}, Away = {odd_price['3way']['highest'][2]} " \
                  f", Over2d5 =  {odd_price['O/U']['highest'][0]}, Under2d5  =  {odd_price['O/U']['highest'][1]} , AH2_1 = {odd_price['AH']['AH_2']['highest'][0]} , AH2_2 = {odd_price['AH']['AH_2']['highest'][1]} , " \
                  f"AH1d75_1 = {odd_price['AH']['AH_1.75']['highest'][0]} , AH1d75_2 = {odd_price['AH']['AH_1.75']['highest'][1]} ," \
                  f"AH1d5_1 = {odd_price['AH']['AH_1.5']['highest'][0]} , AH1d5_2 = {odd_price['AH']['AH_1.5']['highest'][1]} ," \
                  f"AH1d25_1 = {odd_price['AH']['AH_1.25']['highest'][0]} , AH1d25_2 = {odd_price['AH']['AH_1.25']['highest'][1]} ," \
                  f"AH1_1 = {odd_price['AH']['AH_1']['highest'][0]} , AH1_2 = {odd_price['AH']['AH_1']['highest'][1]} ," \
                  f"AH0d75_1 = {odd_price['AH']['AH_0.75']['highest'][0]} , AH0d75_2 = {odd_price['AH']['AH_0.75']['highest'][1]} ," \
                  f"AH0d5_1 = {odd_price['AH']['AH_0.5']['highest'][0]} , AH0d5_2 = {odd_price['AH']['AH_0.5']['highest'][1]} ," \
                  f"AH0d25_1 = {odd_price['AH']['AH_0.25']['highest'][0]} , AH0d25_2 = {odd_price['AH']['AH_0.25']['highest'][1]} ," \
                  f"AH0_1 = {odd_price['AH']['AH_0']['highest'][0]} , AH0_2 = {odd_price['AH']['AH_0']['highest'][1]} ," \
                  f"AH_p0d25_1 = {odd_price['AH']['AH_p0.25']['highest'][0]} , AH_p0d25_2 = {odd_price['AH']['AH_p0.25']['highest'][1]} ," \
                  f"AH_p0d5_1 = {odd_price['AH']['AH_p0.5']['highest'][0]} , AH_p0d5_2 = {odd_price['AH']['AH_p0.5']['highest'][1]} ," \
                  f"AH_p0d75_1 = {odd_price['AH']['AH_p0.75']['highest'][0]} , AH_p0d75_2 = {odd_price['AH']['AH_p0.75']['highest'][1]} ," \
                  f"AH_p1_1 = {odd_price['AH']['AH_p1']['highest'][0]} , AH_p1_2 = {odd_price['AH']['AH_p1']['highest'][1]} ," \
                  f"AH_p1d25_1 = {odd_price['AH']['AH_p1.25']['highest'][0]} , AH_p1d25_2 = {odd_price['AH']['AH_p1.25']['highest'][1]} ," \
                  f"AH_p1d5_1 = {odd_price['AH']['AH_p1.5']['highest'][0]} , AH_p1d5_2 = {odd_price['AH']['AH_p1.5']['highest'][1]} ," \
                  f"AH_p1d75_1 = {odd_price['AH']['AH_p1.75']['highest'][0]} , AH_p1d75_2 = {odd_price['AH']['AH_p1.75']['highest'][1]} ," \
                  f"AH_p2_1 = {odd_price['AH']['AH_p2']['highest'][0]} , AH_p2_2 = {odd_price['AH']['AH_p2']['highest'][1]} , updated_at = '{datetime.today().strftime('%Y-%m-%d')}'" \
                  f"WHERE match_id = {match_id} and bookmaker_id = 13"
                mycursor.execute(sql)
                mydb.commit()

                total_added_count += 1
                print("         # Update successful! ")
            else:                       # this is new in odds, so will insert
                # sql = f"INSERT INTO odds (match_id, bookmaker_id, Home, Draw, Away, Over2d5, Under2d5 , AH2_1, AH2_2, AH1d75_1, AH1d75_2, AH1d5_1, AH1d5_2 , AH1d25_1, AH1d25_2, AH1_1, AH1_2, AH0d75_1, AH0d75_2, AH0d5_1, AH0d5_2, AH0d25_1, AH0d25_2, AH0_1, AH0_2) " \
                # f"VALUES ({match_id}, 12, {odd_price['3way']['aver'][0]}, {odd_price['3way']['aver'][1]}, {odd_price['3way']['aver'][2]}, {odd_price['O/U']['aver'][0]}, {odd_price['O/U']['aver'][1]} ," \
                # f"{odd_price['AH']['AH_2']['aver'][0]} , {odd_price['AH']['AH_2']['aver'][1]} ,{odd_price['AH']['AH_1.75']['aver'][0]} , {odd_price['AH']['AH_1.75']['aver'][1]} , " \
                # f"{odd_price['AH']['AH_1.5']['aver'][0]} , {odd_price['AH']['AH_1.5']['aver'][1]} ,{odd_price['AH']['AH_1.25']['aver'][0]} , {odd_price['AH']['AH_1.25']['aver'][1]} , " \
                # f"{odd_price['AH']['AH_1']['aver'][0]} , {odd_price['AH']['AH_1']['aver'][1]} ,{odd_price['AH']['AH_0.75']['aver'][0]} , {odd_price['AH']['AH_0.75']['aver'][1]} , " \
                # f"{odd_price['AH']['AH_0.5']['aver'][0]} , {odd_price['AH']['AH_0.5']['aver'][1]} ,{odd_price['AH']['AH_0.25']['aver'][0]} , {odd_price['AH']['AH_0.25']['aver'][1]} , " \
                # f"{odd_price['AH']['AH_0']['aver'][0]} , {odd_price['AH']['AH_0']['aver'][1]} ) "
                
                # mycursor.execute(sql)
                # mydb.commit()

                sql = f"INSERT INTO odds (match_id, bookmaker_id, Home, Draw, Away, Over2d5, Under2d5 , AH2_1, AH2_2, AH1d75_1, AH1d75_2, AH1d5_1, AH1d5_2 , AH1d25_1, AH1d25_2, AH1_1, AH1_2, AH0d75_1, AH0d75_2, AH0d5_1, AH0d5_2, AH0d25_1, AH0d25_2, AH0_1, AH0_2 , AH_p0d25_1 , AH_p0d25_2, AH_p0d5_1, AH_p0d5_2, AH_p0d75_1 , AH_p0d75_2, AH_p1_1, AH_p1_2, AH_p1d25_1, AH_p1d25_2, AH_p1d5_1, AH_p1d5_2, AH_p1d75_1, AH_p1d75_2, AH_p2_1, AH_p2_2 , updated_at ) " \
                f"VALUES ({match_id}, 13, {odd_price['3way']['highest'][0]}, {odd_price['3way']['highest'][1]}, {odd_price['3way']['highest'][2]}, {odd_price['O/U']['highest'][0]}, {odd_price['O/U']['highest'][1]} , " \
                f"{odd_price['AH']['AH_2']['highest'][0]} , {odd_price['AH']['AH_2']['highest'][1]} ,{odd_price['AH']['AH_1.75']['highest'][0]} , {odd_price['AH']['AH_1.75']['highest'][1]} , " \
                f"{odd_price['AH']['AH_1.5']['highest'][0]} , {odd_price['AH']['AH_1.5']['highest'][1]} ,{odd_price['AH']['AH_1.25']['highest'][0]} , {odd_price['AH']['AH_1.25']['highest'][1]} , " \
                f"{odd_price['AH']['AH_1']['highest'][0]} , {odd_price['AH']['AH_1']['highest'][1]} ,{odd_price['AH']['AH_0.75']['highest'][0]} , {odd_price['AH']['AH_0.75']['highest'][1]} , " \
                f"{odd_price['AH']['AH_0.5']['highest'][0]} , {odd_price['AH']['AH_0.5']['highest'][1]} ,{odd_price['AH']['AH_0.25']['highest'][0]} , {odd_price['AH']['AH_0.25']['highest'][1]} , " \
                f"{odd_price['AH']['AH_0']['highest'][0]} , {odd_price['AH']['AH_0']['highest'][1]} ,{odd_price['AH']['AH_p0.25']['highest'][0]} , {odd_price['AH']['AH_p0.25']['highest'][1]} , " \
                f"{odd_price['AH']['AH_p0.5']['highest'][0]} , {odd_price['AH']['AH_p0.5']['highest'][1]},{odd_price['AH']['AH_p0.75']['highest'][0]} , {odd_price['AH']['AH_p0.75']['highest'][1]} , "  \
                f"{odd_price['AH']['AH_p1']['highest'][0]} , {odd_price['AH']['AH_p1']['highest'][1]},{odd_price['AH']['AH_p1.25']['highest'][0]} , {odd_price['AH']['AH_p1.25']['highest'][1]} , "  \
                f"{odd_price['AH']['AH_p1.5']['highest'][0]} , {odd_price['AH']['AH_p1.5']['highest'][1]},{odd_price['AH']['AH_p1.75']['highest'][0]} , {odd_price['AH']['AH_p1.75']['highest'][1]} , "  \
                f"{odd_price['AH']['AH_p2']['highest'][0]} , {odd_price['AH']['AH_p2']['highest'][1]} , '{datetime.today().strftime('%Y-%m-%d')}') "
                mycursor.execute(sql)
                mydb.commit()

                total_added_count += 1
                print("        # insert successful! ")
                
        else:
            print("        # Can't find match id in season_match_plan table.")
    else:
        print("        # Can't find team_id in team_list.")
    
# function for scraping MO, O/U, AH odds of following matches    
def get_odds(turl, OU_url , AH_url):
    odd_price = {"3way": {}, "O/U": {} , "AH": {}} 
    highest_list = []   
    ################################ driver setting part start############################
    driver1 = webdriver.Chrome(driverpath,options=chrome_options)
    
    ################################ driver setting part End #############################`
    print("        * start scraping 1X2 data --------------------")
    driver1.get(turl)
    time.sleep(1)
    #################################################################################
    tfoot = driver1.find_elements_by_tag_name('tfoot')
    high_elemnet = tfoot[0].find_element_by_class_name("highest")
    if high_elemnet:
        av_values = high_elemnet.find_elements_by_class_name("right")
        if len(av_values) > 2:
          for i in range(0, 3):
            if (av_values[i].text == "") or (av_values[i].text == "-"):
              highest_list.append("0")
            else: 
              highest_list.append(av_values[i].text)
          

    three_way = { "highest": highest_list}
    odd_price['3way'] = three_way
    
    ###########################################################################

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
                  if (av_values[i+1].text == "")  or (av_values[i+1].text == "-"):
                    highest_list.append('0')
                  else:
                    highest_list.append(av_values[i+1].text)
    else:
        highest_list = ['0', '0']
        
      
    O_U = { "highest": highest_list}
    
    odd_price['O/U'] = O_U
    #####################################################################################
    print("        * start scraping Asian Handicap data --------------------")
    AH_odds = {"AH_2":{}, "AH_1.75":{}, "AH_1.5":{}, "AH_1.25":{}, "AH_1":{}, "AH_0.75":{}, "AH_0.5":{}, "AH_0.25":{}, "AH_0":{}}
    driver1.execute_script("uid(4)._onClick();")
    time.sleep(1)

    tfoot_OU = []
    
    tfoot_index = 0
    ##################= Asian Handicap 2.0 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -2 ']")
    
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -2 values !")
        AH_odds["AH_2"] = {"highest": ['0', '0']}
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
        AH_odds["AH_1.75"] = {   "highest": ['0', '0']}
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
            AH_odds["AH_1.75"] = {   "highest": ['0', '0']}
        
    ##################= Asian Handicap 1.5 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -1.5 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -1.5 values !")
        AH_odds["AH_1.5"] = {   "highest": ['0', '0']}
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
            AH_odds["AH_1.5"] = {   "highest": ['0', '0']}
    ##################= Asian Handicap 1.25 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -1.25 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -1.25 values !")
        AH_odds["AH_1.25"] = {   "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--1.25-0-0',this)")
            
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_1.25"] = AH
            driver1.execute_script("page.togleTableContent('P--1.25-0-0',this)")
        else:
            AH_odds["AH_1.25"] = {   "highest": ['0', '0']} 
    ##################= Asian Handicap 1 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -1 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -1 values !")
        AH_odds["AH_1"] = {   "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--1.00-0-0',this)")
            
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_1"] = AH
            driver1.execute_script("page.togleTableContent('P--1.00-0-0',this)")
        else:
            AH_odds["AH_1"] = {   "highest": ['0', '0']}
    ##################= Asian Handicap 0.75 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -0.75 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -0.75 values !")
        AH_odds["AH_0.75"] = {   "highest": ['0', '0']}
    else:
        container_parent = element_OU[0].find_element_by_xpath('./../../..');
        style_display = container_parent.value_of_css_property("display")
        if style_display != "none":
            driver1.execute_script("page.togleTableContent('P--0.75-0-0',this)")
            
            tfoot_OU = container_parent.find_elements_by_tag_name('tfoot')
            AH = get_various_AsianHandicap(tfoot_OU)
            AH_odds["AH_0.75"] = AH
            driver1.execute_script("page.togleTableContent('P--0.75-0-0',this)")
        else:
            AH_odds["AH_0.75"] = {   "highest": ['0', '0']}
    ##################= Asian Handicap 0.5 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -0.5 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -0.5 values !")
        AH_odds["AH_0.5"] = {   "highest": ['0', '0']}
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
            AH_odds["AH_0.5"] = {   "highest": ['0', '0']}  
    ##################= Asian Handicap 0.25 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap -0.25 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -0.25 values !")
        AH_odds["AH_0.25"] = {   "highest": ['0', '0']}
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
          AH_odds["AH_0.25"] = {   "highest": ['0', '0']}
    ##################= Asian Handicap 0 result#######################################################
    element_OU = driver1.find_elements_by_xpath("//a[text()='Asian handicap 0 ']")
    if len(element_OU) == 0:
        print("           Couldn't find Asian handicap -0.00 values !")
        AH_odds["AH_0"] = {   "highest": ['0', '0']}
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
          AH_odds["AH_0"] = {   "highest": ['0', '0']}
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
    
    highest_list = []
    if len(tfoot_OU):
        high_elemnet = tfoot_OU[0].find_element_by_class_name("highest")
        if high_elemnet:
            av_values = high_elemnet.find_elements_by_class_name("right")
            if len(av_values) > 1:
                for i in  range(0, 2):
                  if (av_values[i+1].text == "-") or  (av_values[i+1].text == ""):
                    highest_list.append("0")
                  else:
                    highest_list.append(av_values[i+1].text)
    else:
        highest_list = ['0', '0']
        
    AH = {"highest": highest_list}
    return AH

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
  elif 'Tomorrow' in date_txt:
      yesterday = datetime.now() + timedelta(1)
      return datetime.strftime(yesterday, '%Y-%m-%d')
  else:
      date_part = date_txt.split(' ');
      if len(date_part) > 2:
        return date_part[2] + "-" +switch_month(date_part[1]) + '-' + date_part[0]
      else:
        print("   * can't find correct date")
        return "No date"

def insert_Price_To_Matchplan(league, season):
    driver = webdriver.Chrome(driverpath,options=chrome_options)
    current_season = False
    
    page_url = site_url + league

    driver.get(page_url)
    
    print(f"----------------{league}  start--------------------------------")
  
    time.sleep(2)
    tbody = driver.find_element_by_tag_name('tbody')                # get tobody of all matches
    #print(tbody.text)
    index = 0
    match_date = ""
    all_tr_array = tbody.find_elements_by_tag_name("tr")

    for each_tr in all_tr_array:
        classField = each_tr.get_attribute('class')
        xeid = each_tr.get_attribute("xeid")
        
        if 'nob-border' in classField:                                # it means date tr
            date_th = each_tr.find_elements_by_tag_name('th')[0]
            date_txt = date_th.text
            if getDate_from_trTxt(date_txt) != "No date":
                match_date =  getDate_from_trTxt(date_txt)
        

        if xeid:                                                      # means match tr
            print(f"    --- {league} {index}th match start---")
            a_array = each_tr.find_elements_by_tag_name("a")
            
            if len(a_array) > 4:                                      # for TV series
                team_text = a_array[1].text
                hrefUrl = each_tr.find_elements_by_tag_name("a")[1].get_attribute('href')
            else :
                team_text = a_array[0].text
                hrefUrl = each_tr.find_elements_by_tag_name("a")[0].get_attribute('href')
            
            if (" - " in team_text):
                print(f"        {match_date} , {team_text} ")
                insert_update_odds(hrefUrl, match_date, team_text)              # get every match information
                index += 1
            else:
                print("        * Cant find match")
    print(f"---------------- {league} -  End--------------------------------")
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