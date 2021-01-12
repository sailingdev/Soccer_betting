#########################################
 # date: 12/26/2020
 # Writer: Matthew David S.
 # skype: SuperStar.Dev
#########################################

import requests
from bs4 import BeautifulSoup
import argparse
import pandas as pd
from selenium import webdriver 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time


driverpath = "E:\Zillow_scraping_project\chromedriver.exe"
chrome_options = Options()
chrome_options.add_argument('headless')
chrome_options.add_experimental_option("excludeSwitches", ['enable-logging']);
chrome_options.add_argument('ignore-certificate-errors')
chrome_options.add_argument('--disable-gpu')

chrome_options.add_argument("--disable-extensions")
chrome_options.page_load_strategy = 'eager'
chrome_options.binary_location = 'C:\Program Files\Google\Chrome\Application\chrome.exe'



def get_data(url):
    address_list = []
    cost_list = []
    bds_list = []
    ba_list = []
    sqft_list = []
    date_list = []

    print(" URL : ", url)
    count = 0
    driver1 = webdriver.Chrome(driverpath,options=chrome_options)
    driver1.get(url)
    time.sleep(2)
    result_container = driver1.find_element_by_id("grid-search-results");
    ul_list = result_container.find_elements_by_tag_name("ul")[0]
    li_list = ul_list.find_elements_by_tag_name('li')
  
    for each_li in li_list:
        if each_li.find_elements_by_class_name("list-card-price"):
            price = each_li.find_element_by_class_name("list-card-price").text
            cost_list.append(price)
        if each_li.find_elements_by_class_name("list-card-link"):
            address = each_li.find_element_by_class_name("list-card-link").text
            
            address_list.append(address)
        if each_li.find_elements_by_class_name("list-card-details"):
            ul_card_details = each_li.find_element_by_class_name("list-card-details")
            detail_li_list = ul_card_details.find_elements_by_tag_name("li")
            if detail_li_list:
                
                bds = detail_li_list[0].text
                bds = bds.split(",")[0]
                bds_list.append(bds)

                ba = detail_li_list[1].text
                ba = ba.split(",")[0]
                ba_list.append(ba)

                sqft = detail_li_list[2].text
                sqft_list.append(sqft)
                
        if each_li.find_elements_by_class_name("list-card-variable-text"):
            sold_date = each_li.find_element_by_class_name("list-card-variable-text").text
            sold_date = sold_date.split(" ")[1]
            date_list.append(sold_date)
            count += 1

        
    data = {
			'adress' 	: 	address_list,
			'cost'		:	cost_list,
			'bds'       : 	bds_list,
			'ba'		: 	ba_list,
			'sqft'  	: 	sqft_list,
			'date' : 	date_list	
	}
    df = pd.DataFrame(data,columns= ['adress','cost', 'bds', "ba", "sqft", "date"])
    df.to_csv(f"data.csv", index=False, sep='\t', encoding = 'utf-16', mode='a', header=False)
    

def main():
    url = "https://www.zillow.com/goochland-va/sold/house,townhouse_type/2-_beds/2.0-_baths/"
    for page in range(1, 7):
        get_data(url + str(page) + "_p/")


if '__main__' == __name__:
	main()