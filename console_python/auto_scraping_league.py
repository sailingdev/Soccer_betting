import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3
from sportmonks import *
import locale
import time
#locale.setlocale( locale.LC_ALL, 'deu_deu') ##testing 

sportmonks_token = "4Kj1qmmeUiN7isAnIGBwHNYVUUzodVwvyJuyRi2UvVP61ignYAhdob3kRfIv"
http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

################################################################
# Steps followed while scraping:
#   1. Get all predictions from API
#   2. Loop the predication data and store only the required leagues
#################################################################

mydb = mysql.connector.connect(
  host="localhost",
  user="akhil",
  passwd="password",
  database="soccer"
)

mycursor = mydb.cursor(buffered=True)
seasonId = 857 #"2021-2022"


leagues = {	
  "aut-bundesliga": 1,                  # Austria
  "bul-parva-liga" : 2,       				  # Bulgaria
  "bul-a-grupa": 2,    
  "cze-1-fotbalova-liga": 3,            # Chezch
  "cze-gambrinus-liga": 3,		
  "cro-1-hnl": 4,                       # Croatia
  "den-superligaen": 5,                 # Denmark
  "den-sas-ligaen": 5,
  "eng-premier-league": 6,              # England
  "fra-ligue-1": 7,                     # France
  "bundesliga": 8,                      # Germany
  "gre-super-league": 9,                # Greece
  "hun-nb-i": 10,                       # Hungary
  "hun-nb1": 10,
  "hun-otp-liga": 10,
  "ita-serie-a": 11,                    # Italy
  "ned-eredivisie": 12,                 # Netherland
  "nor-eliteserien": 13,                # Norway from 2020
  "nor-tippeligaen": 13,
  "por-primeira-liga": 14,              # Portugal, Check
  "por-liga-sagres": 14,
  "srb-super-liga": 15,                 # Serbia
  "esp-primera-division": 16,           # Spain
  "swe-allsvenskan": 17,                # Sweden
  "sui-super-league": 18,               # Swiztland
  "tur-sueperlig": 19,                  # Turkey
  "ukr-premyer-liga": 20                # Ukraine
}

leaguelist = {
	"esp-primera-division"	: 564,  		#spain
	"eng-premier-league"	: 8,   			#England
	"bundesliga"			: 82,   		#Germany
	"ita-serie-a" 			: 384, 			#italy
	"fra-ligue-1" 			: 301,   		#france
	"ned-eredivisie"		: 72,  			#Netherland
	"aut-bundesliga"		: 181,  		#Austria
	"por-primeira-liga"		: 462,  		#portugal
	"gre-super-league"		: 325,   		#Greece
	"tur-sueperlig"			: 600,   		#Turkey
	"nor-eliteserien"		: 444,  		#Norway
	"swe-allsvenskan"		: 573, 			#Sweden
	"sui-super-league"		: 591,  		#Swiztland
	"den-superligaen"		: 271,     		#Denmark
	"ukr-premyer-liga"		: 609,     		#Ukraine
	"bul-parva-liga"		: 229,       	#bulgaria
	"cze-1-fotbalova-liga"	: 262,      	#Chezch
	"cro-1-hnl"				: 244 ,         #Croatia
	"hun-nb-i"				: 334,    		#Hungary
	"srb-super-liga"		: 531    		#Serbia
}

def storeData(predicationData,lgId):
  global seasonId
  sql = f'SELECT id from predictions where season_id = {seasonId} and league_id={lgId} limit 1'
  mycursor.execute(sql)
  result = mycursor.fetchall()
  if len(result):
    print("- No need to update. already saved in DB!", lgId)
  else:
    sql = f"INSERT INTO predictions (season_id, league_id, hit_ratio, log_loss, predictability, predictive_power, hll_3ways_ft, hll_btts, hll_overunder25, hll_overunder35, hll_scores, mhr_3ways_ft, mhr_btts, mhr_overunder25, mhr_overunder35, mhr_scores, mp_3ways_ft, mp_btts, mp_overunder25, mp_overunder35, mp_scores, mpp_3ways_ft, mpp_btts, mpp_overunder25, mpp_overunder35, mpp_scores, mll_3ways_ft, mll_btts, mll_overunder25, mll_overunder35, mll_scores, updated_at) VALUES ({seasonId}, {lgId}, '{predicationData['hit_ratio']}', '{predicationData['log_loss']}', '{predicationData['predictability']}', '{predicationData['predictive_power']}', '{predicationData['historical_log_loss']['3ways-ft']}', '{predicationData['historical_log_loss']['btts']}', '{predicationData['historical_log_loss']['overunder25']}', '{predicationData['historical_log_loss']['overunder35']}', '{predicationData['historical_log_loss']['scores']}', '{predicationData['model_hit_ratio']['3ways-ft']}', '{predicationData['model_hit_ratio']['btts']}', '{predicationData['model_hit_ratio']['overunder25']}', '{predicationData['model_hit_ratio']['overunder35']}', '{predicationData['model_hit_ratio']['scores']}', '{predicationData['model_predictability']['3ways-ft']}', '{predicationData['model_predictability']['btts']}', '{predicationData['model_predictability']['overunder25']}', '{predicationData['model_predictability']['overunder35']}', '{predicationData['model_predictability']['scores']}', '{predicationData['model_predictive_power']['3ways-ft']}', '{predicationData['model_predictive_power']['btts']}', '{predicationData['model_predictive_power']['overunder25']}', '{predicationData['model_predictive_power']['overunder35']}', '{predicationData['model_predictive_power']['scores']}', '{predicationData['model_log_loss']['3ways-ft']}', '{predicationData['model_log_loss']['btts']}', '{predicationData['model_log_loss']['overunder25']}', '{predicationData['model_log_loss']['overunder35']}', '{predicationData['model_log_loss']['scores']}', now())"
    print(sql)
    mycursor.execute(sql)
    mydb.commit()


def main():
  init(sportmonks_token)
  
  print(f"-------------Get all league predications----------------")
  leagueData = get(f'predictions/leagues')
  #print(leagueData)
  for one in leagueData:
    try:
      for lgName,lgId in leaguelist.items(): 
        if one['predictability']['data']['league_id'] == lgId:
          print("Storing --- ", lgId, lgName)
          storeData(one['predictability']['data'], lgId)
          
    except KeyError:
      print(f"------------- predictability Data not found. Name: {one['name']} ----------------")


if __name__ == "__main__":
	main()

