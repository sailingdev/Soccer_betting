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
# This is the sample instructions to insert the match plan and match-player info.
# insert_match_plan("2021-2022", "eng-premier-league", 1, 5)  match 1~ 5 eg: England 1 ~ 380
# direct write the info for inserting..... for saving time.
#################################################################

mydb = mysql.connector.connect(
  host="localhost",
  user="akhil",
  passwd="password",
  database="soccer"
)

mycursor = mydb.cursor(buffered=True)
seasons = {
  "2021-2022": 857
}

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


def main():
  init(sportmonks_token)
  for season, value1 in seasons.items():
    for league, value2 in leagues.items():
      print(season, league, f'predictions/valuebets/fixture/{value2}')
      leagueData = get(f'predictions/valuebets/fixture/{value2}', None, value2)
      print(leagueData)
      exit(0)


if __name__ == "__main__":
	main()

