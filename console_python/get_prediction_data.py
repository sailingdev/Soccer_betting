import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3
from sportmonks import *
import locale
import time
import datetime

#locale.setlocale( locale.LC_ALL, 'deu_deu') ##testing 

sportmonks_token = "qWE7Q9apsiG3xGhxldIdekQYk5c1ERCW4gdtKoF0tas64syfCD7ooW3Nxs48"
http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

################################################################
# Steps followed while scraping:
#   1. Get all predictions from API
#   2. Loop the predication data and store only the required leagues
#################################################################

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="P@ssw0rd2021",
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

allColumnNames = []
def setColumnNames():
  global allColumnNames
  sql = f'show columns from predictions'
  mycursor.execute(sql)
  result = mycursor.fetchall()
  for k in result:
    allColumnNames.append(k[0])
  
def storeData(predicationData):
  global allColumnNames;
  sql = f'SELECT id from predictions where league_id = "{predicationData["league_id"]}" and fixture_id={predicationData["fixture_id"]} and season_id="{predicationData["season_id"]}" and match_date="{predicationData["match_date"]}" limit 1'
  mycursor.execute(sql)
  result = mycursor.fetchall()
  if len(result):
    print("- No need to update. already saved in DB!", predicationData['fixture_id'])
  else:
    columns = [];
    values = [];
    for k,v in predicationData.items():
      if k not in allColumnNames:
        print("Adding new column: ", k);
        mycursor.execute("alter table predictions add column `"+k+"` varchar(15) default null")
        mydb.commit()
        allColumnNames.append(k)
        
      columns.append( "`"+k+"`")
      if type(v) != str:
        v = str(v)
      values.append('"'+v+'"');  
      
    insertQuery = "insert into predictions("+", ".join(columns)+") values("+", ".join(values)+")"
    print(insertQuery)
    mycursor.execute(insertQuery)
    mydb.commit()
    
    
allTeamInfo = {}
def findTeamId(teamId):
  global allTeamInfo;
  
  if allTeamInfo.get(teamId) == None:
    teamData = get(f'teams/{teamId}')
    print("Find teamId by name: ", teamData['name'])
    sql = f"SELECT team_id from team_list where team_name = '{teamData['name']}' or team_name_odd = '{teamData['name']}'"
    mycursor.execute(sql)
    result = mycursor.fetchall()
    if result == []:
      print("=======TeamId not found in DB====.. Adding new team");
      sql = f"insert into team_list(team_name, team_name_odd, img_src) values('{teamData['name']}', '{teamData['name']}', '{teamData['logo_path']}')"
      print(sql)
      mycursor.execute(sql)
      mydb.commit()
      allTeamInfo[teamId] = mycursor.lastrowid 
      return mycursor.lastrowid
    else:
      print("-------TeamId found: ", result[0][0])
      allTeamInfo[teamId] = result[0][0]
      return result[0][0]
  else:
    return allTeamInfo.get(teamId)

def main():
  init(sportmonks_token)
  
  setColumnNames();
 
  date = datetime.datetime(2022,2,26)
  endDate = "2022-12-31"
  for i in range(365): 
    date += datetime.timedelta(days=1)
    match_date = date.strftime("%Y-%m-%d")
    if endDate == match_date:
      break
    for lgName,lgId in leaguelist.items():  
      print("=======Datewise API call - Date: ", match_date, ", LeagueId: ", lgId)
      match_data_ofDay = get(f'fixtures/date/{match_date}', None, lgId)
      if match_data_ofDay != None:
        for det1 in match_data_ofDay:
          fixture_id = det1['id']
          print("Fixture details: ", fixture_id, det1['localteam_id'], det1['visitorteam_id'])
          leagueData = get(f'predictions/probabilities/fixture/{fixture_id}', None, lgId)
          valueBetData = get(f'predictions/valuebets/fixture/{fixture_id}')
          try:
            league_id = leagues[list(leaguelist.keys())[list(leaguelist.values()).index(lgId)]]
            mycursor.execute('SELECT league_title FROM soccer.league where league_id = (%s);', (league_id,))
            league_name = list(mycursor.fetchone())[0]
          except Exception as ex:
            print('League Name not found' + str(ex))
          try:
            home_team_id = findTeamId(det1['localteam_id'])
            away_team_id = findTeamId(det1['visitorteam_id'])
            mycursor.execute('SELECT team_name FROM soccer.team_list where team_id = (%s);', (home_team_id,))
            home_team = list(mycursor.fetchone())[0]
            mycursor.execute('SELECT team_name FROM soccer.team_list where team_id = (%s);', (away_team_id,))
            away_team = list(mycursor.fetchone())[0]
          except Exception as ex:
            print('Team names not found' + str(ex))
          try:
            mycursor.execute('SELECT season_title FROM soccer.season where season_id = (%s);', (seasonId,))
            season_name = list(mycursor.fetchone())[0]
          except Exception as ex:
            print('Season Name not found' + str(ex))
          
          if leagueData != None:
            dataData = {
              "league_id" : league_id,
              "league_name": league_name,
              "fixture_id" : fixture_id,
              "season_id" : season_name,
              "home_team" : home_team,
              "away_team" : away_team,
              "match_date": match_date 
            }
            if valueBetData != None:
              if valueBetData['fixture_id'] == fixture_id:
                print("**************AutoBet found");
                for k3,v3 in valueBetData['predictions'].items():
                  k3 = "autobet_"+k3
                  dataData[k3] = v3

            for k,v in leagueData['predictions'].items():
              if k == "correct_score":
                for k2,v2 in leagueData['predictions']['correct_score'].items():
                  k2 = "correct_score_"+k2
                  dataData[k2] = v2  
              else:  
                dataData[k] = v
            storeData(dataData)


if __name__ == "__main__":
	main()

