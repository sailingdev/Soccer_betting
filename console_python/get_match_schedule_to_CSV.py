import requests
from bs4 import BeautifulSoup
import argparse
import pandas as pd
import mysql.connector



################################################################
# This is the sample instructions to insert the match plan and match-player info.
# insert_match_plan("2014-2015", "eng-premier-league", 1,5)  match 1~ 5 eg: England 1 ~ 380
# direct write the info for inserting..... for saving time.
#################################################################
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="soccer"
)
mycursor = mydb.cursor()

def switch_season(argument):
	switcher = {
	  "2019-2020": 12,
	  "2019-2020-qualifikationsgruppe": 567,
	  "2019-2020-meistergruppe": 145,
	  "2019-2020-abstieg" : 86,
	  "2019-2020-meisterschaft": 234, 
	  "2020": 64

	}
	return switcher.get(argument, "null")
def switch_league(argument):
	switcher = {
	  "esp-primera-division": 16,  #spain
	  "eng-premier-league": 6,   #England
	  "bundesliga": 8,   #Germany
	  "ita-serie-a" : 11,  #italy
	  "fra-ligue-1" : 7,   #france
	  "ned-eredivisie": 12,  #Netherland
	  "aut-bundesliga": 1,  #Austria
		"por-primeira-liga": 14,  #portugal
		"por-liga-sagres": 14,
		"por-liga-zon-sagres":14,
		
		"gre-super-league": 9,   #Greece
		"tur-sueperlig": 19,   #Turkey
		"nor-eliteserien": 13,  #Norway
		"nor-tippeligaen":13,
		"swe-allsvenskan": 17,  #Sweden
		"sui-super-league": 18,   #Swiztland
		"den-superliga": 5,     #Denmark
		"den-sas-ligaen":5,
		"ukr-premyer-liga": 20,     #Ukraine
		"bul-a-grupa": 2,       #bulgaria
		"bul-parva-liga" : 2 , 
		"cze-1-fotbalova-liga": 3,      #Chezch
		"cze-gambrinus-liga": 3,
		"cro-1-hnl": 4 ,          #Croatia
		"hun-nb-i": 10,     #Hungary
		"hun-nb1": 10,
		"hun-otp-liga":10,
		"srb-super-liga": 15    #Serbia
	}
	return switcher.get(argument, "null")
def get_Real_LeagueUrl(argument):
	switcher = {
		"Spain La Liga" 			:		 "esp-primera-division",  	#spain
		"English Premier League" 	:		 "eng-premier-league",   	#England
		"German Bundesliga 1" 		: 		 "bundesliga",  			#Germany
		"Italy Serie A" 			: 		 "ita-serie-a",  			#italy
		"French Ligue 1" 			: 		 "fra-ligue-1",   			#france
		"Netherlands Eredivisie" 	: 		 "ned-eredivisie", 			#Netherland
		"Austrian Tipico Bundesliga": 		 "aut-bundesliga",		    #Austria
		"Portugal Primeira Liga"	: 		 "por-primeira-liga",  		#portugal
		"Greece Super League" 		: 		 "gre-super-league",   		#Greece
		"Turkish Super Lig" 		: 		 "tur-sueperlig",  			#Turkey
		"Norway Eliteserien" 		: 		 "nor-eliteserien",  		#Norway
		"Sweden Allsenskan" 		: 		 "swe-allsvenskan",  		#Sweden
		"Swiss Super League" 		: 		 "sui-super-league",   		#Swiztland
		"Denmark Superliga" 		: 		 "den-superliga",    		 #Denmark
		"Ukraine Premier League" 	: 		 "ukr-premyer-liga",     	#Ukraine
		"Bulgarina Parva Lig"		: 		 "bul-parva-liga",     		#bulgaria
		"Czech 1.Liga"				: 		 "cze-1-fotbalova-liga",     #Chezch
		"Croatia 1. HNL" 			: 		 "cro-1-hnl" ,         		 #Croatia
		"Hungary OTP Bank Liga" 	: 		 "hun-nb-i",     			#Hungary
		"Serbia Super Liga" 		: 		 "srb-super-liga"   		#Serbia
	}
	return switcher.get(argument, "null")

def insert_match_plan(season=None , league=None, firstMatch = None, lastMatch = None):
	LeagueList = []
	seasonlist = []
	YearList = []
	MonthList = []
	DayList = []
	timeList = []
	hometeamList = []
	homerankingList = []
	awayteamList = []
	awayrankingList = []
	statusList = []
	

	print(f"--------------------------------- start-----------------------------------------")
	realLeague = get_Real_LeagueUrl(league)
	if season:
		URL = f"https://www.worldfootball.net/all_matches/{realLeague}-{season}/"
	else:
		# URL = f"https://www.worldfootball.net/all_matches/eng-premier-league-2014-2015/"
		print("Enter the season !")
		return
		
	page = requests.get(URL)
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find('table', class_="standard_tabelle")
	
	tr_results = results.find_all("tr")

	for ev_tr in tr_results:
		if ev_tr.find_all("td"):
			pass
		else:
			tr_results.remove(ev_tr)

	match_date=""
	for i in range(firstMatch-1,lastMatch):
		all_td = tr_results[i].find_all("td")
		if(len(all_td)) :
			print(f"------------------ {i + 1}th Match process start --------------------")
			LeagueList.append(league)
			seasonlist.append(season)

			if all_td[0].text !="":
				match_date = all_td[0].text
			dateSplitList = match_date.split("/")	
			DayList.append(dateSplitList[0])
			MonthList.append(dateSplitList[1])
			YearList.append(dateSplitList[2])
			timeList.append(all_td[1].text)
			hometeamList.append(all_td[2].text)
			awayteamList.append(all_td[4].text)
			match_total_result = all_td[5].text.strip()
			if "-" in match_total_result:
					statusList.append("Open")
			else:
					statusList.append(match_total_result)

			sql = f'SELECT team_id FROM team_list WHERE team_name = "{all_td[2].text}" UNION ' \
				  f'SELECT team_id FROM team_list WHERE team_name = "{all_td[4].text}"'
			mycursor.execute(sql)
			myresult = mycursor.fetchall()
			
			home_team_id = myresult[0][0]
			#print(home_team_id)
			away_team_id = myresult[1][0]


			sql = f"SELECT S_H_ranking FROM season_league_team_info WHERE team_id = {home_team_id} AND ARS != '' ORDER BY info_id DESC LIMIT 1"
			mycursor.execute(sql)
			myresult = mycursor.fetchall()
			if len(myresult):
				homerankingList.append(myresult[0][0])
				#print(myresult[0][0])
			else:
				homerankingList.append("new")

			sql = f"SELECT S_A_ranking FROM season_league_team_info WHERE team_id = {away_team_id} AND ARS != '' ORDER BY info_id DESC LIMIT 1"
			mycursor.execute(sql)
			myresult = mycursor.fetchall()
			if len(myresult):
				awayrankingList.append(myresult[0][0])
			else:
				awayrankingList.append("new")
			
			
			print("   1 record inserted, ID:  in List")
			
			print(f"------------------ {i + 1}th Match process end --------------------")
			i += 1
			

	reallist = {
			'League' : LeagueList,
			'Season' : seasonlist,
			'Year'   : YearList,
			"Month"  : MonthList,
			'Date' 	 : DayList,
			'Time'   : timeList,
			'Home Team'   : hometeamList,
			'Home Ranking' : homerankingList,
			"Away Team"   : awayteamList,
			'Away Ranking' : awayrankingList,
			'Status' : statusList,
	}
	df = pd.DataFrame(reallist,columns= ['League','Season', 'Year', "Month", "Date", "Time", "Home Team","Home Ranking" , "Away Team", "Away Ranking","Status"])
	df.to_csv("Match_Fixture.csv", index=False, sep='\t', encoding = 'utf-16', mode='a', header=False)
	#df.to_csv("Match_Fixture.csv", index=False, sep='\t', encoding = 'utf-16', mode='a')
	print(f"--------------------------------- end -----------------------------------------")

#################################################################################################

insert_match_plan("2020-2021","Spain La Liga",1,380)
insert_match_plan("2020-2021","English Premier League",1,380)
insert_match_plan("2020-2021","German Bundesliga 1",1,306)
insert_match_plan("2020-2021","Netherlands Eredivisie",1,306)
insert_match_plan("2020-2021","French Ligue 1",1,380)
insert_match_plan("2020-2021","Austrian Tipico Bundesliga",1,132)
insert_match_plan("2020-2021","Portugal Primeira Liga",1,306)
insert_match_plan("2020-2021","Turkish Super Lig",1,420)
insert_match_plan("2020","Norway Eliteserien",1,240)
insert_match_plan("2020","Sweden Allsenskan",1,240)
insert_match_plan("2020-2021","Swiss Super League",1,90)
insert_match_plan("2020-2021","Denmark Superliga",1,132)
insert_match_plan("2020-2021","Ukraine Premier League",1,91)
insert_match_plan("2020-2021","Bulgarina Parva Lig",1,182)
insert_match_plan("2020-2021","Czech 1.Liga",1,306)
insert_match_plan("2020-2021","Croatia 1. HNL",1,180)
insert_match_plan("2020-2021","Hungary OTP Bank Liga",1,198)
insert_match_plan("2020-2021","Serbia Super Liga",1,380)
insert_match_plan("2020-2021","Italy Serie A",1,380)
insert_match_plan("2020-2021","Greece Super League",1,21)