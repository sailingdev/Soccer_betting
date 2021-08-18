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
	  "2020": 64,
	  "2020-2021" : 799,
	  "2021" : 844 ,
      "2021-2022" : 857
	}
	return switcher.get(argument, "null")

def switch_league(argument):
    switcher = {	
        "aut-bundesliga": 1,                  # Austria
        "bul-parva-liga" : 2,				  # Bulgaria
		"bul-a-grupa": 2,    
		"cze-1-fotbalova-liga": 3,            # Chezch
        "cze-gambrinus-liga": 3,		
		"cro-1-hnl": 4,                       # Croatia
		"den-superligaen": 5,                   # Denmark
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
		"tur-sueperlig": 19,                   # Turkey
        "ukr-premyer-liga": 20                # Ukraine
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
		"Denmark Superliga" 		: 		 "den-superligaen",    		 #Denmark
		"Ukraine Premier League" 	: 		 "ukr-premyer-liga",     	#Ukraine
		"Bulgarina Parva Lig"		: 		 "bul-parva-liga",     		#bulgaria
		"Czech 1.Liga"				: 		 "cze-1-fotbalova-liga",     #Chezch
		"Croatia 1. HNL" 			: 		 "cro-1-hnl" ,         		 #Croatia
		"Hungary OTP Bank Liga" 	: 		 "hun-nb-i",     			#Hungary
		"Serbia Super Liga" 		: 		 "srb-super-liga"   		#Serbia
	}
	return switcher.get(argument, "null")

def whole_matches_ranking_to_csv():
	

	print(f"--------------------------------- start-----------------------------------------")
	
	sql = 'SELECT D.league_title , E.season_title, A.date, SUBSTRING(A.date, 7, LENGTH(A.date) ) AS YEAR,SUBSTRING(A.date, 4, 2 ) AS MONTH, CONCAT( B.team_name," : ", C.team_name) AS Game,  \
		CONCAT(A.total_home_score, " :: ", A.total_away_score) AS FTScore,CASE \
		WHEN A.total_home_score > A.total_away_score THEN "H" \
		WHEN A.total_home_score = A.total_away_score THEN "D" \
		ELSE "A" \
		END AS FTR, A.total_home_score, A.total_away_score, (A.total_home_score + A.total_away_score) AS TotalGoals , B.team_name AS home_team,  \
		F.HRS as static_HRS, F.S_H_ranking, A.HPPG, A.HGDPG, A.D_Home_RS_8, A.D_Home_ranking_8,A.D_Home_RS_6, A.D_Home_ranking_6, \
		A.home_team_score, A.home_team_strength, C.team_name,  G.ARS as static_ARS, G.S_A_ranking, A.APPG, A.AGDPG , A.D_away_RS_8 ,A.D_Away_ranking_8, A.D_away_RS_6 ,A.D_Away_ranking_6,A.away_team_score, A.away_team_strength,  \
		CONCAT(F.S_H_ranking, " v ", G.S_A_ranking) AS static_ranking, CONCAT(A.D_Home_ranking_8, " v ", A.D_Away_ranking_8) AS Dynamic_ranking_8 ,CONCAT(A.D_Home_ranking_6, " v ", A.D_Away_ranking_6) AS Dynamic_ranking_6 ,\
		A.wd_1 as Home_price, \
		A.wd_x as Draw_price, \
		A.wd_2 as Away_price, \
		A.over_2d5 as Away_price, \
		A.under_2d5 as Away_price \
		FROM season_match_plan AS A \
		INNER JOIN team_list AS B ON A.home_team_id = B.team_id \
		INNER JOIN team_list AS C ON A.away_team_id = C.team_id \
		INNER JOIN league AS D ON D.league_id = A.league_id \
		INNER JOIN season AS E ON E.season_id = A.season_id \
		INNER JOIN season_league_team_info AS F ON A.season_id = F.season_id AND A.league_id = F.league_id AND A.home_team_id = F.team_id \
		INNER JOIN season_league_team_info AS G ON A.season_id = G.season_id AND A.league_id = G.league_id AND A.away_team_id = G.team_id'
	mycursor.execute(sql)
	myresult = mycursor.fetchall()
	
	df = pd.DataFrame(myresult,columns= ['League','Season',"Date", 'Year', "Month",  "Game", "FT Score","FTR","FTHG","FTAG","Total Goals", 
		"Home Team","Static HRS","Static Home Rank", "HPPG", "HGDPG", "Dynamic_HRS_8","Dynamic Home Rank_8", "Dynamic_HRS_6","Dynamic Home Rank_6","Home Team Score", "Home Team Strength" ,
		"Away Team", "Static ARS","Static Away Rank", "APPG","AGDPG" ,"Dynamic_ARS_8", "Dynamic Away Rank_8","Dynamic_ARS_6", "Dynamic Away Rank_6","Away Team Score", "Away Team Strength",
		"Static Rank", "Dynamic Rank_8", "Dynamic Rank_6", "Home_price", "Draw_price", "Away_price", "Over 2.5_Price", "Under 2.5_Price"])
	df.to_csv("Match_rank.csv", index=False, sep='\t', encoding = 'utf-16', mode='a', header=True)
	#df.to_csv("Match_Fixture.csv", index=False, sep='\t', encoding = 'utf-16', mode='a')
	print(f"--------------------------------- end -----------------------------------------")

#################################################################################################


whole_matches_ranking_to_csv()


