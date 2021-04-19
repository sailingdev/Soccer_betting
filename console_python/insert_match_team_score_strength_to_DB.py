# This file must be runed after inert_team_point_to_DB.py because 
# here we have to calculare Static ranking using team points
import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3
import sys
import json

def switch_season(argument):
	switcher = {
	"2010-2011": 19,
	"2011-2012": 17,
	"2012-2013": 15,
	"2013-2014": 13,
	"2014-2015": 1,
	"2015-2016": 2,
	"2016-2017": 3,
	"2017-2018": 4,
	"2018-2019": 5,
		"2010": 20,
		"2011": 18,
		"2012": 16,
		"2013": 14,
		"2014": 6,
		"2015": 7,
		"2016": 8,
		"2017": 9,
		"2018": 10,
		"2019": 11, 
	}
	return switcher.get(argument, "null")
def switch_league(argument):
	switcher = {
	  
	  "england/premier-league": 6,   #England
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

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="soccer"
)
mycursor = mydb.cursor()
updated_count = 0
def insert_match_team_socre_strength_TPGR():
	global updated_count
	
	sql = f"SELECT  * FROM season_match_plan where match_id BETWEEN 53859 and 59579"
	
	mycursor.execute(sql)
	wholeMatchResult = mycursor.fetchall()
	length_result = len(wholeMatchResult)
	for i in range(0, length_result):                   # loop for each match in match plan total 53856
		print(f"------------{i + 1}th row Start-------------")
		if wholeMatchResult[i][11] == "END":
			if wholeMatchResult[i][18] == None:         # the game ended but team score not added yet . so will calculate!
				match_id = wholeMatchResult[i][0]
				season_id = wholeMatchResult[i][1]
				home_team_id = wholeMatchResult[i][5]
				away_team_id = wholeMatchResult[i][6]

				home_TGPR = get_team_TGPR(match_id, season_id, home_team_id)
				home_TGPR = round(home_TGPR, 3)
				print(f"   home TGPR {home_TGPR}")
				
				away_TGPR = get_team_TGPR(match_id, season_id, away_team_id)
				away_TGPR = round(away_TGPR, 3)
				print(f"   away_TGPR  {away_TGPR}")

				home_team_score_strenth_list = get_team_score_strength_of_team(match_id, home_team_id,season_id)    # get home team score and its length
				print(home_team_score_strenth_list[0], home_team_score_strenth_list[1])                             # socre and strength
				away_team_score_strenth_list = get_team_score_strength_of_team(match_id, away_team_id,season_id)    # get home team score and its length
				print(away_team_score_strenth_list[0], away_team_score_strenth_list[1])

				sql = f"update season_match_plan set Home_TGPR ='{home_TGPR}', Away_TGPR = '{away_TGPR}' \
					, home_team_score = '{home_team_score_strenth_list[0]}' ,home_team_strength = '{home_team_score_strenth_list[1]}' \
					, away_team_score = '{away_team_score_strenth_list[0]}' ,away_team_strength = '{away_team_score_strenth_list[1]}' \
					where match_id = {match_id}"
				#print(sql)
				mycursor.execute(sql)
				mydb.commit()
				
				print("     Successfully Inserted!")
				updated_count += 1
			else:
			 	print("     already added before")
						
		else:
			print("    not ended yet")
		print(f"------------{i + 1}th row End-------------")

def get_player_TGPR_season(player_id, season_id):
	sql = f'SELECT A.season_id, A.goals, A.started FROM player_career AS A INNER JOIN season AS B ON A.season_id = B.season_id WHERE player_id = {player_id} ORDER BY B.season_title ASC'
	mycursor.execute(sql)
	wholeresult = mycursor.fetchall()
	player_TGPR = 0
	total_goals = 0
	total_started = 0
	for result in wholeresult:
		if result[0] ==  season_id:
			break
		else:
			total_goals += result[1]
			total_started += result[2]
	if total_started != 0:
		player_TGPR = total_goals / total_started
	
	return player_TGPR;

def get_team_TGPR(match_id, season_id, team_id):
	sql = f"SELECT  * FROM match_team_player_info where match_id = {match_id} and team_id = {team_id}"
	mycursor.execute(sql)
	wholePlayerResult = mycursor.fetchall()
	team_TPGR = 0
	NumberOfPlayer = 0                      # the number of player who have points
	for eachPlayer in wholePlayerResult:    # loop each plaer
		player_id =  eachPlayer[3]
		Player_TPGR = get_player_TGPR_season(player_id , season_id)
		#print(f"      player tpgr is {Player_TPGR}")
		team_TPGR += Player_TPGR

	return team_TPGR / 11

def get_team_score_strength_of_team(match_id, team_id ,season_id):
	print(f"match_id - {match_id}, team_id - {team_id}, season_id - {season_id}")

	sql = f"SELECT  * FROM match_team_player_info where match_id = {match_id} and team_id = {team_id}"
	mycursor.execute(sql)
	wholePlayerResult = mycursor.fetchall()
	team_score = 0
	NumberOfPlayer = 0                      # the number of player who have points

	for eachPlayer in wholePlayerResult:    # loop each plaer
		player_id =  eachPlayer[3]
		#print(f"      player id -  {player_id}")
		Player_score = get_player_score_season(player_id , season_id)
		#print(f"      player tpgr is {Player_score}")
		team_score += Player_score
	#print(f"     team score {team_score}")

	team_strength = get_strength(team_score)
	return_list = [team_score, team_strength]

	return return_list

def get_player_score_season(player_id , season_id):
	sql = f'SELECT A.season_id, A.goals, A.started FROM player_career AS A INNER JOIN season AS B ON A.season_id = B.season_id WHERE player_id = {player_id} ORDER BY B.season_title ASC'
	mycursor.execute(sql)
	wholeresult = mycursor.fetchall()
	player_TGPR = 0
	total_goals = 0
	total_started = 0
	for result in wholeresult:
		if result[0] ==  season_id:
			break
		else:
			total_goals += result[1]
			total_started += result[2]
	if total_started != 0:
		player_TGPR = total_goals / total_started
	
	player_TGPR = round(player_TGPR, 2)

	if total_started >= 20 :
		if player_TGPR < 0.13:
			return 0
		if (player_TGPR >= 0.13) & (player_TGPR < 0.3):
			return 100
		if (player_TGPR >= 0.3) & (player_TGPR < 0.76):
			return 1000
		if (player_TGPR >= 0.76):
			return 10000
	else :
		if  (player_TGPR < 0.13):
			return 0
		else:
			return 100

def get_strength(score):
	if (score < 400) :
		return "Weak"
	if (score >= 400) & (score <= 900):
		return "Medium"
	if (score >= 1000) & (score <= 1200):
		return "Weak"
	if (score >= 1300) & (score < 2000):
		return "Medium"
	if (score >= 2000) & (score <= 2100):
		return "Weak"
	if (score == 2200):
		return "Medium"
	if (score >= 2300) & (score < 3000):
		return "Strong"
	if (score == 3000):
		return "Weak"
	if (score == 3100):
		return "Medium"
	if (score >= 3200) & (score < 4000):
		return "Strong"
	if (score == 4000):
		return "Medium"
	if (score >= 4100) & (score < 10000):
		return "Strong"
	if (score >= 10000) & (score <= 10200):
		return "Weak"
	if (score == 10300):
		return "Medium"
	if (score >= 10400) & (score < 11000):
		return "Strong"
	if (score >= 11000) & (score <= 11100):
		return "Weak"
	if (score == 11200):
		return "Medium"
	if (score >= 11300) & (score < 12000):
		return "Strong"
	if (score == 12000):
		return "Weak"
	if (score == 12100):
		return "Medium"
	if (score >= 12200) & (score < 20000):
		return "Strong"
	if (score >= 20000) & (score < 20300):
		return "Weak"
	if (score >= 20300) & (score < 21000):
		return "Strong"
	if (score == 21000):
		return "Medium"
	if (score >= 21100):
		return "Strong"
	
def main():
	insert_match_team_socre_strength_TPGR()
	print (f"--------------- updated count number is : {updated_count} --------------------")
   
if __name__ == "__main__":
	main()

