import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3
from sportmonks import *
import locale
import time
locale.setlocale( locale.LC_ALL, 'deu_deu') 

sportmonks_token = "4Kj1qmmeUiN7isAnIGBwHNYVUUzodVwvyJuyRi2UvVP61ignYAhdob3kRfIv"
http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

################################################################
# This is the sample instructions to insert the match plan and match-player info.
# insert_match_plan("2021-2022", "eng-premier-league", 1, 5)  match 1~ 5 eg: England 1 ~ 380
# direct write the info for inserting..... for saving time.
#################################################################

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="P@ssw0rd2021",
  database="soccer"
)

NotFoundMatch_count = 0

mycursor = mydb.cursor(buffered=True)
def switch_season(argument):
    switcher = {
        "2019-2020": 12,
        "2020": 64,
        "2021-2022" : 799,
        "2021"    : 844,
        "2021-2022": 857
    }
    return switcher.get(argument, "null")

def get_leagueid_DB(argument):
	switcher = {	
        "aut-bundesliga": 1,                  # Austria
        "bul-parva-liga" : 2,				  # Bulgaria
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
	return switcher.get(argument, "null")
	
def get_bookmakerid_API(argument):
	switcher = {
		"bet365" 		: 2, 
		"Betfair" 		: 15,
		"Betway" 		: 271057011,
		"Dafabet" 		: 25,
		"Matchbook" 	: 44 ,
		"Pncl" 			: 70, 
		"Sbo" 			: 25679320 , 
		"Unibet" 		: 97,
		"WilliamHill" 	: 187
	}
	return switcher.get(argument, "null")

def get_bookmakerid_DB(argument):
	switcher = {
		"bet365" 		: 1, 
		"Betfair" 		: 2,
		"Betway" 		: 3,
		"Dafabet" 		: 4,
		"Matchbook" 	: 5,
		"Pncl" 			: 6, 
		"Sbo" 			: 7, 
		"Unibet" 		: 8,
		"WilliamHill" 	: 9
	}
	return switcher.get(argument, "null")

def get_marketingid_DB(argument):
	switcher = {
		"Home" 		: 1, 
		"Draw" 		: 2,
		"Away" 		: 3,
		"Over2.5" 	: 4 ,
		"Under2.5" 	: 5 ,
	}
	return switcher.get(argument, "null")

def get_leagueid_API(argument):
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
	return leaguelist.get(argument, "null")

def insert_bookmaker_odd(match_id, fixture_id, bookmaker_name):
	home_value = 0
	draw_value = 0
	away_value = 0
	over_value = 0
	under_value = 0

	bookmaker_API_id = get_bookmakerid_API(bookmaker_name)
	print("   @ ",bookmaker_name)
	odd_data_array = get_prematch_odds_by_fixture_bookmakers(str(fixture_id), str(bookmaker_API_id))
	if odd_data_array is None:
		print("    - Not found odd data")
	else:
		for each_odd in odd_data_array:
			if each_odd['id'] == 1: 							# 3 way results 
				print("    - found 3 way result odd in API")
				bookmaker_data_array = each_odd['bookmaker']['data'][0]['odds']['data']

				for each_data in bookmaker_data_array:
					if each_data['label'] == "1":				# value is Home
						home_value = each_data['value']
						if "," in str(home_value):
							home_value = 0
					if each_data['label'] == "X":				# value is Home
						draw_value = each_data['value']
						if "," in str(draw_value):
							draw_value = 0
					if each_data['label'] == "2":				# value is Home
						away_value = each_data['value']
						if "," in str(away_value):
							away_value = 0
			if each_odd['id'] == 12:							# Over/Under odd
				print("    - found Over/Under odd in API")
				bookmaker_data_array = each_odd['bookmaker']['data'][0]['odds']['data']
				for each_data in bookmaker_data_array:
					if (each_data['label'] == "Over") & (each_data['total'] == "2.5"):				# value is Home
						over_value = each_data['value']
						if "," in str(over_value):
							over_value = 0
					if (each_data['label'] == "Under" ) & (each_data['total'] == "2.5"):			# value is Home
						under_value = each_data['value']
						if "," in str(under_value):
							under_value = 0
		print(f'     Home: {home_value} , Darw: {draw_value}, Away: {away_value}, Over: {over_value} , Under: {under_value}')
		
		bookmaker_id = get_bookmakerid_DB(bookmaker_name)
		marketing_id = get_marketingid_DB('Home')
		sql = f"INSERT INTO odds (match_id, bookmaker_id, Home, Draw, Away, Over2d5, Under2d5 ) " \
				f"VALUES ({match_id}, {bookmaker_id}, {home_value}, {draw_value}, {away_value}, {over_value}, {under_value})"
		print(sql)
		mycursor.execute(sql)
		mydb.commit()
					
def insert_match_odd(match_id, league, match_date, home_team_name, away_team_name):
	global NotFoundMatch_count
	print(f"    {match_id}, {match_date} : { home_team_name} - { away_team_name}")
	sql = f'SELECT * from odds where match_id = {match_id}'
	mycursor.execute(sql)
	result = mycursor.fetchall()

	if len(result):											# alreay existing match odd data in odd table
		print("    - No need to update. already saved in DB!")
	else:													# No match odd data in odds table
		league_API_id = get_leagueid_API(league)
		fixture_id = 0
		match_data_ofDay = get(f'fixtures/date/{match_date}', None, league_API_id)

		########################################## start founding fixture id in API #####################################################

		for eachMatch in match_data_ofDay:					# while looping , found fixture_id of this match in API
			Hometeam_API_id = eachMatch['localteam_id']
			team_info = team(Hometeam_API_id)
			localteamName = team_info['name']
			if (localteamName in home_team_name) | (home_team_name in localteamName):
				print("    - found match in API! ")
				fixture_id = eachMatch['id']
				break
		
		if fixture_id == 0:									# if while not found fixture id by lcoal team  then with visitor team
			for eachMatch in match_data_ofDay:				# while looping , found fixture_id of this match in API
				Vistorteam_API_id = eachMatch['visitorteam_id']
				team_info = team(Vistorteam_API_id)
				visitorteamName = team_info['name']
				if (visitorteamName in away_team_name) | (away_team_name in visitorteamName):
					print("    - found match in API! ")
					fixture_id = eachMatch['id']
					break
		
		if fixture_id == 0:		
			league_DB_id = get_leagueid_DB(league)							# still not fuounding fixture in API
			sql = f"SELECT match_id , total_home_score, half_home_score, total_away_score, half_away_score from season_match_plan where date = '{match_date}' and league_id = {league_DB_id} and status = 'END' order by time"
			mycursor.execute(sql)
			result = mycursor.fetchall()
			index = 0
			for each_result in result:						# founding match id by index and its total and half score.
				if each_result[0] == match_id:
					eachMatch_API = match_data_ofDay[index]
					ht_score_API = eachMatch_API['scores']['ht_score']
					ft_score_API = eachMatch_API['scores']['ft_score']
					ht_score_DB = str(each_result[2]) + '-' + str(each_result[4])
					ft_score_DB = str(each_result[1]) + '-' + str(each_result[3])
					if (ht_score_API ==  ht_score_DB) & (ft_score_API == ft_score_DB):
						fixture_id = eachMatch_API['id']
						break
				else:
					index += 1
			if fixture_id == 0:								# not found with index matching, founding by looping with matches of API
				for each_result in result:
					if each_result[0] == match_id:
						for eachMatch_API in match_data_ofDay:
							ht_score_API = eachMatch_API['scores']['ht_score']
							ft_score_API = eachMatch_API['scores']['ft_score']
							ht_score_DB = str(each_result[2]) + '-' + str(each_result[4])
							ft_score_DB = str(each_result[1]) + '-' + str(each_result[3])
							if (ht_score_API ==  ht_score_DB) & (ft_score_API == ft_score_DB):
								fixture_id = eachMatch_API['id']
								break
		########################################## end founding fixture id in API #####################################################

		if fixture_id == 0:									# not found in fixture in API
			print("    - Not found match in API! check the matches in API")
			NotFoundMatch_count += 1
		else:												# found match in API
			print("      fixture id is ", fixture_id)
			insert_bookmaker_odd(match_id, fixture_id, "bet365")
			insert_bookmaker_odd(match_id, fixture_id, "Betfair")
			insert_bookmaker_odd(match_id, fixture_id, "Betway")
			insert_bookmaker_odd(match_id, fixture_id, "Matchbook")
			insert_bookmaker_odd(match_id, fixture_id, "WilliamHill")
			insert_bookmaker_odd(match_id, fixture_id, "Dafabet")
			insert_bookmaker_odd(match_id, fixture_id, "Pncl")
			insert_bookmaker_odd(match_id, fixture_id, "Sbo")
			insert_bookmaker_odd(match_id, fixture_id, "Unibet")
			#time.sleep(12)
	
def insert_league_odd(league , season):
	print(f"------------- start inserting odds for {league}, {season} ----------------")

	sql = f"SELECT a.match_id, a.date, b.team_name, c.team_name FROM season_match_plan as a " \
		f"inner join team_list as b on a.home_team_id = b.team_id "	\
		f"inner join team_list as c on a.away_team_id = c.team_id "	\
		f"where a.league_id = {get_leagueid_DB(league)} and a.season_id = {switch_season(season)} and status = 'END'"
	print(sql)
	mycursor.execute(sql)
	matchArray = mycursor.fetchall()
	index = 1
	for eachMatch in matchArray:
		match_date = eachMatch[1]
		print(f"  ------------------- {league} {season} - {index} match processing start -------------------  ")
		insert_match_odd(eachMatch[0], league, match_date, eachMatch[2], eachMatch[3])
		
		print(f"  ------------------- {league} {season} - {index} match processing end ---------------------  ")
		index += 1
		#break
	print(f"------------- End inserting odds for {league}, {season} ----------------")
	
def main():
	init(sportmonks_token)
	#####################################################################################
	insert_league_odd("eng-premier-league",		"2021-2022")
	insert_league_odd("esp-primera-division",	"2021-2022")
	insert_league_odd("fra-ligue-1",			"2021-2022")
	insert_league_odd("ned-eredivisie",			"2021-2022")
	insert_league_odd("aut-bundesliga",			"2021-2022")
	insert_league_odd("bundesliga",				"2021-2022")
	insert_league_odd("ita-serie-a",			"2021-2022")
	insert_league_odd("por-primeira-liga",		"2021-2022")
	insert_league_odd("gre-super-league",		"2021-2022")
	insert_league_odd("tur-sueperlig",			"2021-2022")
	insert_league_odd("nor-eliteserien",		"2021")
	insert_league_odd("swe-allsvenskan",		"2021")
	insert_league_odd("sui-super-league",		"2021-2022")
	insert_league_odd("den-superligaen",		"2021-2022")
	insert_league_odd("ukr-premyer-liga",		"2021-2022")
	insert_league_odd("bul-parva-liga",			"2021-2022")
	insert_league_odd("cze-1-fotbalova-liga",	"2021-2022")
	insert_league_odd("cro-1-hnl",				"2021-2022")
	insert_league_odd("hun-nb-i",				"2021-2022")
	insert_league_odd("srb-super-liga",			"2021-2022")

	print(f" ------------ not found match count in API : {NotFoundMatch_count} -----------")

	#####################################################################################

if __name__ == "__main__":
	main()

