import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3

http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

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
mycursor = mydb.cursor(buffered=True)

def switch_season(argument):
    switcher = {
        "2019-2020": 12,
        "2020": 64,
        "2020-2021" : 799,
        "2021"    : 844,
        "2021-2022": 857,
        "2022" : 916,
        "2022-2023" : 935
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

added_matches_count = 0
added_player_count = 0

def doing_scraping_match_plan(season=None , league=None, firstMatch = None, lastMatch = None, newInsertFlag = False):
	#global added_matches_count
	print(f"---------------------------------{season}-{league}- start-----------------------------------------")
	if season:
		URL = f"https://www.worldfootball.net/all_matches/{league}-{season}/"
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
	if lastMatch ==  None :
		lastMatch = len(tr_results)
	if firstMatch == None:        # when the fist match start is not defined
		firstMatch = 1
	
	for i in range(firstMatch-1, lastMatch):
		all_td = tr_results[i].find_all("td")
		if(len(all_td)) :
			print(f"------------------{season}-{league}- {i + 1}th Match process start --------------------")
			if all_td[0].text !="":
				match_date = convert_strDate_sqlDateFormat(all_td[0].text)
			match_total_result = all_td[5].text
			start_time = all_td[1].text
			match_status = all_td[6]
			sql = f'SELECT team_id FROM team_list WHERE team_name = "{all_td[2].text}" UNION ' \
				f'SELECT team_id FROM team_list WHERE team_name = "{all_td[4].text}"'
			print(sql)
			mycursor.execute(sql)
			myresult = mycursor.fetchall()
			home_team_id = myresult[0][0]
			away_team_id = myresult[1][0]
			
			total_home_score = "-"
			total_away_score = "-"
			half_home_score = "-"
			half_away_score = "-"

			#sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and date = '{match_date}'"
			
			if not newInsertFlag:   										# if this is option for updating the match schedule. then we must update 
				print("    There is already info for match, so we will check update status.")
				sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id}"
				mycursor.execute(sql)
				myresult = mycursor.fetchall()
				count_of_match = len(myresult)
				if count_of_match == 1:										# when the match is only one in this league, so not repeated game
					print("   there is only one game in this league !")
					status = ""                                        
					current_match_id = myresult[0][0]												
					sql = f"SELECT * from match_team_player_info where match_id = {current_match_id}"
					mycursor.execute(sql)
					myresult = mycursor.fetchall()
					if len(myresult):                                       # match info is in match_team_player_info , this is already completed game no need to update
						print("    No need to update!")
					else:                                                   # Need to update , will check the status of the match and decide the update
						if "(" in match_total_result:                       # if the match was finished
							print("   Match was finished , will update soon")
							total = match_total_result.split(" ")[0]
							half = match_total_result.split(" ")[1]
							status = "END"
							if len(total.split(":")) > 1:
								total_home_score = total.split(":")[0].strip()
								total_away_score = total.split(":")[1].strip()
								if len(half.split(":")) > 1:
									half_home_score = half.split(":")[0][1:]
									half_away_score = half.split(":")[1][:-1]
							print(f"   {match_date}, {home_team_id}, {away_team_id},{total_home_score}-{total_away_score},{half_home_score}-{half_away_score} ")
							sql = f"UPDATE season_match_plan set date = '{match_date}', time = '{start_time}', total_home_score = {total_home_score}, half_home_score = {half_home_score}, total_away_score = {total_away_score} , half_away_score = {half_away_score} , status = '{status}' where match_id = {current_match_id}"
							mycursor.execute(sql)
							mydb.commit()

							sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 , c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
							mycursor.execute(sql)
							mydb.commit()

							print("    1 completed game updated, ID: ", current_match_id, " in match_plan")

							if all_td[5].find("a"):
								href_info = all_td[5].find("a")['href']
								url = "https://www.worldfootball.net"+href_info
								insert_match_team_player_info(url , current_match_id, home_team_id, away_team_id)
						else:  
							status = ""                                             # if the match is yet planned or resch
							if "resch" in match_total_result :
								status = "resch"
								sql = f"UPDATE season_match_plan set date = '{match_date}' , time = '{start_time}', status = '{status}' where match_id = {current_match_id}"
								mycursor.execute(sql)
								mydb.commit()
								sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 ,c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
								mycursor.execute(sql)
								mydb.commit()
								print("    1 resch game updated, ID: ", current_match_id, " in match_plan")
							elif len (match_status.find_all("img")):
								status = "LIVE"
								sql = f"UPDATE season_match_plan set date = '{match_date}' , time = '{start_time}', status = '{status}' where match_id = {current_match_id}"
								mycursor.execute(sql)
								mydb.commit()
								sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 ,c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
								mycursor.execute(sql)
								mydb.commit()
								print("    1 LIVE game updated, ID: ", current_match_id, " in match_plan")
							elif "dec" in match_total_result:
								print("    this is dec game")
							elif "abor." in match_total_result:
								print("    this is aborted game")
							elif '-' not in match_total_result:				# END game but no half score
								print("   Special Match was finished , will update soon")
								total = match_total_result.split(" ")[0]
								
								status = "END"
								if len(total.split(":")) > 1:
									total_home_score = total.split(":")[0].strip()
									total_away_score = total.split(":")[1].strip()
									half_home_score = total_home_score
									half_away_score = total_away_score
								
								print(f"   {match_date}, {home_team_id}, {away_team_id},{total_home_score}-{total_away_score},{half_home_score}-{half_away_score} ")
								sql = f"UPDATE season_match_plan set date = '{match_date}', time = '{start_time}', total_home_score = {total_home_score}, half_home_score = {half_home_score}, total_away_score = {total_away_score} , half_away_score = {half_away_score} , status = '{status}' where match_id = {current_match_id}"
								
								mycursor.execute(sql)
								mydb.commit()

								sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 , c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
								mycursor.execute(sql)
								mydb.commit()

								print("    1 completed game updated, ID: ", current_match_id, " in match_plan")

								if all_td[5].find("a"):
									href_info = all_td[5].find("a")['href']
									url = "https://www.worldfootball.net" + href_info
									insert_match_team_player_info(url , current_match_id, home_team_id, away_team_id)
							else:
								sql = f"UPDATE season_match_plan set date = '{match_date}' , time = '{start_time}', status = '{status}' where match_id = {current_match_id}"
								print(sql)
								mycursor.execute(sql)
								mydb.commit()
								sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 ,c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
								mycursor.execute(sql)
								mydb.commit()
								print("    1 planned game updated, ID: ", current_match_id, " in match_plan")
				
				if count_of_match > 1 :										# if the match is repeated game,eg: Croatia, Hungary
					print("    there are many same matches in this league, so will check the fixture carefully")
					if "(" in match_total_result:							# if the match is ended game
						sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status = 'END' and date = '{match_date}'"
						# sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status = 'END'"
						mycursor.execute(sql)
						ended_Match_array = mycursor.fetchall()
						if len(ended_Match_array):							# if matching date-ended game is existing in DB
							print("    No need to update")
						else : 												# no matching ended game exist in DB find match id and update andinsert
							sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status != 'END' order by date"
							mycursor.execute(sql)
							myresult = mycursor.fetchall()
							if len(myresult):								# finding first not-ended  game in DB and update and insert them.
								print("   Match was finished , will update soon")
								current_match_id = myresult[0][0]
								total = match_total_result.split(" ")[0]
								half = match_total_result.split(" ")[1]
								status = "END"
								if len(total.split(":")) > 1:
									total_home_score = total.split(":")[0].strip()
									total_away_score = total.split(":")[1].strip()
									if len(half.split(":")) > 1:
										half_home_score = half.split(":")[0][1:]
										half_away_score = half.split(":")[1][:-1]
								print(f"   {match_date}, {home_team_id}, {away_team_id},{total_home_score}-{total_away_score},{half_home_score}-{half_away_score} ")
								sql = f"UPDATE season_match_plan set date = '{match_date}', time = '{start_time}', total_home_score = {total_home_score}, half_home_score = {half_home_score}, total_away_score = {total_away_score} , half_away_score = {half_away_score} , status = '{status}' where match_id = {current_match_id}"
								
								mycursor.execute(sql)
								mydb.commit()
								sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 ,c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
								mycursor.execute(sql)
								mydb.commit()
								print("    1 completed game updated, ID: ", current_match_id, " in match_plan")

								if all_td[5].find("a"):
									href_info = all_td[5].find("a")['href']
									url = "https://www.worldfootball.net"+href_info
									insert_match_team_player_info(url , current_match_id, home_team_id, away_team_id)

					else :													# if the match is yet planned or resch game						
						status = ""
						if "resch" in match_total_result:
							sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status = 'resch' and date ='{match_date}'"
							mycursor.execute(sql)
							result = mycursor.fetchall()
							if len(result):
								print("    No need to update")
							else:
								#sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status = ''"
								sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status != 'END'"
								mycursor.execute(sql)
								result = mycursor.fetchall()
								if len(result):
									current_match_id = result[0][0]
									status = "resch"
									sql = f"UPDATE season_match_plan set date = '{match_date}' , time = '{start_time}', status = '{status}' where match_id = {current_match_id}"
									mycursor.execute(sql)
									mydb.commit()
									sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 ,c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
									mycursor.execute(sql)
									mydb.commit()
									print("    1 resch game updated, ID: ", current_match_id, " in match_plan")
						elif len(match_status.find_all("img")):			    # Live Match
							sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status = 'LIVE' and date ='{match_date}'"
							mycursor.execute(sql)
							result = mycursor.fetchall()
							if len(result):
								print("    No need to update")
							else:
								sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status != 'END'"
								mycursor.execute(sql)
								result = mycursor.fetchall()
								if len(result):
									current_match_id = result[0][0]
									status = "LIVE"
									sql = f"UPDATE season_match_plan set date = '{match_date}' , time = '{start_time}', status = '{status}' where match_id = {current_match_id}"
									mycursor.execute(sql)
									mydb.commit()
									sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 ,c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
									mycursor.execute(sql)
									mydb.commit()
									print("    1 LIVE game updated, ID: ", current_match_id, " in match_plan")
						elif "dec" in match_total_result:
							print("    this is dec game")
						elif '-' not in match_total_result:
							print("   Special Match was finished , will update soon")
							sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status = 'END' and date = '{match_date}'"
							mycursor.execute(sql)
							ended_Match_array = mycursor.fetchall()
							if len(ended_Match_array):							# if matching date-ended game is existing in DB
								print("    No need to update")
							else : 												# no matching ended game exist in DB find match id and update andinsert
								sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status != 'END' order by date"
								mycursor.execute(sql)
								myresult = mycursor.fetchall()
								if len(myresult):								# finding first not-ended  game in DB and update and insert them.
									print("   Match was finished , will update soon")
									current_match_id = myresult[0][0]
									total = match_total_result.split(" ")[0]
									
									status = "END"
									if len(total.split(":")) > 1:
										total_home_score = total.split(":")[0].strip()
										total_away_score = total.split(":")[1].strip()
										half_home_score = total_home_score
										half_away_score = total_away_score
									print(f"   {match_date}, {home_team_id}, {away_team_id},{total_home_score}-{total_away_score},{half_home_score}-{half_away_score} ")
									sql = f"UPDATE season_match_plan set date = '{match_date}', time = '{start_time}', total_home_score = {total_home_score}, half_home_score = {half_home_score}, total_away_score = {total_away_score} , half_away_score = {half_away_score} , status = '{status}' where match_id = {current_match_id}"
									
									mycursor.execute(sql)
									mydb.commit()
									sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 ,c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
									mycursor.execute(sql)
									mydb.commit()
									print("    1 completed game updated, ID: ", current_match_id, " in match_plan")

									if all_td[5].find("a"):
										href_info = all_td[5].find("a")['href']
										url = "https://www.worldfootball.net" + href_info
										insert_match_team_player_info(url , current_match_id, home_team_id, away_team_id)
						else:
							sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status = 'LIVE' and date ='{match_date}'"
							mycursor.execute(sql)
							result = mycursor.fetchall()
							if len(result):
								print("    No need to update")
							else:
								sql = f"SELECT * from season_match_plan where season_id = {switch_season(season)} and league_id = {switch_league(league)} and home_team_id = {home_team_id} and away_team_id = {away_team_id} and status != 'END'"
								mycursor.execute(sql)
								result = mycursor.fetchall()
								if len(result):
									current_match_id = result[0][0]
									status = "LIVE"
									sql = f"UPDATE season_match_plan set date = '{match_date}', time = '{start_time}', status = '{status}' where match_id = {current_match_id}"
									mycursor.execute(sql)
									mydb.commit()
									sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 ,c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {current_match_id}"
									mycursor.execute(sql)
									mydb.commit()
									print("    1 Planned game updated, ID: ", current_match_id, " in match_plan")
			if newInsertFlag:                                               # if this is option for new inserting, then we must insert the game into DB
				print("This is new game for this season and league, so we will insert this!")
				status = ""
				if "(" in match_total_result:                           	# if the match was finished
					print("   Match was finished ")
					total = match_total_result.split(" ")[0]
					half = match_total_result.split(" ")[1]
					status = "END"
					if len(total.split(":")) > 1:
						total_home_score = total.split(":")[0].strip()
						total_away_score = total.split(":")[1].strip()
						if len(half.split(":")) > 1:
							half_home_score = half.split(":")[0][1:]
							half_away_score = half.split(":")[1][:-1]
					print(f"   {match_date}, {home_team_id}, {away_team_id},{total_home_score}-{total_away_score},{half_home_score}-{half_away_score} ")
					sql = "INSERT INTO season_match_plan (season_id, league_id , date, time, home_team_id , away_team_id , " \
						"total_home_score, half_home_score, total_away_score, half_away_score, status)" \
						"VALUES (%s, %s , %s, %s, %s, %s, %s, %s, %s, %s , %s)"
					val = (switch_season(season), switch_league(league),match_date, start_time,home_team_id, away_team_id,total_home_score , \
						half_home_score,total_away_score , half_away_score, status)
					mycursor.execute(sql, val)
					last_match_id = mycursor.lastrowid
					mydb.commit()
					print("    1 completed game inserted, ID: ", mycursor.lastrowid, " in match_plan")
					
					sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 , c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {last_match_id}"
					mycursor.execute(sql)
					mydb.commit()

					if all_td[5].find("a"):
						href_info = all_td[5].find("a")['href']
						url = "https://www.worldfootball.net" + href_info
						insert_match_team_player_info(url , last_match_id, home_team_id, away_team_id)

				else:                                                   # if the match is planned
					print(f"   {match_date}, {home_team_id}, {away_team_id},{total_home_score}-{total_away_score},{half_home_score}-{half_away_score} ")
					print("    Match is planned , not finished yet.")

					if len (match_status.find_all("img")):
						status = "LIVE"

					if "resch" in match_total_result :
						status = "resch"

					sql = "INSERT INTO season_match_plan (season_id, league_id , date, time,home_team_id , away_team_id , " \
						"total_home_score, half_home_score, total_away_score, half_away_score, status)" \
						"VALUES (%s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
					val = (switch_season(season), switch_league(league),match_date, start_time, home_team_id, away_team_id,total_home_score , \
						half_home_score,total_away_score , half_away_score, status)
					mycursor.execute(sql, val)
					mydb.commit()
					print("    1 planned game inserted, ID: ", mycursor.lastrowid, " in match_plan")
					last_match_id = mycursor.lastrowid
					sql = f"UPDATE season_match_plan AS a SET WN = WEEK(a.date - INTERVAL 1 DAY)+1 , c_WN = (SELECT WEEK  FROM date_week_map AS b WHERE a.date = b.date ) where match_id = {last_match_id}"
					mycursor.execute(sql)
					mydb.commit()

			print(f"------------------{season}-{league}- {i + 1}th Match process end --------------------")
			i += 1
			#return

	print(f"---------------------------------{season}-{league} end -----------------------------------------")

def convert_strDate_sqlDateFormat(str_date):
	#  23/10/2020  - > 2020-10-23
	list = str_date.split('/');
	date = list[2] + '-' + list[1] + '-' + list[0];
	return date;

def insert_match_team_player_info(url , last_match_id, home_team_id, away_team_id):
	global added_matches_count
	http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
	
	page = requests.get(url)
	
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find_all('table', class_="standard_tabelle")
	goal_player_id_list=[]
	assist_player_id_list = []

	print("    ------------")
	if len(results) <7:
		goal_assist_container = results[1]
		home_team_container = results[2]
		away_team_container = results[3]
	else:
		goal_assist_container = results[1]
		home_team_container = results[3]
		away_team_container = results[4]

	tr_results = goal_assist_container.find_all("tr")
	for every_tr in tr_results:
		#if video exist
		tdlist = every_tr.find_all('td')
		if len(tdlist) > 1 :
			if tdlist[1].attrs.get('style'):      # td has its style? padding-left: If having, will be away team
				team_id_for_player = away_team_id
			else:
				team_id_for_player = home_team_id
			a_results = tdlist[1].find_all("a")   # player_name container
			if a_results:
				goal_player_id_list.append(get_player_id(a_results[0]['title'] , a_results[0]['href'], team_id_for_player))
				if len(a_results) > 1:
					assist_player_id_list.append(get_player_id(a_results[1]['title'], a_results[1]['href'] ,team_id_for_player))
	# print("    goal list - ", goal_player_id_list, "assist list - ", assist_player_id_list)

	a_results = home_team_container.find_all("a")
	if len(a_results) > 10:
		for i in range(0,11):
			id = get_player_id(a_results[i]['title'], a_results[i]['href'], home_team_id)
			update_insert_PlayerCareer(id, a_results[i]['href'])
			goals = goal_player_id_list.count(id)
			assists = assist_player_id_list.count(id)

			sql = "INSERT INTO match_team_player_info ( match_id, team_id , player_id , " \
				"goals, assists)" \
				"VALUES (%s, %s , %s, %s, %s)"
			val = (last_match_id, home_team_id, id, goals, assists)
			mycursor.execute(sql, val)
			mydb.commit()
			print(" inserted home team player - ",i+1 ,a_results[i]['title'] , id, goals, assists)
		################################ End insert home team info ##########################################
		a_results = away_team_container.find_all("a")
		for i in range(0,11):
			id = get_player_id(a_results[i]['title'], a_results[i]['href'], away_team_id)
			update_insert_PlayerCareer(id, a_results[i]['href'])
			goals = goal_player_id_list.count(id)
			assists = assist_player_id_list.count(id)

			sql = "INSERT INTO match_team_player_info ( match_id, team_id , player_id , " \
				"goals, assists)" \
				"VALUES (%s, %s , %s, %s, %s)"
			val = (last_match_id, away_team_id, id, goals, assists)
			mycursor.execute(sql, val)
			mydb.commit()
			print(" inserted away team player - ", i+1 , a_results[i]['title'] , id, goals, assists)

		added_matches_count += 1
		################################ End insert away team info ##########################################

def get_player_id(player_name, player_href, team_id):
	url = "https://www.worldfootball.net" + player_href	
	player_adding_info = get_more_player_info(url , player_name)
	player_birthday = player_adding_info[1]
	
	player_number  = player_adding_info[5]
	img_src_flag = 0

	if 'gross/0.' in player_adding_info[0]:      		# No image, so empty man
		sql = f'SELECT * FROM playerlist WHERE player_name like "%{player_name}" and birthday = "{player_birthday}"'
		mycursor.execute(sql)
		player_existing_result = mycursor.fetchall()
		if len(player_existing_result):                 # match found with player name and birthday
			print(f"   There is already in playerlist - {player_name} : {player_birthday}")
			player_id = player_existing_result[0][0]
			return player_id
		else:      										# not found with name and birthday , so will find with player number and team id
			if player_number == "":
				player_number = 0
			sql = f"SELECT player_id, player_name, birthday from playerlist where now_pNumber = {player_number} and now_team_id = {team_id}"
			mycursor.execute(sql)
			player_existing_result = mycursor.fetchall()
			if len(player_existing_result):            # name or birthday match..so will check availabe 
				now_player_name = player_existing_result[0][1]
				now_player_birthday = player_existing_result[0][2]
				now_player_id = player_existing_result[0][0]
				
				if (player_birthday ==  now_player_birthday) | (player_name == now_player_name):
					print(f"   There is already in playerlist - {player_name} : {player_birthday}")
					sql= f'UPDATE playerlist set birthday = "{player_birthday}", player_name = "{player_name}" where player_id = {now_player_id}'
					mycursor.execute(sql)
					mydb.commit()
					print(mycursor.rowcount, "record Updated. its name or birthday updated.. not img_src")
					return now_player_id
				else:
					player_id = add_extra_player(player_name, player_adding_info, team_id)
					return player_id
			else:
					player_id = add_extra_player(player_name, player_adding_info, team_id)
					return player_id

	else:												# has its own image
		sql = f"SELECT * from playerlist where img_src = '{player_adding_info[0]}'"
		mycursor.execute(sql)
		player_existing_result = mycursor.fetchall()
		if len(player_existing_result) == 0:
			sql = f'SELECT * FROM playerlist WHERE player_name  like "%{player_name}" and birthday = "{player_birthday}"'
			mycursor.execute(sql)
			player_existing_result = mycursor.fetchall()
			if len(player_existing_result):   # its img_src, pnumber, team_id update
				player_id = player_existing_result[0][0]
				print(f"   There is already in playerlist - {player_name} : {player_birthday}")
				if player_number == "":
						player_number = 0
				sql = f'UPDATE playerlist set img_src = "{player_adding_info[0]}", now_pNumber = {player_number}, now_team_id = {team_id} where player_id = {player_id}'
				mycursor.execute(sql)
				mydb.commit()
				print(mycursor.rowcount, "record Updated. its img_src or pnumber, team id updated, have its own imag")
				return player_id
			else:   
				if player_number == "":
					player_number = 0				
				sql = f"SELECT player_id, player_name, birthday from playerlist where now_pNumber = {player_number} and now_team_id = {team_id}"
				#print(sql)
				mycursor.execute(sql)
				player_existing_result = mycursor.fetchall()
				if len(player_existing_result):            # name or birthday match..so will check availabe 
					now_player_name = player_existing_result[0][1]
					now_player_birthday = player_existing_result[0][2]
					now_player_id = player_existing_result[0][0]
					if (player_birthday ==  now_player_birthday) | (player_name == now_player_name):
						print(f"   There is already in playerlist - {player_name} : {player_birthday}")
						sql= f'UPDATE playerlist set img_src = "{player_adding_info[0]}", birthday = "{now_player_birthday}", player_name = "{now_player_name}" where player_id = {now_player_id}'
						mycursor.execute(sql)
						mydb.commit()
						print(mycursor.rowcount, "record Updated. its img, birthday or name changed.. have its own image, but not searched")
						return now_player_id
					else:
						player_id = add_extra_player(player_name, player_adding_info, team_id)
						return player_id
				else:
						player_id = add_extra_player(player_name, player_adding_info, team_id)
						return player_id
		else:
			img_src_flag = 1
			player_id = player_existing_result[0][0]
			player_birthday = player_adding_info[1]
			print(f"   There is already in playerlist - {player_name} : {player_birthday}")
			now_pNumber = player_adding_info[5]
			if now_pNumber == "":
				now_pNumber = 0
			sql = f'UPDATE playerlist SET player_name = "{player_name}", birthday = "{player_birthday}" ,now_team_id = {team_id}, now_pNumber = {now_pNumber} WHERE player_id = {player_id}'
			mycursor.execute(sql)
			mydb.commit()
			print(mycursor.rowcount, "record Updated. searched with its own image and his whole info updated!")
			return player_id
				
def get_more_player_info(url , player_name):
	
	page = requests.get(url,headers={"User-Agent":"Mozilla/5.0"})
	
	soup = BeautifulSoup(page.content, "html.parser")

	#################### person information part ##########################
	results = soup.find('div', itemtype="http://schema.org/Person")
	
	player_img = results.find("img")["src"]

	player_nation_container = results.find(string = "Nationality:").findNext("td")
	player_nation_container = player_nation_container.find_all("img")
	count = 0
	player_nation=""
	for i in player_nation_container:
		if count > 0:
			player_nation +=","
		player_nation += i['alt']
		count += 1

	player_weight = "???"
	if results.find(string="Weight:"):
		player_weight = results.find(string="Weight:").findNext("td").text.strip()
	player_foot = "???"
	if results.find(string="Foot:"):
		player_foot = results.find(string = "Foot:").findNext("td").text.strip()
	player_birthday = "???"
	if results.find(string="Born:"):
		player_birthday = results.find(string="Born:").findNext("td").text.strip()
		player_birthday = player_birthday.replace('.', '/')
		player_birthday = player_birthday.split(" ")[0]
		if player_birthday  == "":
			player_birthday = "???"
	################## get player's number of team ##########################
	player_number = ""
	results = soup.find('tr', class_ = "dunkel")
	if results:
		td_results = results.find_all("td")
		if td_results:
				player_number = td_results[2].text.strip()
	else :
		results = soup.find('tr', class_ = "hell")
		if results:
			td_results = results.find_all("td")
			if td_results:
				player_number = td_results[2].text.strip()
	if player_number != "":
			player_number = player_number.split('#')[1]

	return_list = [player_img, player_birthday , player_nation, player_weight, player_foot, player_number]

	return return_list

def add_extra_player(player_name, player_adding_info, team_id):
	global added_player_count
	player_birthday = player_adding_info[1]
	player_nation = player_adding_info[2]
	print(f"   this is new - {player_name} : {player_birthday}")
	sql = "INSERT INTO soccer.playerlist (player_name, birthday , nationality, img_src, height, weight, foot" \
		", position , now_pNumber, now_team_id ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s,%s, %s)"
	val = (player_name, player_birthday, player_nation, player_adding_info[0], "???", player_adding_info[3], player_adding_info[4], "??", player_adding_info[5], team_id)
	mycursor.execute(sql, val)
	mydb.commit()
	
	print("new player added - soccer ", player_name, player_birthday)
	added_player_count += 1
	return mycursor.lastrowid

def update_insert_PlayerCareer(player_id, player_href):
	sql = f'SELECT * FROM player_career WHERE player_id ="{player_id}"'
	mycursor.execute(sql)
	myresult = mycursor.fetchall()
	href_info = player_href
	url = "https://www.worldfootball.net" + href_info + "/2/"
	
	page = requests.get(url , headers={"User-Agent":"Mozilla/5.0"})
	
	soup = BeautifulSoup(page.content, "html.parser")
		################################### page url check end ###############################
	if len(myresult):           # player data is already existed in career table , so have to update or insert
		print(f"{player_id}th data is already added! so will update ")
		extra_results = soup.find('table', class_="standard_tabelle")
		extra_tr_results = extra_results.find_all("tr")
		count = 1
		tr_index = 1
		for tr in extra_tr_results:
			all_td = tr.find_all("td")
			
			if(len(all_td)) :
				if (tr_index > 1) and len(all_td) < 2: # no carrer
					break
				else: 
					flag = all_td[0].find('img')['src']
					league_id = fn_Get_LeagueId(all_td[1].text, all_td[1].find('a')['href'])
					season_id = fn_Get_SeasonId(all_td[2].text)
					
					if "2018" in all_td[2].text:          # 2018 lower season no updated and will break 
						print("    now season is lower than 2018 , so break")
						break
					
					team_id = fn_Get_TeamId(all_td[3].text)
					sql = f'SELECT * from player_career where player_id = {player_id} and league_id = {league_id} and season_id = {season_id} and team_id = {team_id}'
					mycursor.execute(sql)
					career_result = mycursor.fetchall()
					if(len(career_result)):       # if the season and league is existing , will update them 
						sql = f"update player_career set matches = { fn_filter_value(all_td[4].text)} , \
								goals = { fn_filter_value(all_td[5].text)}, \
								started = { fn_filter_value(all_td[6].text)}, \
								s_in = { fn_filter_value(all_td[7].text)},  \
								s_out = { fn_filter_value(all_td[8].text)}, \
								yellow = { fn_filter_value(all_td[9].text)}, \
								s_yellow = { fn_filter_value(all_td[10].text)}, \
								red = { fn_filter_value(all_td[11].text)}  \
								where player_id = {player_id} and league_id = {league_id} and season_id = {season_id} and team_id = {team_id}"
						mycursor.execute(sql)
						mydb.commit()
						print(f"   Updated new row-{count}")
						count = count + 1
					else:						# if the data not existing in DB, will inset this
						sql = f"INSERT INTO player_career (player_id, flag, league_id, season_id, team_id, matches, goals, started,s_in, s_out, yellow, s_yellow, red ) \
							VALUES ({player_id},'{flag}', {league_id} ,{ season_id}, {team_id}, \
								{  fn_filter_value(all_td[4].text)}, \
								{ fn_filter_value(all_td[5].text)}, \
								{ fn_filter_value(all_td[6].text)}, \
								{ fn_filter_value(all_td[7].text)}, \
								{ fn_filter_value(all_td[8].text)}, \
								{ fn_filter_value(all_td[9].text)}, \
								{ fn_filter_value(all_td[10].text)}, \
								{ fn_filter_value(all_td[11].text)})"
					
						mycursor.execute(sql)
						mydb.commit()
						print(f"   added extra new row-{count}")
						count = count + 1
			tr_index = tr_index +1

	else:                       # NO player data  , so have to insert
		################################### career check start ###############################
		extra_results = soup.find('table', class_="standard_tabelle")
		extra_tr_results = extra_results.find_all("tr")
		count = 1
		tr_index = 1
		for tr in extra_tr_results:
			all_td = tr.find_all("td")
			http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
			if(len(all_td)) :
				if (tr_index > 1) and len(all_td) < 2: # no carrer
					break
				else: 
					flag = all_td[0].find('img')['src']
					league_id = fn_Get_LeagueId(all_td[1].text, all_td[1].find('a')['href'])
					season_id = fn_Get_SeasonId(all_td[2].text)
					team_id = fn_Get_TeamId(all_td[3].text)
					
					sql = f"INSERT INTO player_career (player_id, flag, league_id, season_id, team_id, matches, goals, started,s_in, s_out, yellow, s_yellow, red ) \
							VALUES ({player_id},'{flag}', {league_id} ,{ season_id}, {team_id}, \
						{  fn_filter_value(all_td[4].text)}, \
						{ fn_filter_value(all_td[5].text)}, \
						{ fn_filter_value(all_td[6].text)}, \
						{ fn_filter_value(all_td[7].text)}, \
						{ fn_filter_value(all_td[8].text)}, \
						{ fn_filter_value(all_td[9].text)}, \
						{ fn_filter_value(all_td[10].text)}, \
						{ fn_filter_value(all_td[11].text)})"
					
					mycursor.execute(sql)
					mydb.commit()
					print(f"   added extra new row-{count}")
					count = count + 1
			tr_index = tr_index +1
						
	print("    Player's career updated as new data!")

def fn_filter_value(str):
	if '?' in str:
		return 0
	else: 
		return int(str)
					
def fn_Get_LeagueId(league_dname, league_extra_info):
	realLeague = league_extra_info.split("/")[2]
	if league_dname == "Bundesliga":
		if realLeague == "bundesliga":
			return 8
		if realLeague == "aut-bundesliga":
			return 1
		else:
			sql = f'SELECT league_id FROM league where league_title = "{realLeague}"'  # if league dname's duplicate exists, then search league_title
			mycursor.execute(sql)
			myresult = mycursor.fetchone()
			if myresult:
				return myresult[0]
			else:
				sql = f'INSERT INTO league (league_dname , league_title ) VALUES ("{league_dname}, {realLeague}")'
				mycursor.execute(sql)
				mydb.commit()
				print("   -------added new league-"+league_dname + ":"+ realLeague)
				return mycursor.lastrowid
	if league_dname == "Super League":
		# realLeague = league_extra_info.split("/")[2]
		if realLeague == "gre-super-league":
			return 9
		if realLeague == "sui-super-league":
			return 18
		else:
			sql = f'SELECT league_id FROM league where league_title = "{realLeague}"'  # if league dname's duplicate exists, then search league_title
			mycursor.execute(sql)
			myresult = mycursor.fetchone()
			if myresult:
				return myresult[0]
			else:
				sql = f'INSERT INTO league (league_dname , league_title ) VALUES ("{league_dname}, {realLeague}")'
				mycursor.execute(sql)
				mydb.commit()
				print("   -------added new league-"+league_dname + ":"+ realLeague)
				return mycursor.lastrowid
	
	sql = f'SELECT league_id FROM league where league_dname = "{league_dname}"'
	mycursor.execute(sql)
	myresult = mycursor.fetchone()
	if myresult:
		return myresult[0]
	else:
		sql = f'INSERT INTO league (league_dname , league_title ) VALUES ("{league_dname}", "{realLeague}")'
		mycursor.execute(sql)
		mydb.commit()
		print("   -------added new league-"+league_dname + ":"+ realLeague)
		return mycursor.lastrowid

def fn_Get_SeasonId(season_title):
	sql = f'SELECT season_id FROM season where season_title = "{season_title}"'
	mycursor.execute(sql)
	myresult = mycursor.fetchone()
	if myresult:
		return myresult[0]
	else:
		sql = f'INSERT INTO season (season_title ) VALUES ("{season_title}")'
		mycursor.execute(sql)
		mydb.commit()
		print("   ----------added new season-" + season_title)
		return mycursor.lastrowid

def fn_Get_TeamId(team_name):
	sql = f'SELECT team_id FROM team_list where team_name = "{team_name}"'
	mycursor.execute(sql)
	myresult = mycursor.fetchone()
	if myresult:
		return myresult[0]
	else:
		sql = f'INSERT INTO team_list (team_name ) VALUES ("{team_name}")'
		
		mycursor.execute(sql)
		mydb.commit()
		print("   ---------added new team-" + team_name)
		return mycursor.lastrowid

#################################################################################################

def main():
	# doing_scraping_match_plan("2021-2022", "aut-bundesliga")
	# doing_scraping_match_plan("2021-2022", "bul-parva-liga")
	# doing_scraping_match_plan("2021-2022", "cze-1-fotbalova-liga")
	# doing_scraping_match_plan("2021-2022", "den-superligaen")
	# doing_scraping_match_plan("2021-2022", "eng-premier-league")
	# doing_scraping_match_plan("2021-2022", "fra-ligue-1")
	# doing_scraping_match_plan("2021-2022", "bundesliga")
	# doing_scraping_match_plan("2021-2022", "gre-super-league")
	# doing_scraping_match_plan("2021-2022", "ita-serie-a")
	# doing_scraping_match_plan("2021-2022", "ned-eredivisie")
	# doing_scraping_match_plan("2021-2022", "por-primeira-liga")
	# doing_scraping_match_plan("2021-2022", "srb-super-liga")
	# doing_scraping_match_plan("2021-2022", "esp-primera-division")
	# doing_scraping_match_plan("2021-2022", "sui-super-league")
	# doing_scraping_match_plan("2021-2022", "tur-sueperlig")
	# doing_scraping_match_plan("2021-2022", "ukr-premyer-liga")
	doing_scraping_match_plan("2022", "swe-allsvenskan")
	doing_scraping_match_plan("2022", "nor-eliteserien")
	# doing_scraping_match_plan("2021-2022", "cro-1-hnl")
	# doing_scraping_match_plan("2021-2022", "hun-nb-i")
 
	# doing_scraping_match_plan("", "", firstMatch = None, lastMatch = None, newInsertFlag = True)
	
	print("")
	print(f"-------- total added matches number is {added_matches_count} -------------")
	print(f"-------- total added players number is {added_player_count} -------------")

if __name__ == "__main__":
    main()