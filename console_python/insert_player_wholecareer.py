import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector

import certifi
import urllib3

http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

################################################################
# This is the sample instructions to insert the team info(team_list and season_league_team into) into database.
# python3 Get_season_league_teamname.py -season 2014-2015 -league esp-primera-division
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
	  "2020-2021" : 799,
	  "2019-2020": 12,
	  "2020" : 64, 
	}
	return switcher.get(argument, "null")
def switch_league(argument):
	switcher = {
		"esp-primera-division": 16,  		#spain
		"eng-premier-league": 6,  		 	#England
		"bundesliga": 8,   					#Germany
		"ita-serie-a" : 11, 				 #italy
		"fra-ligue-1" : 7,  				 #france
		"ned-eredivisie": 12,  				#Netherland
		"aut-bundesliga": 1,  				#Austria
		"por-primeira-liga": 14, 			 #portugal
		"gre-super-league": 9,   			#Greece
		"tur-sueperlig": 19,   				#Turkey
		"nor-eliteserien": 13,  			#Norway
		"swe-allsvenskan": 17,  			#Sweden
		"sui-super-league": 18,  			 #Swiztland
		"den-superliga": 5,     			#Denmark
		"ukr-premyer-liga": 20,    			 #Ukraine
		"bul-parva-liga" : 2,      			#bulgaria
		"cze-1-fotbalova-liga": 3,      	#Chezch
		"cro-1-hnl": 4 ,         			 #Croatia
		"hun-nb-i": 10,     				#Hungary
		"srb-super-liga": 15   				 #Serbia
	}
	return switcher.get(argument, "null")

def insert_player_wholecareer(season=None , league=None, pageNumber = None):

	################################### page url check start ###############################
	print(f"-------------------------------{season}-{pageNumber}page start-----------------------------------------")
	URL = f"https://www.worldfootball.net/players_list/{league}-{season}/nach-mannschaft/{pageNumber}/"
	page = requests.get(URL , headers={"User-Agent":"Mozilla/5.0"})
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find('table', class_="standard_tabelle")
	tr_results = results.find_all("tr")
	Now_season_id = switch_season(season)

	i = 1
	for tr in tr_results:
		all_td = tr.find_all("td")
		http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
		if(len(all_td)) :
			
			player_name = all_td[0].text
			team_name = all_td[2].text
						
			team_id = fn_Get_TeamId(team_name)
			
			player_href = all_td[0].find('a')['href']
		   
			player_id = get_player_id(player_name, player_href, team_id)

			print(f"------------{league}-{season}-{pageNumber}-{i}th player : id- {player_id} data handling start!-----------")
			sql = f'SELECT * FROM player_career WHERE player_id ="{player_id}"'
			mycursor.execute(sql)
			myresult = mycursor.fetchall()
			href_info = all_td[0].find("a")['href']
			url = "https://www.worldfootball.net"+href_info+"/2/"
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

							if "2019" in all_td[2].text:          # 2019 lower season no updated and will break 
								print("    now season is lower than 2019 , so break")
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
						
			
			print(f"------------{league}-{season}-{pageNumber}-{i}th player : id- {player_id} : name: {player_name}'s data handling End !-----------")
			i = i+1

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
			#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
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
					#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
					sql= f'UPDATE playerlist set birthday = "{player_birthday}", player_name = "{player_name}" where player_id = {now_player_id}'
					mycursor.execute(sql)
					mydb.commit()
					#print(mycursor.rowcount, "record Updated. its name or birthday updated.. not img_src")
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
				#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
				if player_number == "":
    					player_number = 0
				sql = f'UPDATE playerlist set img_src = "{player_adding_info[0]}", now_pNumber = {player_number}, now_team_id = {team_id} where player_id = {player_id}'
				mycursor.execute(sql)
				mydb.commit()
				#print(mycursor.rowcount, "record Updated. its img_src or pnumber, team id updated, have its own imag")
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
						#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
						sql= f'UPDATE playerlist set img_src = "{player_adding_info[0]}", birthday = "{now_player_birthday}", player_name = "{now_player_name}" where player_id = {now_player_id}'
						mycursor.execute(sql)
						mydb.commit()
						#print(mycursor.rowcount, "record Updated. its img, birthday or name changed.. have its own image, but not searched")
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
			
			#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
			now_pNumber = player_adding_info[5]
			if now_pNumber == "":
				now_pNumber = 0
			sql = f'UPDATE playerlist SET player_name = "{player_name}", birthday = "{player_birthday}" ,now_team_id = {team_id}, now_pNumber = {now_pNumber} WHERE player_id = {player_id}'
			mycursor.execute(sql)
			mydb.commit()
			#print(mycursor.rowcount, "record Updated. searched with its own image and his whole info updated!")
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
	player_birthday = player_adding_info[1]
	player_nation = player_adding_info[2]
	print(f"   this is new - {player_name} : {player_birthday}")
	sql = "INSERT INTO soccer.playerlist (player_name, birthday , nationality, img_src, height, weight, foot" \
		  ", position , now_pNumber, now_team_id ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s,%s, %s)"
	val = (player_name, player_birthday, player_nation, player_adding_info[0], "???", player_adding_info[3], player_adding_info[4], "??", player_adding_info[5], team_id)
	mycursor.execute(sql, val)
	mydb.commit()
	
	print("new player added - soccer ", player_name, player_birthday)
	return mycursor.lastrowid


def get_totalPageCount_onPlayerPage(season, league):
	if season:
		URL = f"https://www.worldfootball.net/players_list/{league}-{season}/nach-mannschaft/1/"
	page = requests.get(URL , headers={"User-Agent":"Mozilla/5.0"})
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find('table', class_="auswahlbox")
	if results:
		tr_result = results.find("tr")
		if len(tr_result):
			try:
				all_td = tr_result.find_all("td")
				option_container = all_td[5]
				option_result = option_container.find_all("option")
				return len(option_result)
			except:
				print("Please check the part to get correct page numbers")
				return 0
	else:
			print("Page not available yet")
			return 0


def main():
	season = "2020-2021"
	# season = "2020"
	
	league_list_1 = [
		"esp-primera-division" ,
		"eng-premier-league",  		 	#England
		"bundesliga",   					#Germany
		"ita-serie-a" , 				 #italy
		"fra-ligue-1",  				 #france
		"ned-eredivisie",  				#Netherland
		"aut-bundesliga",  				#Austria
		"por-primeira-liga", 			 #portugal
		"gre-super-league" ,   			#Greece
		"tur-sueperlig" ,   				#Turkey
		#"nor-eliteserien" ,  			#Norway
		#"swe-allsvenskan" ,  			#Sweden
		"sui-super-league" ,  			 #Swiztland
		"den-superliga" ,     			#Denmark
		"ukr-premyer-liga" ,    			 #Ukraine
		"bul-parva-liga" ,      			#bulgaria	
		"cze-1-fotbalova-liga" ,      	#Chezch
		"cro-1-hnl"  ,         			 #Croatia
		"hun-nb-i" ,     				#Hungary
		"srb-super-liga"    
		 ]
	league_list_2 = [
		"nor-eliteserien" ,  			#Norway
		"swe-allsvenskan"   			#Sweden
	]
	for league in league_list_1:
		startPageNumber = 1
		totalpageCount = get_totalPageCount_onPlayerPage(season, league ) + 1
		for x in range(startPageNumber, totalpageCount):
			insert_player_wholecareer(season,league,x)


if '__main__' == __name__:
	main()



