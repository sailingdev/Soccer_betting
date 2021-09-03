import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import pandas as pd
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
  passwd="P@ssw0rd2021",
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
		"den-superligaen": 5,     			#Denmark
		"ukr-premyer-liga": 20,    			 #Ukraine
		"bul-parva-liga" : 2,      			#bulgaria
		"cze-1-fotbalova-liga": 3,      	#Chezch
		"cro-1-hnl": 4 ,         			 #Croatia
		"hun-nb-i": 10,     				#Hungary
		"srb-super-liga": 15   				 #Serbia
	}
	return switcher.get(argument, "null")

def player_total_data_to_excel(season=None , league=None, pageNumber = None):
	league_list = []
	team_list = []
	player_list = []
	score_list = []
	GS_list = []
	Goals_list = []
	GPGR_list = []

	################################### page url check start ###############################
	print(f"-------------------------------{season}-{pageNumber}page start-----------------------------------------")
	URL = f"https://www.worldfootball.net/players_list/{league}-{season}/nach-mannschaft/{pageNumber}/"
	page = requests.get(URL , headers={"User-Agent":"Mozilla/5.0"})
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find('table', class_="standard_tabelle")
	tr_results = results.find_all("tr")
	Now_season_id = switch_season(season)
	league_id = switch_league(league)
	sql = f"SELECT league_title from league where league_id = {league_id}"
	mycursor.execute(sql)
	league_result = mycursor.fetchall()
	if league_result:
			league_name = league_result[0][0]
	i = 1
	for tr in tr_results:
		all_td = tr.find_all("td")
		http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
		if(len(all_td)) :      				# loop for every player
			
			player_name = all_td[0].text
			team_name = all_td[2].text
			
			league_list.append(league_name)
			team_list.append(team_name)
			player_list.append(player_name)

			team_id = fn_Get_TeamId(team_name)
			player_href = all_td[0].find('a')['href']
			player_id = get_player_id(player_name, player_href, team_id)

			total_started = 0
			Goals = 0

			print(f"-------------{season}-{pageNumber}-{i}th player : id- {player_id} data handling start!-----------")
			sql = f'SELECT SUM(goals), SUM(started) FROM player_career WHERE player_id = {player_id}'
			mycursor.execute(sql)
			result = mycursor.fetchall()
			if result:
				total_started = result[0][1]
				
				if total_started == None:
    					total_started = 0
				print(f"total_started: {total_started}")

				Goals = result[0][0]
				if Goals != 0:
					sql = f'SELECT goals, started from player_career where player_id = {player_id} and season_id = 799'
					mycursor.execute(sql)
					now_season_result = mycursor.fetchall()
					if now_season_result:
						print(sql)
						print(now_season_result[0][1])
						total_started = total_started - now_season_result[0][1]
						Goals = Goals - now_season_result[0][0]
			GS_list.append(total_started)
			Goals_list.append(Goals)

			score = ""
			player_TGPR = 0
			
			if total_started != 0 :
				player_TGPR = Goals / total_started
				player_TGPR = round(player_TGPR, 2)				
			
			score_list.append(score)
			GPGR_list.append(player_TGPR)

			
			print(f"-------------{season}-{pageNumber}-{i}th player : id- {player_id} : name: {player_name}'s data handling End !-----------")
			i = i+1

	reallist = {
			'League' 	: 	league_list,
			'Team'		:	 team_list,
			'Player'    : 	player_list,
			'Score'		: 	 score_list,
			'GS'  		: 	GS_list,
			'Goals' 	: 	Goals_list,
			"GPGR" 		:	GPGR_list
	}
	df = pd.DataFrame(reallist,columns= ['League','Team', 'Player', "Score", "GS", "Goals", "GPGR"])
	df.to_csv(f"player_career.csv", index=False, sep='\t', encoding = 'utf-16', mode='a', header=False)
	#df.to_excel(f"{league_name}.xlsx", "Players", index=False, encoding = 'utf-16', header = False, mode = 'a')


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

def main():
	season = "2020-2021"
	league = "srb-super-liga"
	
	startPageNumber = 1
	totalpageCount = get_totalPageCount_onPlayerPage(season, league ) + 1
	for x in range(startPageNumber, totalpageCount):
		player_total_data_to_excel(season,league,x)


if '__main__' == __name__:
	main()



