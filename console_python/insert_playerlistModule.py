import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3

################################################################
# This is the sample instructions to insert the player info into playerlist.
# python3 insert_playerlistModule.py -season 2014-2015 -league esp-primera-division -page 1

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
        "2020": 64, #795
        "2020-2021" : 799,
        "2021"    : 844,
        "2021-2022": 857
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
		"gre-super-league": 9,   #Greece
		"tur-sueperlig": 19,   #Turkey
		
		"nor-eliteserien": 13,  #Norway
		"swe-allsvenskan": 17,  #Sweden 

		"sui-super-league": 18,   #Swiztland 
		"den-superliga": 5,     #Denmark
		"ukr-premyer-liga": 20,     #Ukraine     
		"bul-parva-liga" : 2 ,    #bulgaria
		"cze-1-fotbalova-liga": 3,      #Chezch
		"cze-gambrinus-liga": 3,
		"cro-1-hnl": 4 ,          #Croatia
		"hun-nb-i": 10,     #Hungary
		"hun-nb1": 10,
		"hun-otp-liga":10,
		"srb-super-liga": 15    #Serbia
	}
	return switcher.get(argument, "null")

new_added_playercount = 0

def insert_playerList(season=None , league=None, page = None):
		
	pageNumber = page
	print(f"-------------------------------{season}-{league}-{pageNumber}page start-----------------------------------------")
	if season:
		URL = f"https://www.worldfootball.net/players_list/{league}-{season}/nach-mannschaft/{page}/"
	else:
		#URL = f"https://www.worldfootball.net/players_list/esp-primera-division-2014-2015/nach-mannschaft/1/"
		print("Enter the season !")
		return

	page = requests.get(URL , headers={"User-Agent":"Mozilla/5.0"})
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find('table', class_="standard_tabelle")
	tr_results = results.find_all("tr")
	
	i = 0
	for tr in tr_results:
		all_td = tr.find_all("td")
		
		if(len(all_td)) :
			player_name = all_td[0].text

			team_name = all_td[2].text
			sql =f"SELECT team_id FROM team_list WHERE team_name  = '{team_name}'"
			mycursor.execute(sql)
			myresult = mycursor.fetchall()
			if len(myresult):
				team_id = myresult[0][0]
			else :
				print("   Couldn't find the team of this player")
				return

			player_born = all_td[3].text
			player_height = all_td[4].text
			player_position = all_td[5].text
			href_info = all_td[0].find("a")['href']
			player_id = get_player_id(player_name, href_info, team_id)

			print(f"   --------{i+1}th---------------")
			i +=1

	print(f"-----------------------{season}-{league}-{pageNumber}page end-- new player added {new_added_playercount} ----------------------")
	
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
				sql = f"UPDATE playerlist set img_src = '{player_adding_info[0]}', now_pNumber = '{player_number}', now_team_id = '{team_id}' where player_id = {player_id}"
				mycursor.execute(sql)
				mydb.commit()
				print(mycursor.rowcount, "record Updated. its img_src or pnumber, team id updated, have its own imag")
				return player_id
			else:   
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
		  ", position , now_pNumber, now_team_id ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s, %s, %s)"
	val = (player_name, player_birthday, player_nation, player_adding_info[0], "???", player_adding_info[3], player_adding_info[4], "??", player_adding_info[5], team_id)
	mycursor.execute(sql, val)
	mydb.commit()
	global new_added_playercount
	new_added_playercount =new_added_playercount + 1
	print("new player added - soccer ", player_name, player_birthday)
	return mycursor.lastrowid

def Insert_Update_Players(season, league):
	startPageNumber = 1
	totalpageCount = get_totalPageCount_onPlayerPage(season, league ) + 1
	for x in range(startPageNumber, totalpageCount):
		insert_playerList(season,league,x)

	print(f" added new player count is {new_added_playercount}");
	
Insert_Update_Players("2021-2022", "aut-bundesliga")
Insert_Update_Players("2021-2022", "bul-parva-liga")
Insert_Update_Players("2021-2022", "cze-1-fotbalova-liga")
Insert_Update_Players("2021-2022", "cro-1-hnl")
#Insert_Update_Players("2021-2022", "den-superliga")
Insert_Update_Players("2021-2022", "eng-premier-league")
Insert_Update_Players("2021-2022", "fra-ligue-1")
Insert_Update_Players("2021-2022", "bundesliga")
#Insert_Update_Players("2021-2022", "gre-super-league")
Insert_Update_Players("2021-2022", "hun-nb-i")
Insert_Update_Players("2021-2022", "ita-serie-a")
Insert_Update_Players("2021-2022", "ned-eredivisie")
Insert_Update_Players("2021", "nor-eliteserien")
Insert_Update_Players("2021-2022", "por-primeira-liga")
Insert_Update_Players("2021-2022", "srb-super-liga")
Insert_Update_Players("2021-2022", "esp-primera-division")
Insert_Update_Players("2021", "swe-allsvenskan")
#Insert_Update_Players("2021-2022", "tur-superlig")
#Insert_Update_Players("2021-2022", "srb-super-liga")
Insert_Update_Players("2021-2022", "ukr-premyer-liga")
