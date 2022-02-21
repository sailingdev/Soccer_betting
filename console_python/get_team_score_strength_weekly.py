import requests
from bs4 import BeautifulSoup
import argparse
import sys
import json
import mysql.connector

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
        "2021-2022": 857
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

def get_Real_LeagueUrl(argument):
	switcher = {
		16			:		 "esp-primera-division",  	#spain
		6		 	:		 "eng-premier-league",   	#England
		8			: 		 "bundesliga",  			#Germany
		11 			: 		 "ita-serie-a",  			#italy
		7 			: 		 "fra-ligue-1",   			#france
		12 			: 		 "ned-eredivisie", 			#Netherland
		1			: 		 "aut-bundesliga",		    #Austria
		14			: 		 "por-primeira-liga",  		#portugal
		9 			: 		 "gre-super-league",   		#Greece
		19 			: 		 "tur-sueperlig",  			#Turkey
		13			: 		 "nor-eliteserien",  		#Norway
		17 			: 		 "swe-allsvenskan",  		#Sweden
		18 			: 		 "sui-super-league",   		#Swiztland
		5 			: 		 "den-superligaen",    		 #Denmark
		20 			: 		 "ukr-premyer-liga",     	#Ukraine
		2			: 		 "bul-parva-liga",     		#bulgaria
		3			: 		 "cze-1-fotbalova-liga",     #Chezch
		4 			: 		 "cro-1-hnl" ,         		 #Croatia
		10		 	: 		 "hun-nb-i",     			#Hungary
		15 			: 		 "srb-super-liga"   		#Serbia
	}
	return switcher.get(argument, "null")

def doing_team_news(season, league_id, date, home_team_name_id, away_team_name_id):
	print(season, league_id, date, time, home_team_name_id, away_team_name_id)

	league_url = get_Real_LeagueUrl(int(league_id))
	print(league_url)

	if league_url:
		URL = f"https://www.worldfootball.net/all_matches/{league_url}-{season}/"
	print(URL)
	
	page = requests.get(URL)
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find('table', class_="standard_tabelle")

	tr_results = results.find_all("tr")    # whole match result of this season and league
	
	for ev_tr in tr_results:
		if ev_tr.find_all("td"):
			pass
		else:
			tr_results.remove(ev_tr)
	
	match_date=""
	
	for i in range(0, len(tr_results)):
	
		all_td = tr_results[i].find_all("td")
		if all_td[0].text !="":
			match_date = convert_strDate_sqlDateFormat(all_td[0].text)
		
		start_time = all_td[1].text
		
		sql = f'SELECT team_id FROM team_list WHERE team_name = "{all_td[2].text}" UNION ' \
				  f'SELECT team_id FROM team_list WHERE team_name = "{all_td[4].text}"'
		mycursor.execute(sql)
		myresult = mycursor.fetchall()
		cur_home_team_id = str(myresult[0][0])
		cur_away_team_id = str(myresult[1][0])

		if (match_date == date) & (home_team_name_id ==  cur_home_team_id) & (away_team_name_id ==  cur_away_team_id):
			a_results =  all_td[5].find_all('a')
			if len(a_results):
				url = "https://www.worldfootball.net" + a_results[0]['href']
				return get_team_score_strength(url, home_team_name_id, away_team_name_id, switch_season(season))
			
		i += 1
		
def get_team_score_strength(url, home_team_id, away_team_id, season_id):
	page = requests.get(url)
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find_all('table', class_="standard_tabelle")

	hometeam_data = {}
	awayteam_data = {}

	if len(results) < 7:
		home_team_info = results[2]
		away_team_info = results[3]
	else:
		home_team_info = results[3]
		away_team_info = results[4]

	hometeam_data = get_data_from_hometeam(home_team_info, home_team_id, season_id)
	awayteam_data = get_data_from_awayteam(away_team_info, away_team_id, season_id)

	return {**hometeam_data, **awayteam_data}

def convert_strDate_sqlDateFormat(str_date):
    # 23/10/2020  -> 2020-10-23
	list = str_date.split('/');
	date = list[2] + '-' + list[1] + '-' + list[0];
	return date;

def get_data_from_hometeam(team_info, team_id, season_id):
	tr_results  = team_info.find_all("tr")
	team_score = 0
	if tr_results:
		for i in range(0, 11):
			td_results = tr_results[i].find_all("td")
			player_number = td_results[0].text.strip()
			name_container = td_results[1].find('a')
			player_name = name_container.text.strip()
			player_href = td_results[1].find('a')['href']
			player_id = get_player_id(player_name, player_href, team_id)
			player_score = get_player_score_season(player_id,season_id)
			team_score  = team_score + player_score
	
	return_val = {"home_team_score":team_score, "home_team_strength":get_strength(team_score)}
	
	return return_val

def get_data_from_awayteam(team_info, team_id, season_id):
	tr_results  = team_info.find_all("tr")
	team_score = 0
	if tr_results:
		for i in range(0, 11):
			td_results = tr_results[i].find_all("td")
			player_number = td_results[0].text.strip()
			name_container = td_results[1].find('a')
			player_name = name_container.text.strip()
			player_href = td_results[1].find('a')['href']
			player_id = get_player_id(player_name, player_href, team_id)
			player_score = get_player_score_season(player_id,season_id)
			team_score  = team_score + player_score	
	
	return_val = {"away_team_score":team_score, "away_team_strength":get_strength(team_score)}
	
	return return_val

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
	
	page = requests.get(url, headers={"User-Agent":"Mozilla/5.0"})
	soup = BeautifulSoup(page.content, "html.parser")

	#################### person information part ##########################
	results = soup.find('div', itemtype="http://schema.org/Person")
	#print("results", results)
	#player_img = results.find("img", alt = player_name)["src"]
	player_nation_container = results.find(string = "Nationality:").findNext("td")
	player_nation_container = player_nation_container.find_all("img")
	count = 0
	player_nation=""
	for i in player_nation_container:
		if count > 0:
			player_nation +=","
		player_nation += i['alt']
		count += 1

	player_birthday = ""
	if results.find(string="Born:"):
		player_birthday = results.find(string="Born:").findNext("td").text.strip()
		player_birthday = player_birthday.replace('.', '/')
	
	

	################## get player's number of team ##########################
	
	return_list = [player_birthday, player_nation]

	return return_list

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
				print(sql)
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
	print(f"   this is new - {player_name} : {player_birthday}"

	sql = f'INSERT INTO soccer.playerlist (player_name, birthday , nationality, img_src, height, weight, 
			foot, position , now_pNumber, now_team_id ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s,%s, %s)'
	
	val = (player_name, player_birthday, player_nation, player_adding_info[0], "???", player_adding_info[3], player_adding_info[4], "??", player_adding_info[5], team_id)
	mycursor.execute(sql, val)
	mydb.commit()
	print("new player added - soccer ", player_name, player_birthday)
	return mycursor.lastrowid

def main():
	if len(sys.argv) > 5:
    	
		text = doing_team_news(sys.argv[1], sys.argv[2], convert_strDate_sqlDateFormat(sys.argv[3]), sys.argv[4], sys.argv[5]);
		text = json.dumps(text)
		text = text.encode('utf8');
		print(text);
	else:
		print("Need full arguments!")

	#text = doing_team_news('2020-2021', '8' ,'17/10/2020','61', '65');
	#text = json.dumps(text)
	#text = text.encode('utf8');
	#print(text);


if __name__ == "__main__":
	main()

