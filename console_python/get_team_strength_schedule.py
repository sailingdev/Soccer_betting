import sched
import time
import datetime as dt
import threading
import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import mysql.connector.pooling

dbconfig = {
    "host":"localhost",
    "port":"3306",
    "user":"root",
    "password":"",
    "database":"soccer",
	"auth_plugin" : 'mysql_native_password'
}

class MySQLPool(object):
	def __init__(self, host="127.0.0.1", port="3306", user="root",
			password="", database="soccer", pool_name="mypool", pool_size=3 , auth_plugin = "mysql_native_password"):
		res = {}
		self._host = host
		self._port = port
		self._user = user
		self._password = password
		self._database = database
		self._auth_plugin = 'mysql_native_password'

		res["host"] = self._host
		res["port"] = self._port
		res["user"] = self._user
		res["password"] = self._password
		res["database"] = self._database
		res['auth_plugin'] = 'mysql_native_password'
		self.dbconfig = res
		self.pool = self.create_pool(pool_name=pool_name, pool_size=pool_size)

	def create_pool(self, pool_name="mypool", pool_size=5):
		pool = mysql.connector.pooling.MySQLConnectionPool(
			pool_name=pool_name,
			pool_size=pool_size,
			pool_reset_session=True,
			**self.dbconfig)
		return pool

	def close(self, conn, cursor):
		"""
		A method used to close connection of mysql.
		:param conn: 
		:param cursor: 
		:return: 
		"""
		cursor.close()
		conn.close()

	def execute(self, sql, args=None, commit=False):
		"""
		Execute a sql, it could be with args and with out args. The usage is 
		similar with execute() function in module pymysql.
		:param sql: sql clause
		:param args: args need by sql clause
		:param commit: whether to commit
		:return: if commit, return None, else, return result
		"""
		# get connection form connection pool instead of create one.
		conn = self.pool.get_connection()
		cursor = conn.cursor()
		if args:
			cursor.execute(sql, args)
		else:
			cursor.execute(sql)
		if commit is True:
			conn.commit()
			self.close(conn, cursor)
			return "Success"
		else:
			res = cursor.fetchall()
			self.close(conn, cursor)
			return res

	def executemany(self, sql, args, commit=False):
		"""
		Execute with many args. Similar with executemany() function in pymysql.
		args should be a sequence.
		:param sql: sql clause
		:param args: args
		:param commit: commit or not.
		:return: if commit, return None, else, return result
		"""
		# get connection form connection pool instead of create one.
		conn = self.pool.get_connection()
		cursor = conn.cursor()
		cursor.executemany(sql, args)
		if commit is True:
			conn.commit()
			self.close(conn, cursor)
			return None
		else:
			res = cursor.fetchall()
			self.close(conn, cursor)
			return res

def switch_season(argument):
	switcher = {
		"2020": 64,
		"2020-2021" : 799,
		"2021" : 844 ,
		'2021-2022' : 857,
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
		"gre-superleague": 9,   #Greece
		"gre-super-league": 9,   #Greece
		"tur-sueperlig": 19,   #Turkey
		"nor-eliteserien": 13,  #Norway
		"nor-tippeligaen":13,
		"swe-allsvenskan": 17,  #Sweden
		"sui-super-league": 18,   #Swiztland
		"den-superliga": 5,     #Denmark
		"den-sas-ligaen":5,
		"ukr-premyer-liga": 20,     #Ukraine       
		"bul-parva-liga" : 2 , #bulgaria
		"cze-1-fotbalova-liga": 3,      #Chezch
		"cze-gambrinus-liga": 3,
		"cro-1-hnl": 4 ,          #Croatia
		"hun-nb-i": 10,     #Hungary
		"hun-nb1": 10,
		"hun-otp-liga":10,
		"srb-super-liga": 15    #Serbia
	}
	return switcher.get(argument, "null")

scheduler = sched.scheduler(time.time, time.sleep);

count = 0
index = 0

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
		5 			: 		 "den-superliga",    		 #Denmark
		20 			: 		 "ukr-premyer-liga",     	#Ukraine
		2			: 		 "bul-parva-liga",     		#bulgaria
		3			: 		 "cze-1-fotbalova-liga",     #Chezch
		4 			: 		 "cro-1-hnl" ,         		 #Croatia
		10		 	: 		 "hun-nb-i",     			#Hungary
		15 			: 		 "srb-super-liga"   		#Serbia
	}
	return switcher.get(argument, "null")

def doing_team_news(season, league_id, date, time, home_team_name_id, away_team_name_id):
	print("       ", season, league_id, date, time, home_team_name_id, away_team_name_id)
	mysql_pool = MySQLPool(**dbconfig)
	#mycursor = mydb.cursor()
	league_url = get_Real_LeagueUrl(int(league_id))
	#print(league_url)
	if league_url:
		URL = f"https://www.worldfootball.net/all_matches/{league_url}-{season}/"
	
	
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
			match_date = convert_strDate_sqlDateFormat( all_td[0].text)
		
		start_time = all_td[1].text
		
		sql = f'SELECT team_id FROM team_list WHERE team_name = "{all_td[2].text}" UNION ' \
				  f'SELECT team_id FROM team_list WHERE team_name = "{all_td[4].text}"'
		
		myresult = mysql_pool.execute(sql)
		#myresult = mycursor.fetchall()
		cur_home_team_id = myresult[0][0]
		cur_away_team_id = myresult[1][0]
		#print(f'   {type(match_date)} = {type(date)} , {type(start_time)} = {type(time)} , {type(home_team_name_id)} = {type(cur_home_team_id)}')
		if (match_date == date) & (home_team_name_id ==  cur_home_team_id) & (away_team_name_id ==  cur_away_team_id):
			a_results =  all_td[5].find_all('a')
			
			if len(a_results):
				url = "https://www.worldfootball.net" + a_results[0]['href']
				#print(url)
				return get_team_score_strength(url, home_team_name_id, away_team_name_id, switch_season(season))
			
		i += 1
		
def get_team_score_strength(url, home_team_id, away_team_id, season_id):
	page = requests.get(url)
	soup = BeautifulSoup(page.content, "html.parser")
	results = soup.find_all('table', class_="standard_tabelle")

	hometeam_data = {}
	awayteam_data = {}

	if len(results) <7:
		home_team_info = results[2]
		away_team_info = results[3]
	else:
		home_team_info = results[3]
		away_team_info = results[4]

	hometeam_data = get_data_from_hometeam(home_team_info, home_team_id, season_id)
	awayteam_data = get_data_from_awayteam(away_team_info, away_team_id, season_id)

	return {**hometeam_data, **awayteam_data}

def get_data_from_hometeam(team_info, team_id, season_id):
	playersdata = []
	tr_results  = team_info.find_all("tr")
	team_score = 0
	if len(tr_results) > 10:
		for i in range(0, 11):
			td_results = tr_results[i].find_all("td")
			player_number = td_results[0].text.strip()
			name_container = td_results[1].find('a')
			player_name = name_container.text.strip()
			player_href = td_results[1].find('a')['href']
			player_id = get_player_id(player_name, player_href, team_id)
			player_score = get_player_score_season(player_id,season_id)
			team_score  = team_score + player_score
			data = [player_number, player_name, player_id, player_score]
			playersdata.append(data)
	if len(playersdata):
		return_val = {"home_total_score":team_score, "home_team_strength":get_strength(team_score)}
	else:
		print("none")
		return_val = {}
	return return_val

def get_data_from_awayteam(team_info, team_id, season_id):
	playersdata = []
	tr_results  = team_info.find_all("tr")
	team_score = 0
	if len(tr_results) > 10:
		for i in range(0, 11):
			td_results = tr_results[i].find_all("td")
			player_number = td_results[0].text.strip()
			name_container = td_results[1].find('a')
			player_name = name_container.text.strip()
			player_href = td_results[1].find('a')['href']
			player_id = get_player_id(player_name, player_href, team_id)
			player_score = get_player_score_season(player_id,season_id)
			team_score  = team_score + player_score
			data = [player_number, player_name, player_id, player_score]
			playersdata.append(data)
	if playersdata:
		return_val = {"away_total_score":team_score, "away_team_strength":get_strength(team_score)}
	else:
		print("none")
		return_val = {}
	return return_val

def get_player_score_season(player_id , season_id):
	#mycursor = mydb.cursor()
	mysql_pool = MySQLPool(**dbconfig)
	sql = f'SELECT A.season_id, A.goals, A.started FROM player_career AS A INNER JOIN season AS B ON A.season_id = B.season_id WHERE player_id = {player_id} ORDER BY B.season_title ASC'
	#mycursor.execute(sql)
	
	wholeresult = mysql_pool.execute(sql)
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
 
def get_player_id(player_name, player_href, team_id):
	url = "https://www.worldfootball.net" + player_href	
	player_adding_info = get_more_player_info(url , player_name)
	player_birthday = player_adding_info[1]
	player_number  = player_adding_info[5]
	img_src_flag = 0
	#mycursor = mydb.cursor()
	mysql_pool = MySQLPool(**dbconfig)
	if 'gross/0.' in player_adding_info[0]:      		# No image, so empty man
		sql = f'SELECT * FROM playerlist WHERE player_name like "%{player_name}" and birthday = "{player_birthday}"'
		#mycursor.execute(sql)
		player_existing_result = mysql_pool.execute(sql)
		if len(player_existing_result):                 # match found with player name and birthday
			#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
			player_id = player_existing_result[0][0]
			return player_id
		else:      										# not found with name and birthday , so will find with player number and team id
			if player_number == "":
				player_number = 0
			sql = f"SELECT player_id, player_name, birthday from playerlist where now_pNumber = {player_number} and now_team_id = {team_id}"
			#mycursor.execute(sql)
			player_existing_result =mysql_pool.execute(sql)
			if len(player_existing_result):            # name or birthday match..so will check availabe 
				now_player_name = player_existing_result[0][1]
				now_player_birthday = player_existing_result[0][2]
				now_player_id = player_existing_result[0][0]
				
				if (player_birthday ==  now_player_birthday) | (player_name == now_player_name):
					#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
					sql= f'UPDATE playerlist set birthday = "{player_birthday}", player_name = "{player_name}" where player_id = {now_player_id}'
					mysql_pool.execute(sql, None,True)
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
		#mycursor.execute(sql)
		#player_existing_result = mycursor.fetchall()
		player_existing_result = mysql_pool.execute(sql)
		if len(player_existing_result) == 0:
			sql = f'SELECT * FROM playerlist WHERE player_name  like "%{player_name}" and birthday = "{player_birthday}"'
			#mycursor.execute(sql)
			#player_existing_result = mycursor.fetchall()
			player_existing_result = mysql_pool.execute(sql)
			if len(player_existing_result):   # its img_src, pnumber, team_id update
				player_id = player_existing_result[0][0]
				#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
				if player_number == "":
						player_number = 0
				sql = f'UPDATE playerlist set img_src = "{player_adding_info[0]}", now_pNumber = {player_number}, now_team_id = {team_id} where player_id = {player_id}'
				#mycursor.execute(sql)
				#mydb.commit()
				mysql_pool.execute(sql, None, True)
				#print(mycursor.rowcount, "record Updated. its img_src or pnumber, team id updated, have its own imag")
				return player_id
			else:   
				if player_number == "":
					player_number = 0				
				sql = f"SELECT player_id, player_name, birthday from playerlist where now_pNumber = {player_number} and now_team_id = {team_id}"
				#print(sql)
				#mycursor.execute(sql)
				player_existing_result = mysql_pool.execute(sql)
				if len(player_existing_result):            # name or birthday match..so will check availabe 
					now_player_name = player_existing_result[0][1]
					now_player_birthday = player_existing_result[0][2]
					now_player_id = player_existing_result[0][0]
					if (player_birthday ==  now_player_birthday) | (player_name == now_player_name):
						#print(f"   There is already in playerlist - {player_name} : {player_birthday}")
						sql= f'UPDATE playerlist set img_src = "{player_adding_info[0]}", birthday = "{now_player_birthday}", player_name = "{now_player_name}" where player_id = {now_player_id}'
						#mycursor.execute(sql)
						#mydb.commit()
						mysql_pool.execute(sql, None, True)
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
			#mycursor.execute(sql)
			#mydb.commit()
			mysql_pool.execute(sql, None, True)
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
	mydb = mysql.connector.connect(
		host="localhost",
		user="root",
		passwd="",
		database="soccer"
	)
	player_birthday = player_adding_info[1]
	player_nation = player_adding_info[2]
	#print(f"   this is new - {player_name} : {player_birthday}"
	mycursor = mydb.cursor()
	sql = "INSERT INTO soccer.playerlist (player_name, birthday , nationality, img_src, height, weight, foot" \
		  ", position , now_pNumber, now_team_id ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s,%s, %s)"
	val = (player_name, player_birthday, player_nation, player_adding_info[0], "???", player_adding_info[3], player_adding_info[4], "??", player_adding_info[5], team_id)
	mycursor.execute(sql, val)
	mydb.commit()
	#print("new player added - soccer ", player_name, player_birthday)
	return mycursor.lastrowid

def convert_strDate_sqlDateFormat(str_date):
	#  23/10/2020  - > 2020-10-23
	list = str_date.split('/');
	date = list[2] + '-' + list[1] + '-' + list[0];
	return date;

def get_team_strength_threading(match_id):
	#mycursor_threading = mydb.cursor()
	mysql_pool = MySQLPool(**dbconfig)
	print(f"  - starting team strength of {match_id} 's match")
	sql = f"SELECT * from season_match_plan where match_id = {match_id}"
	result = mysql_pool.execute(sql)
	result = result[0]
	if result[19]:                                   # home team strength
		print("     ~ no need to make a thread")
	else:
		print("     ~ doing get team news")
		season_id = result[1]
		sql = f"SELECT season_title from season where season_id  = {season_id}"
		season_title_result = mysql_pool.execute(sql)
		season_title_result = season_title_result[0][0]
		season_title = season_title_result.replace('/', '-')
		news_result = doing_team_news(season_title, result[2], str(result[3]), result[4], result[5], result[6])
		print("      # team news result :" , news_result)
		if news_result != None:
			if len(news_result) < 4:                      # team news is not available
				print("          # team news is still not available")
				now = dt.datetime.now()
				now_hour = int(now.hour)
				match_time = result[4]
				
				
				(match_hour, min) = match_time.split(':')
				match_hour = int(match_hour)
				if now_hour <= match_hour:
					threading.Timer(600,get_team_strength_threading, [match_id]).start()
				else:
					print("          # available time passed, so not repeated !")
			else:
		
				home_team_strength = news_result['home_team_strength']
			
				away_team_strength = news_result['away_team_strength']

				sql = f"UPDATE season_match_plan set  home_team_strength = '{home_team_strength}', away_team_strength = '{away_team_strength}' where match_id = {match_id}"
				nes_status = mysql_pool.execute(sql, None, True)
				print("          # team news inserted : ", nes_status)
		else:
			print("          # team news info is wrong or something go away!")
			now = dt.datetime.now()
			now_hour = int(now.hour)
			match_time = result[4]
			
			
			(match_hour, min) = match_time.split(':')
			match_hour = int(match_hour)
			if now_hour <= match_hour:
				threading.Timer(600,get_team_strength_threading, [match_id]).start()
			else:
				print("          # available time passed, so not repeated !")

def make_schedule_ofToday():
    # get match list of today
	mysql_pool = MySQLPool(**dbconfig)
	today = dt.datetime.today().strftime('%Y-%m-%d')
	(year, month, day) = today.split('-')
	year = int(year)
	month = int(month)
	day = int(day)
	#mycursor = mydb.cursor()
	sql = f"SELECT `match_id`, `time` FROM season_match_plan where date = '{today}' and status = 'LIVE' order by time "

	#mycursor.execute(sql)
	match_list  = mysql_pool.execute(sql)
	for match in match_list:
		match_time = match[1]
		match_id = match[0]
		
		(hour, min) = match_time.split(':')
		hour = int(hour) - 1
		min = int(min)
		
		time_tuple = (year, month, day , hour, min, 0, 0, 0 , 0)

		schedule_time = time.mktime(time_tuple)
		scheduler.enterabs(schedule_time , 1 , get_team_strength_threading, (match_id, ));
		print("# Schedule created for ", match_time, match_id)
	scheduler.run()

def main():
    
    make_schedule_ofToday()
    
if __name__ == "__main__":
	main()