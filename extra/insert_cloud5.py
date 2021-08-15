import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3

http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

################################################################
# This is the sample instructions to insert the match plan and match-player info.
# insert_match_plan("2014-2015", "eng-premier-league", 1,5)  match 1~ 5 eg: England 1 ~ 380
# direct write the info for inserting..... for saving time.
#################################################################

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Password@302",
  database="soccer_cloud_5"
)

mycursor = mydb.cursor()

def switch_season(argument):
    switcher = {
      "2014-2015": 1,
      "2015-2016": 2,
      "2016-2017_2" : 3,
      "2016-2017": 3,
      "2017-2018": 4,
      "2018-2019": 5,
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
        "srb-super-liga": 15    #Serbia
    }
    return switcher.get(argument, "null")

def insert_match_plan(season=None , league=None, firstMatch = None, lastMatch = None):

    print(f"--------------------------------- start-----------------------------------------")
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
    for i in range(firstMatch-1,lastMatch):

        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

        all_td = tr_results[i].find_all("td")
        if(len(all_td)) :
            print(f"------------------ {i + 1}th Match process start --------------------")
            if all_td[0].text !="":
                match_date = all_td[0].text
            match_total_result = all_td[5].text


            sql = f'SELECT team_id FROM team_list WHERE team_name = "{all_td[2].text}" UNION ' \
                  f'SELECT team_id FROM team_list WHERE team_name = "{all_td[4].text}"'
            mycursor.execute(sql)
            myresult = mycursor.fetchall()
            home_team_id = myresult[0][0]
            away_team_id = myresult[1][0]
            total_home_score = 0
            total_away_score = 0
            half_home_score = 0
            half_away_score = 0

            total = match_total_result.split(" ")[0]
            half = match_total_result.split(" ")[1]
            if len(total.split(":")) > 1:
                total_home_score = total.split(":")[0].strip()
                total_away_score = total.split(":")[1].strip()
                if len(half.split(":")) > 1:
                    half_home_score = half.split(":")[0][1:]
                    half_away_score = half.split(":")[1][:-1]


            print(f"{match_date}, {home_team_id}, {away_team_id},{total_home_score}-{total_away_score},{half_home_score}-{half_away_score} ")
            sql = "INSERT INTO season_match_plan (season_id, league_id , date, home_team_id , away_team_id , " \
                  "total_home_score, half_home_score, total_away_score, half_away_score)" \
                  "VALUES (%s, %s , %s, %s, %s, %s, %s, %s, %s)"
            val = (switch_season(season), switch_league(league),match_date, home_team_id, away_team_id,total_home_score , \
                   half_home_score,total_away_score , half_away_score)
            mycursor.execute(sql, val)
            mydb.commit()
            print("1 record inserted, ID: ", mycursor.lastrowid, " in match_plan")
            last_match_id = mycursor.lastrowid

            if all_td[5].find("a"):
                href_info = all_td[5].find("a")['href']
                url = "https://www.worldfootball.net"+href_info
                insert_match_team_player_info(url , last_match_id, home_team_id, away_team_id)
            print(f"------------------ {i + 1}th Match process end --------------------")
            i += 1
            #return

    print(f"--------------------------------- end -----------------------------------------")

def insert_match_team_player_info(url , last_match_id, home_team_id, away_team_id):
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
        a_results = every_tr.find_all("a")
        if a_results:
             goal_player_id_list.append(get_player_id(a_results[0]['title'] , a_results[0]['href']))
             if len(a_results) > 1:
                assist_player_id_list.append(get_player_id(a_results[1]['title'], a_results[1]['href'] ))
    print("    goal list - ", goal_player_id_list, "assist list - ", assist_player_id_list)


    a_results = home_team_container.find_all("a")
    if len(a_results) > 10:
        for i in range(0,11):
            id = get_player_id(a_results[i]['title'], a_results[i]['href'])
            goals = goal_player_id_list.count(id)
            assists = assist_player_id_list.count(id)

            sql = "INSERT INTO match_team_player_info ( match_id, team_id , player_id , " \
                  "goals, assists)" \
                  "VALUES (%s, %s , %s, %s, %s)"
            val = (last_match_id, home_team_id, id, goals, assists)
            mycursor.execute(sql, val)
            mydb.commit()
            print(" inserted home team player - ",i+1 ,a_results[i]['title'] , id, goals, assists)
        a_results = away_team_container.find_all("a")
        for i in range(0,11):
            id = get_player_id(a_results[i]['title'], a_results[i]['href'])
            goals = goal_player_id_list.count(id)
            assists = assist_player_id_list.count(id)

            sql = "INSERT INTO match_team_player_info ( match_id, team_id , player_id , " \
                  "goals, assists)" \
                  "VALUES (%s, %s , %s, %s, %s)"
            val = (last_match_id, away_team_id, id, goals, assists)
            mycursor.execute(sql, val)
            mydb.commit()
            print(" inserted away team player - ", i+1 , a_results[i]['title'] , id, goals, assists)

def get_player_id(player_name, player_href):

    url = "https://www.worldfootball.net"+player_href
    #print(url);

    page = requests.get(url,headers={"User-Agent":"Mozilla/5.0"})
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find('div', itemtype="http://schema.org/Person")

    """player_nation_container = results.find(string="Nationality:").findNext("td")
    player_nation_container = player_nation_container.find_all("img")
    count = 0;
    player_nation = ""
    for i in player_nation_container:
        if count > 0:
            player_nation += ","
        player_nation += i['alt']
        count += 1"""

    player_birthday = "???"
    if results.find(string="Born:"):
        player_birthday = results.find(string="Born:").findNext("td").text.strip()
        player_birthday = player_birthday.split(" ")[0]
        player_birthday = player_birthday.replace(".", "/")


    sql = f'SELECT player_id FROM playerlist WHERE player_name LIKE "%{player_name}%" and birthday = "{player_birthday}"'

    mycursor.execute(sql)
    myresult = mycursor.fetchone()
    if myresult:
        id = myresult[0]
    else:
        id= add_extra_player(player_name,player_birthday,url)
    return id

def add_extra_player(player_name, player_birthday,url):
    page = requests.get(url,headers={"User-Agent":"Mozilla/5.0"})
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find('div', itemtype="http://schema.org/Person")
    player_img_cont = results.find("img", alt=player_name)
    if player_img_cont:
        player_img = player_img_cont.get('src')
    else:
        player_name = " "+ player_name
        player_img_cont = results.find("img", alt=player_name)
        if player_img_cont:
            player_img = player_img_cont.get('src')
        else:
            print("Add extra player error! No player name")
            exit()


    player_nation_container = results.find(string="Nationality:").findNext("td")
    player_nation_container = player_nation_container.find_all("img")
    count = 0;
    player_nation = ""
    for i in player_nation_container:
        if count > 0:
            player_nation += ","
        player_nation += i['alt']
        count += 1

    player_weight = "???"
    if results.find(string="Weight:"):
        player_weight = results.find(string="Weight:").findNext("td").text.strip()
    player_foot = "???"
    if results.find(string="Foot:"):
        player_foot = results.find(string="Foot:").findNext("td").text.strip()
    sql = "INSERT INTO soccer_cloud.playerlist (player_name, birthday , nationality, img_src, height, weight, foot" \
          ", position ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s)"
    val = (player_name, player_birthday, player_nation, player_img, "???", player_weight, player_foot, "??")
    mycursor.execute(sql, val)
    mydb.commit()
    print("new player added - soccer-cloud: ", player_name, player_birthday)
    sql = "INSERT INTO soccer.playerlist (player_name, birthday , nationality, img_src, height, weight, foot" \
          ", position ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s)"
    val = (player_name, player_birthday, player_nation, player_img, "???", player_weight, player_foot, "??")
    mycursor.execute(sql, val)
    mydb.commit()

    sql = "INSERT INTO soccer_cloud_3.playerlist (player_name, birthday , nationality, img_src, height, weight, foot" \
          ", position ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s)"
    val = (player_name, player_birthday, player_nation, player_img, "???", player_weight, player_foot, "??")
    mycursor.execute(sql, val)
    mydb.commit()

    sql = "INSERT INTO soccer_cloud_4.playerlist (player_name, birthday , nationality, img_src, height, weight, foot" \
          ", position ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s)"
    val = (player_name, player_birthday, player_nation, player_img, "???", player_weight, player_foot, "??")
    mycursor.execute(sql, val)
    mydb.commit()

    sql = "INSERT INTO soccer_cloud_5.playerlist (player_name, birthday , nationality, img_src, height, weight, foot" \
          ", position ) VALUES (%s, %s , %s, %s, %s, %s, %s, %s)"
    val = (player_name, player_birthday, player_nation, player_img, "???", player_weight, player_foot, "??")
    mycursor.execute(sql, val)
    mydb.commit()

    print("new player added - soccer : ", player_name, player_birthday)
    return mycursor.lastrowid

insert_match_plan("2014-2015","srb-super-liga",1,240)
