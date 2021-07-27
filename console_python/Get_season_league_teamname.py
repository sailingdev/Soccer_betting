import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3

http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

################################################################
# This is the sample instructions to insert the team info(team_list and season_league_team info) into database.
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
        "2019-2020": 12,
        "2020": 64, #795
        "2020-2021" : 799,
        "2021"    : 844,
        "2021-2022": 857
    }
    return switcher.get(argument, "null")
    
def switch_league(argument):
    switcher = {	
        "aut-bundesliga": 1,                  # Austria
        "bul-parva-liga" : 2,				  # Bulgaria
		# "bul-a-grupa": 2,    
		"cze-1-fotbalova-liga": 3,            # Chezch
        # "cze-gambrinus-liga": 3,		
		"cro-1-hnl": 4,                       # Croatia
		"den-superliga": 5,                   # Denmark
        # "den-sas-ligaen": 5,
		"eng-premier-league": 6,              # England
		"fra-ligue-1": 7,                     # France
		"bundesliga": 8,                      # Germany
		"gre-super-league": 9,                # Greece
		"hun-nb-i": 10,                       # Hungary
        # "hun-nb1": 10,
        # "hun-otp-liga": 10,
		"ita-serie-a": 11,                    # Italy
		"ned-eredivisie": 12,                 # Netherland
		"nor-eliteserien": 13,                # Norway from 2020
        #"nor-tippeligaen": 13,
		"por-primeira-liga": 14,              # Portugal, Check
        # "por-liga-sagres": 14,
		"srb-super-liga": 15,                 # Serbia
		"esp-primera-division": 16,           # Spain
        "swe-allsvenskan": 17,                # Sweden
        "swi-super-league": 18,               # Swiztland
		"tur-superlig": 19,                   # Turkey
        "ukr-premyer-liga": 20                # Ukraine
    }
    return switcher.get(argument, "null")

def scrape_season_league_teamname(season=None , league=None):

    if season:
        URL = f"https://www.worldfootball.net/players/{league}-{season}"
    else:
        URL = f"https://www.worldfootball.net/players/eng-premier-league-2014-2015/"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")

    results = soup.find('table', class_="standard_tabelle")
    tr_results = results.find_all("tr")
    team_info = []
    for tr in tr_results:
        td = tr.find("td")
        img_src = td.find("img")['src']
        teamname = td.find("img")['title']
        ev_team = [img_src, teamname]
        team_info.append(ev_team)
        #print(f"img : {img_src} , title : {teamname}")

    return team_info

def print_scrape_season_league_teamname(season=None , league=None):
    if season == None:
        print(f"Enter the season and league")
    else:
        team_info = scrape_season_league_teamname(season,league)
        i=0
        for  info in team_info:
            i+=1
            print(f"{season}'s {league} match {i}team src: {info[0]} -- name: {info[1]}\n")

def insert_teamList(season=None, league=None):
    if season == None:
        print("Enter the season and league!")
        return
    teamNameList = scrape_season_league_teamname(season, league)

    for team_info in teamNameList:
        teamname = team_info[1]
        team_src = team_info[0]

        sql = f"SELECT * FROM team_list WHERE team_name ='{teamname}'"
        mycursor.execute(sql)
        myresult = mycursor.fetchall()
        if len(myresult):
            print(f"There is already in team list - {teamname}")
        else:
            print(f"this is new - {teamname}")
            sql = "INSERT INTO team_list (team_name, league_id , img_src) VALUES (%s, %s , %s)"
            val = (teamname, switch_league(league), team_src)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")

        # insert teamlist end

        sql = f"SELECT team_id FROM team_list WHERE team_name = '{teamname}'"
        mycursor.execute(sql)
        myresult = mycursor.fetchone()
        team_id = myresult[0]

        sql = "INSERT INTO season_league_team_info (season_id, league_id, team_id) VALUES (%s, %s , %s)"
        val = (switch_season(season), switch_league(league) , team_id)
        mycursor.execute(sql, val)
        mydb.commit()
        print("insert season_league_team end!")

insert_teamList("2021-2022", "aut-bundesliga")
insert_teamList("2021-2022", "bul-parva-liga")
insert_teamList("2021-2022", "cze-1-fotbalova-liga")
insert_teamList("2021-2022", "cro-1-hnl")
# insert_teamList("2021-2022", "den-superliga")
insert_teamList("2021-2022", "eng-premier-league")
insert_teamList("2021-2022", "fra-ligue-1")
insert_teamList("2021-2022", "bundesliga")
# insert_teamList("2021-2022", "gre-super-league")
insert_teamList("2021-2022", "hun-nb-i")
insert_teamList("2021-2022", "ita-serie-a")
insert_teamList("2021-2022", "ned-eredivisie")
insert_teamList("2021", "nor-eliteserien")
insert_teamList("2021-2022", "por-primeira-liga")
insert_teamList("2021-2022", "srb-super-liga")
insert_teamList("2021-2022", "esp-primera-division")
insert_teamList("2021", "swe-allsvenskan")
# insert_teamList("2021-2022", "swi-super-league")
# insert_teamList("2021-2022", "tur-superlig")
insert_teamList("2021-2022", "ukr-premyer-liga")









