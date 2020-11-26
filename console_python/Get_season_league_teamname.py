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
      "2019-2020": 12,
      "2020": 795,
      "2020-2021" : 799
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
        "gre-super-league": 9,   #Greece
        "tur-sueperlig": 19,   #Turkey
        "nor-eliteserien": 13,  #Norway
        "nor-tippeligaen":13,
        "swe-allsvenskan": 17,  #Sweden
        "sui-super-league": 18,   #Swiztland
        "den-superliga": 5,     #Denmark
        "den-sas-ligaen":5,
        "ukr-premyer-liga": 20,     #Ukraine
        "bul-a-grupa": 2,       #bulgaria
        "bul-parva-liga" : 2 ,
        "cze-1-fotbalova-liga": 3,      #Chezch
        "cze-gambrinus-liga": 3,
        "cro-1-hnl": 4 ,          #Croatia
        "hun-nb-i": 10,     #Hungary
        "hun-nb1": 10,
        "hun-otp-liga":10,
        "srb-super-liga": 15    #Serbia
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

         #insert teamlist end

        sql = f"SELECT team_id FROM team_list WHERE team_name = '{teamname}'"
        mycursor.execute(sql)
        myresult = mycursor.fetchone()
        team_id = myresult[0]

        sql = "INSERT INTO season_league_team_info (season_id, league_id, team_id) VALUES (%s, %s , %s)"
        val = (switch_season(season), switch_league(league) , team_id)
        mycursor.execute(sql, val)
        mydb.commit()
        print("insert season_league_team end!")

insert_teamList("2020-2021","gre-super-league")









