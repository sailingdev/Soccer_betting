import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3

http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

################################################################
# This is the sample instructions to insert the team info(team_list and season_league_team into) into database.
# python3 get_season_league_teamname.py -season 2021-2022 -league esp-primera-division
#################################################################

season_array1 = [19, 17, 15, 13, 1, 2, 3, 4, 5, 12, 799, 857]         # 2021-2022 style
season_array2 = [20, 18, 16, 14, 6, 7, 8, 9, 10, 11, 64, 844]         # 2021 style

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="soccer"
)
mycursor = mydb.cursor()

def insert_team_point_to_DB():
  #sql = f"SELECT  * FROM ranking_range_table"
  #mycursor.execute(sql)
  #rangeVal = mycursor.fetchall()
  
  sql = f"SELECT  season_id, league_id, team_id, info_id FROM season_league_team_info"
  mycursor.execute(sql)
  myresult = mycursor.fetchall()

  for i in range(3503, 3821):   # here the sequence...... index - 1  so index 0 means 1st row...season_league_team_info 3503
      season_id = myresult[i][0]
      league_id = myresult[i][1]
      team_id = myresult[i][2]
      info_id = myresult[i][3]

      print(season_id, league_id, team_id)
      h_mp = h_w = h_d = h_l = h_f = h_a = 0
      a_mp = a_w = a_d = a_l = a_f = a_a = 0
      t_mp = t_w = t_d = t_l = t_f = t_a = 0
      D = P = 0

      sql = f"select total_home_score, total_away_score from season_match_plan where season_id = {season_id} and league_id = {league_id} and home_team_id={team_id}"
      mycursor.execute(sql)
      team_match_results = mycursor.fetchall()
      for match in team_match_results:
        h_mp += 1                                           # total count for home matches
        if (match[0] > match[1]):
          h_w += 1                                          # total count for home wins
        elif (match[0] == match[1]):
          h_d += 1                                          # total count for home draws
        else:
          h_l += 1                                          # total count for home lost
        h_f += match[0]                                     # total count for home goals
        h_a += match[1]                                     # total count for home lost goals
      
      ########## get home data end #####################
      sql = f"select total_home_score, total_away_score from season_match_plan where season_id = {season_id} and league_id = {league_id} and away_team_id={team_id}"
      mycursor.execute(sql)
      team_match_results = mycursor.fetchall()
      for match in team_match_results:
        a_mp += 1                                           # total count for away matches
        if (match[0] > match[1]):
          a_l += 1                                          # total count for away lost
        elif (match[0] == match[1]):
          a_d += 1                                          # total count for away draws
        else:
          a_w += 1                                          # total count for away wins
        a_a += match[0]                                     # total count for away lost goals
        a_f += match[1]                                     # total count for away goals
      print(a_mp, a_w, a_d, a_l, a_f, a_a)
      ########## get AWAY data end #####################
      t_mp = h_mp + a_mp                                    # total matches summed home and away
      t_w = h_w + a_w
      t_d = h_d + a_d
      t_l = h_l + a_l
      t_f = h_f + a_f
      t_a = h_a + a_a
      D = t_f - t_a                                         # diffesr between total goals and total lost goals
      P = t_w *3 + t_d                                      # total points for this season

      ########## get Total data end #####################
      PPG = round(P/t_mp , 2) 
      HPPG = round((h_w*3 + h_d)/h_mp, 2)
      H_percent = str(round((h_w / h_mp * 100))) + "%"
      HG = h_f
      HDGPG = round( (h_f - h_a)/h_mp,2)
      #HRS = HPPG + HDGPG

      APPG = round((a_w*3 + a_d)/a_mp, 2)
      A_percent = str(round((a_w / a_mp * 100))) + "%"
      AG = a_f
      ADGPG = round((a_f - a_a )/a_mp,2)
      #ARS = APPG + ADGPG
      ############################################################################
      preSeasonId = 0
      if season_id in season_array1:
          nowSeasonIndex = season_array1.index(season_id)
          if(nowSeasonIndex > 0):
              preSeasonId = season_array1[nowSeasonIndex - 1]
      if season_id in season_array2:
          nowSeasonIndex = season_array2.index(season_id)
          if(nowSeasonIndex > 0):
              preSeasonId = season_array2[nowSeasonIndex - 1]
      
      ################ get pre season Id end #####################################
      sql = f"SELECT HPPG, HDGPG, APPG, ADGPG FROM season_league_team_info where league_id = {league_id} and season_id = {preSeasonId} and team_id = {team_id}"
      mycursor.execute(sql)
      promoteStatusResult = mycursor.fetchall()
      promoteStatus = 1
      if len(promoteStatusResult):
          promoteStatus = 0
      else:
          promoteStatus = 1
      ################ get promoted status end ##################################
      if (preSeasonId == 0) | (promoteStatus ==  1):    # season is 2010 or team is promoted team
            H_ranking = 2
            A_ranking = 1
            HRS = 0
            ARS = 0

      else:
            HRS = promoteStatusResult[0][0] + promoteStatusResult[0][1]
            ARS = promoteStatusResult[0][2] + promoteStatusResult[0][3]
            H_ranking = getRangeValue(HRS)
            A_ranking = getRangeValue(ARS)

      sql = f"update season_league_team_info set t_mp = {t_mp} ,t_w = {t_w}, t_d = {t_d}, t_l = {t_l},t_f = {t_f}, \
        t_a = {t_a},h_mp = { h_mp},h_w = {h_w}, h_d = {h_d}, h_l = {h_l},h_f = {h_f}, h_a = {h_a}, a_mp = {a_mp}, \
        a_w = {a_w}, a_d = {a_d}, a_l = {a_l}, a_f = {a_f}, a_a = {a_a},D = {D}, P={P}, \
        PPG={PPG}, HPPG={HPPG}, H_percent = '{H_percent}', HG={HG}, HDGPG={HDGPG}, HRS = {HRS}, APPG={APPG}, A_percent = '{A_percent}', \
        AG={AG}, ADGPG={ADGPG},ARS = {ARS} , S_H_ranking =  {H_ranking}, S_A_ranking = {A_ranking}\
        where info_id = {info_id}"
      
      mycursor.execute(sql)
      mydb.commit()

      print(f"-------------------------One row -{info_id} - updated-------------------")
  print("-------------------end-------------------------------------")

def getRangeValue(RS):
    print("---------------------------")
    if RS <= 0:
        return 1
    if (RS >0) & (RS < 1):
        return 2
    if (RS >=1) & (RS < 2):
        return 3
    if (RS >=2) & (RS < 3):
        return 4
    if (RS >=3) & (RS < 4):
        return 5
    if (RS >=4) & (RS < 5):
        return 6
    if (RS >=5):
        return 7
        
insert_team_point_to_DB()