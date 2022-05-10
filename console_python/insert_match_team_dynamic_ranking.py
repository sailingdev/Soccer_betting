# This file must be runed after inert_team_point_stranking_to_DB.py because 
# here we have to calculare Static ranking using team points
import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3
import sys
import json

def switch_season(argument):
    switcher = {
		"2010-2011": 19,
		"2011-2012": 17,
		"2012-2013": 15,
		"2013-2014": 13,
		"2014-2015": 1,
		"2015-2016": 2,
		"2016-2017": 3,
		"2017-2018": 4,
		"2018-2019": 5,
		"2019-2020": 12,
		"2020-2021": 799,
		"2021-2022": 857,
        "2022-2023": 935,	
		"2010": 20,
		"2011": 18,
		"2012": 16,
		"2013": 14,
		"2014": 6,
		"2015": 7,
		"2016": 8,
		"2017": 9,
		"2018": 10,
		"2019": 11,
		"2020": 64,
		"2021": 844,
        "2022": 916
	}
    return switcher.get(argument, "null")

season_array1 = [19, 17, 15, 13, 1, 2, 3, 4, 5, 12, 799, 857, 935]         # 2021-2022 style
season_array2 = [20, 18, 16, 14, 6, 7, 8, 9, 10, 11, 64, 844, 916]         # 2021 style

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

mydb = mysql.connector.connect( 
    host="localhost",
    user="root",
    passwd="P@ssw0rd2021",
    database="soccer"
)
mycursor = mydb.cursor(buffered=True)

def insert_match_team_dynamic_ranking_8(league, season, target_status):
    #sql = f"SELECT  * FROM ranking_range_table"
    #mycursor.execute(sql)
    #rangeVal = mycursor.fetchall()
    moving_num = 8
    print(f"---------------{league}-{season}-{target_status} games - {moving_num}-start-----------------")
    league_id = switch_league(league)
    season_id = switch_season(season)
    
    sql = f"SELECT team_id FROM season_league_team_info where league_id = {league_id} and season_id = {season_id}"
    mycursor.execute(sql)
    wholeTeamIdResult = mycursor.fetchall()
    print(wholeTeamIdResult)
    for eachTeamId in wholeTeamIdResult:
        team_id = eachTeamId[0]
        
        ################################################################################################
        
        print(f"     ------------{league} {season} {moving_num} team id {team_id} start ---------------")
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
        sql = f"SELECT * FROM season_match_plan where league_id = {league_id} and season_id = {preSeasonId} and home_team_id = {team_id}"
        mycursor.execute(sql)
        promoteStatusResult = mycursor.fetchall()
        promoteStatus = 1
        if len(promoteStatusResult):
            promoteStatus = 0
        else:
            promoteStatus = 1
        ############### get promote status ###########################################
        
        print(f"                ---------- home team info start -------------------")
        sql = f"SELECT * FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and status = '{target_status}'  ORDER BY date ASC"
        mycursor.execute(sql)
        matchResult = mycursor.fetchall()                     # home team array
        if  target_status == "END":
            nIndex_Match_of_season = 0
        else:
            sql = f"SELECT count(*) from season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and status = 'END'  "
            mycursor.execute(sql)
            nIndex_Match_of_season = mycursor.fetchone()[0]
        HPPG = ""
        HGDPG = ""
        D_Home_RS = ""
        D_Home_ranking = ""
        sum_HRS = 0
        for eachMatch in matchResult:                         # loop home matches array with team_id to get Dynamic home rankings
            nowMatch_id = eachMatch[0]
            nowDate = eachMatch[3]
            if eachMatch[7] > eachMatch[9] :
                HPPG = "3"
            elif eachMatch[7] == eachMatch[9] :
                HPPG = "1"
            else:
                HPPG = "0"
            HGDPG = str(eachMatch[7] - eachMatch[9])
            ################################ HPPG, HDGPG ##################################
            if (preSeasonId == 0) | (promoteStatus ==  1):    # season is 2010 or team is promoted team
                if nIndex_Match_of_season < moving_num:
                      D_Home_ranking = "1"
                      sum_HRS = (int(HPPG) + int(HGDPG))
                      #  D_Home_RS = str(sum_HRS)
                      D_Home_RS = "N/A"
                if nIndex_Match_of_season >= moving_num:
                      sql = f"SELECT HPPG, HGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {moving_num}"
                      mycursor.execute(sql)
                      previous_num_Match_Results = mycursor.fetchall()
                      HRS = 0
                      for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                            HRS += (int(eachMatch[0]) + int(eachMatch[1]))
                      HRS /= moving_num
                      D_Home_RS = str(HRS)
                      D_Home_ranking = str(getRangeValue(HRS))
                                
            else:                                             # season is over 2011 and not promoted team
                if nIndex_Match_of_season < moving_num:
                    HRS = 0
                    sql = f"SELECT HPPG, HGDPG FROM season_match_plan where league_id = {league_id} and season_id = {preSeasonId} and home_team_id = {team_id}   and status = 'END' ORDER BY date DESC LIMIT {moving_num - nIndex_Match_of_season}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        if eachMatch[0] != "":
                            HRS += (int(eachMatch[0]) + int(eachMatch[1]))
                    sql = f"SELECT HPPG, HGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {nIndex_Match_of_season}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        
                        HRS += (int(eachMatch[0]) + int(eachMatch[1]))
                    
                    HRS /= moving_num
                    D_Home_RS = str(HRS)
                    D_Home_ranking = str(getRangeValue(HRS))
                else:
                    sql = f"SELECT  HPPG, HGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {moving_num}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    HRS = 0
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        #if eachMatch[0] != "":
                        HRS += (int(eachMatch[0]) + int(eachMatch[1]))
                    HRS /= moving_num
                    D_Home_RS = str(HRS)
                    D_Home_ranking = str(getRangeValue(HRS))
            sql = f"update season_match_plan set D_Home_RS_8 = '{D_Home_RS}', D_Home_ranking_8 = '{D_Home_ranking}', HPPG = '{HPPG}', HGDPG = '{HGDPG}' where match_id = {nowMatch_id}"
            mycursor.execute(sql)
            mydb.commit()
            print(f"                    match id {nowMatch_id} 's dynamic home ranking value is updated!")

            if target_status == "END":
                nIndex_Match_of_season += 1  
        print(f"                ---------- home team info End -------------------")

        ###################################### home ranking process end ####################
        print(f"                ---------- away team info start -------------------")
        sql = f"SELECT * FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id}  and status = '{target_status}' ORDER BY date ASC"
        mycursor.execute(sql)
        matchResult = mycursor.fetchall()     # home team array
        if  target_status == "END":
            nIndex_Match_of_season = 0
        else:
            sql = f"SELECT count(*) from season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id} and status = 'END'  "
            mycursor.execute(sql)
            nIndex_Match_of_season = mycursor.fetchone()[0]
        sum_ARS = 0
        APPG = ""
        AGDPG = ""
        D_Away_RS = ""
        D_Away_ranking = ""
        for eachMatch in matchResult:                       # loop away matches array with team_id to get Dynamic away rankings
            nowMatch_id = eachMatch[0]
            nowDate = eachMatch[3]
            if eachMatch[7] < eachMatch[9] :
                APPG = "3"
            elif eachMatch[7] == eachMatch[9] :
                APPG = "1"
            else:
                APPG = "0"
            AGDPG = str(eachMatch[9] - eachMatch[7])
            ################################ APPG, AGDPG ##################################

            if (preSeasonId == 0) | (promoteStatus ==  1):    # season is 2010 or team is promoted team
                if nIndex_Match_of_season < moving_num:
                      D_Away_ranking = "1"
                      sum_ARS = (int(APPG) + int(AGDPG))
                      #  D_Away_RS = str(sum_ARS)
                      D_Away_RS = "N/A"
                if nIndex_Match_of_season >= moving_num:
                      sql = f"SELECT APPG, AGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {moving_num}"
                      mycursor.execute(sql)
                      previous_num_Match_Results = mycursor.fetchall()
                      ARS = 0
                      for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                          ARS += (int(eachMatch[0]) + int(eachMatch[1]))
                      ARS /= moving_num
                      D_Away_RS = str(ARS)
                      D_Away_ranking = str(getRangeValue(ARS))
                
                
            else:
                if nIndex_Match_of_season < moving_num:
                    ARS = 0
                    sql = f"SELECT APPG, AGDPG FROM season_match_plan where league_id = {league_id} and season_id = {preSeasonId} and away_team_id = {team_id}  and status = 'END' ORDER BY date DESC LIMIT {moving_num - nIndex_Match_of_season}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        if eachMatch[0] != "":
                            ARS +=(int(eachMatch[0]) + int(eachMatch[1]))
                    sql = f"SELECT APPG, AGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id} and date < '{nowDate}'   and status = 'END' ORDER BY date DESC LIMIT {nIndex_Match_of_season}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        
                        ARS += (int(eachMatch[0]) + int(eachMatch[1]))
                    
                    ARS /= moving_num
                    D_Away_RS = str(ARS)
                    D_Away_ranking = str(getRangeValue(ARS))
                else:
                    sql = f"SELECT APPG, AGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {moving_num}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    ARS = 0
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        if eachMatch[0] != "":
                            ARS += (int(eachMatch[0]) + int(eachMatch[1]))
                    ARS /= moving_num
                    D_Away_RS = str(ARS)
                    D_Away_ranking = str(getRangeValue(ARS))
            
            
            sql = f"update season_match_plan set D_Away_RS_8 = '{D_Away_RS}', D_Away_ranking_8 = '{D_Away_ranking}', APPG = '{APPG}', AGDPG = {AGDPG} where match_id = {nowMatch_id}"
            mycursor.execute(sql)
            mydb.commit()
            print(f"                    match id {nowMatch_id} 's dynamic away ranking value is updated!")
            if target_status == "END":
                nIndex_Match_of_season += 1 
        print(f"                ---------- away team info End -------------------")
        print(f"     ------------{league} {season} {moving_num} team id {team_id} End ---------------")
        ####################################### One team process End ##########################################################


    print(f"---------------{league}-{season}-{moving_num}-End-----------------")

def insert_match_team_dynamic_ranking_6(league, season, target_status):
    #sql = f"SELECT  * FROM ranking_range_table"
    #mycursor.execute(sql)
    #rangeVal = mycursor.fetchall()
    moving_num = 6
    print(f"---------------{league}-{season}- {target_status} game-{moving_num}-start-----------------")
    
    league_id = switch_league(league)
    season_id = switch_season(season)
    
    sql = f"SELECT team_id FROM season_league_team_info where league_id = {league_id} and season_id = {season_id}"
    mycursor.execute(sql)
    wholeTeamIdResult = mycursor.fetchall()
    print(wholeTeamIdResult)
    for eachTeamId in wholeTeamIdResult:
        team_id = eachTeamId[0]
        
        ################################################################################################
        
        print(f"     ------------ {league} - {season} -{moving_num} team id {team_id} start ---------------")
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
        sql = f"SELECT * FROM season_match_plan where league_id = {league_id} and season_id = {preSeasonId} and home_team_id = {team_id}"
        mycursor.execute(sql)
        promoteStatusResult = mycursor.fetchall()
        promoteStatus = 1
        if len(promoteStatusResult):
            promoteStatus = 0
        else:
            promoteStatus = 1
        ############### get promote status ###########################################
        
        print(f"                ---------- home team info start -------------------")
        sql = f"SELECT * FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and status = '{target_status}'  ORDER BY date ASC"
        mycursor.execute(sql)
        matchResult = mycursor.fetchall()                     # home team array
        if  target_status == "END":
            nIndex_Match_of_season = 0
        else:
            sql = f"SELECT count(*) from season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and status = 'END'  "
            mycursor.execute(sql)
            nIndex_Match_of_season = mycursor.fetchone()[0]
        HPPG = ""
        HGDPG = ""
        D_Home_RS = ""
        D_Home_ranking = ""
        sum_HRS = 0
        for eachMatch in matchResult:                         # loop home matches array with team_id to get Dynamic home rankings
            nowMatch_id = eachMatch[0]
            nowDate = eachMatch[3]
            if eachMatch[7] > eachMatch[9] :
                HPPG = "3"
            elif eachMatch[7] == eachMatch[9] :
                HPPG = "1"
            else:
                HPPG = "0"
            HGDPG = str(eachMatch[7] - eachMatch[9])
            ################################ HPPG, HDGPG ##################################
            if (preSeasonId == 0) | (promoteStatus ==  1):    # season is 2010 or team is promoted team
                if nIndex_Match_of_season < moving_num:
                      D_Home_ranking = "1"
                      sum_HRS = (int(HPPG) + int(HGDPG))
                      #  D_Home_RS = str(sum_HRS)
                      D_Home_RS = "N/A"
                if nIndex_Match_of_season >= moving_num:
                      sql = f"SELECT HPPG, HGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and date < '{nowDate}'   and status = 'END' ORDER BY date DESC LIMIT {moving_num}"
                      mycursor.execute(sql)
                      previous_num_Match_Results = mycursor.fetchall()
                      HRS = 0
                      for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                            HRS += (int(eachMatch[0]) + int(eachMatch[1]))
                      HRS /= moving_num
                      D_Home_RS = str(HRS)
                      D_Home_ranking = str(getRangeValue(HRS))
                                
            else:                                             # season is over 2011 and not promoted team
                if nIndex_Match_of_season < moving_num:
                    HRS = 0
                    sql = f"SELECT HPPG, HGDPG FROM season_match_plan where league_id = {league_id} and season_id = {preSeasonId} and home_team_id = {team_id}   and status = 'END' ORDER BY date DESC LIMIT {moving_num - nIndex_Match_of_season}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        if eachMatch[0] != "":
                            HRS += (int(eachMatch[0]) + int(eachMatch[1]))
                    sql = f"SELECT HPPG, HGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {nIndex_Match_of_season}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        
                        HRS += (int(eachMatch[0]) + int(eachMatch[1]))
                    
                    HRS /= moving_num
                    D_Home_RS = str(HRS)
                    D_Home_ranking = str(getRangeValue(HRS))
                else:
                    sql = f"SELECT  HPPG, HGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and home_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {moving_num}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    HRS = 0
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        #if eachMatch[0] != "":
                        HRS += (int(eachMatch[0]) + int(eachMatch[1]))
                    HRS /= moving_num
                    D_Home_RS = str(HRS)
                    D_Home_ranking = str(getRangeValue(HRS))
            sql = f"update season_match_plan set D_Home_RS_6 = '{D_Home_RS}', D_Home_ranking_6 = '{D_Home_ranking}', HPPG = '{HPPG}', HGDPG = '{HGDPG}' where match_id = {nowMatch_id}"
            mycursor.execute(sql)
            mydb.commit()
            print(f"                    match id {nowMatch_id} 's dynamic home ranking value is updated!")

            if target_status == "END":
                nIndex_Match_of_season += 1   
        print(f"                ---------- home team info End -------------------")

        ###################################### home ranking process end ####################
        print(f"                ---------- away team info start -------------------")
        sql = f"SELECT * FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id}  and status = '{target_status}'  ORDER BY date ASC"
        mycursor.execute(sql)
        matchResult = mycursor.fetchall()     # home team array
        if  target_status == "END":
            nIndex_Match_of_season = 0
        else:
            sql = f"SELECT count(*) from season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id} and status = 'END'  "
            mycursor.execute(sql)
            nIndex_Match_of_season = mycursor.fetchone()[0]
        sum_ARS = 0
        APPG = ""
        AGDPG = ""
        D_Away_RS = ""
        D_Away_ranking = ""
        for eachMatch in matchResult:                       # loop away matches array with team_id to get Dynamic away rankings
            nowMatch_id = eachMatch[0]
            nowDate = eachMatch[3]
            if eachMatch[7] < eachMatch[9] :
                APPG = "3"
            elif eachMatch[7] == eachMatch[9] :
                APPG = "1"
            else:
                APPG = "0"
            AGDPG = str(eachMatch[9] - eachMatch[7])
            ################################ APPG, AGDPG ##################################

            if (preSeasonId == 0) | (promoteStatus ==  1):    # season is 2010 or team is promoted team
                if nIndex_Match_of_season < moving_num:
                      D_Away_ranking = "1"
                      sum_ARS = (int(APPG) + int(AGDPG))
                      #  D_Away_RS = str(sum_ARS)
                      D_Away_RS = "N/A"
                if nIndex_Match_of_season >= moving_num:
                      sql = f"SELECT APPG, AGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {moving_num}"
                      mycursor.execute(sql)
                      previous_num_Match_Results = mycursor.fetchall()
                      ARS = 0
                      for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                          ARS += (int(eachMatch[0]) + int(eachMatch[1]))
                      ARS /= moving_num
                      D_Away_RS = str(ARS)
                      D_Away_ranking = str(getRangeValue(ARS))
                
                
            else:
                if nIndex_Match_of_season < moving_num:
                    ARS = 0
                    sql = f"SELECT APPG, AGDPG FROM season_match_plan where league_id = {league_id} and season_id = {preSeasonId} and away_team_id = {team_id}  and status = 'END' ORDER BY date DESC LIMIT {moving_num - nIndex_Match_of_season}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        if eachMatch[0] != "":
                            ARS +=(int(eachMatch[0]) + int(eachMatch[1]))
                    sql = f"SELECT APPG, AGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {nIndex_Match_of_season}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        
                        ARS += (int(eachMatch[0]) + int(eachMatch[1]))
                    
                    ARS /= moving_num
                    D_Away_RS = str(ARS)
                    D_Away_ranking = str(getRangeValue(ARS))
                else:
                    sql = f"SELECT APPG, AGDPG FROM season_match_plan where league_id = {league_id} and season_id = {season_id} and away_team_id = {team_id} and date < '{nowDate}'  and status = 'END' ORDER BY date DESC LIMIT {moving_num}"
                    mycursor.execute(sql)
                    previous_num_Match_Results = mycursor.fetchall()
                    ARS = 0
                    for eachMatch in previous_num_Match_Results:      # loop previous results to get aver Dynamic ranking
                        if eachMatch[0] != "":
                            ARS += (int(eachMatch[0]) + int(eachMatch[1]))
                    ARS /= moving_num
                    D_Away_RS = str(ARS)
                    D_Away_ranking = str(getRangeValue(ARS))
            
            
            sql = f"update season_match_plan set D_Away_RS_6 = '{D_Away_RS}', D_Away_ranking_6 = '{D_Away_ranking}', APPG = '{APPG}', AGDPG = {AGDPG} where match_id = {nowMatch_id}"
            mycursor.execute(sql)
            mydb.commit()
            print(f"                    match id {nowMatch_id} 's dynamic away ranking value is updated!")
            if target_status == "END":
                nIndex_Match_of_season += 1 
        print(f"                ---------- away team info End -------------------")
        print(f"     ------------ {league} - {season} - {moving_num} team id {team_id} End ---------------")
        ####################################### One team process End ##########################################################


    print(f"---------------{league}-{season}-{moving_num}-End-----------------")

def getRangeValue(RS):
    # print("   ---------------------------")
    if RS < 0:
        return 1
    if (RS >=0) & (RS < 1):
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

def main():
    insert_match_team_dynamic_ranking_8("eng-premier-league",       "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("esp-primera-division",     "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("bundesliga",               "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("ita-serie-a",              "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("fra-ligue-1",              "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("ned-eredivisie",           "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("aut-bundesliga",           "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("por-primeira-liga",        "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("gre-super-league",         "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("tur-sueperlig",            "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("nor-eliteserien",          "2022",      "END")
    insert_match_team_dynamic_ranking_8("swe-allsvenskan",          "2022",      "END")
    insert_match_team_dynamic_ranking_8("sui-super-league",         "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("den-superligaen",          "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("ukr-premyer-liga",         "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("bul-a-grupa",              "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("cze-1-fotbalova-liga",     "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("cro-1-hnl",                "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("hun-nb-i",                 "2021-2022", "END")
    insert_match_team_dynamic_ranking_8("srb-super-liga",           "2021-2022", "END")
    
    insert_match_team_dynamic_ranking_6("eng-premier-league",       "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("esp-primera-division",     "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("bundesliga",               "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("ita-serie-a",              "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("fra-ligue-1",              "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("ned-eredivisie",           "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("aut-bundesliga",           "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("por-primeira-liga",        "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("gre-super-league",         "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("tur-sueperlig",            "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("nor-eliteserien",          "2022",      "END")
    insert_match_team_dynamic_ranking_6("swe-allsvenskan",          "2022",      "END")
    insert_match_team_dynamic_ranking_6("sui-super-league",         "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("den-superligaen",          "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("ukr-premyer-liga",         "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("bul-a-grupa",              "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("cze-1-fotbalova-liga",     "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("cro-1-hnl",                "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("hun-nb-i",                 "2021-2022", "END")
    insert_match_team_dynamic_ranking_6("srb-super-liga",           "2021-2022", "END")

    insert_match_team_dynamic_ranking_8("eng-premier-league",       "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("esp-primera-division",     "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("bundesliga",               "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("ita-serie-a",              "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("fra-ligue-1",              "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("ned-eredivisie",           "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("aut-bundesliga",           "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("por-primeira-liga",        "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("gre-super-league",         "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("tur-sueperlig",            "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("nor-eliteserien",          "2022",      "LIVE")
    insert_match_team_dynamic_ranking_8("swe-allsvenskan",          "2022",      "LIVE")
    insert_match_team_dynamic_ranking_8("sui-super-league",         "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("den-superligaen",          "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("ukr-premyer-liga",         "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("bul-a-grupa",              "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("cze-1-fotbalova-liga",     "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("cro-1-hnl",                "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("hun-nb-i",                 "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_8("srb-super-liga",           "2021-2022", "LIVE")
    
    insert_match_team_dynamic_ranking_6("eng-premier-league",       "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("esp-primera-division",     "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("bundesliga",               "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("ita-serie-a",              "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("fra-ligue-1",              "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("ned-eredivisie",           "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("aut-bundesliga",           "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("por-primeira-liga",        "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("gre-super-league",         "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("tur-sueperlig",            "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("nor-eliteserien",          "2022",      "LIVE")
    insert_match_team_dynamic_ranking_6("swe-allsvenskan",          "2022",      "LIVE")
    insert_match_team_dynamic_ranking_6("sui-super-league",         "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("den-superligaen",          "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("ukr-premyer-liga",         "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("bul-a-grupa",              "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("cze-1-fotbalova-liga",     "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("cro-1-hnl",                "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("hun-nb-i",                 "2021-2022", "LIVE")
    insert_match_team_dynamic_ranking_6("srb-super-liga",           "2021-2022", "LIVE")
    
if __name__ == "__main__":
    main()
