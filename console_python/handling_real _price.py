import requests
from bs4 import BeautifulSoup
import argparse
import mysql.connector
import certifi
import urllib3

http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

################################################################
# Inserting Reference of Dynamic with Strength and league column in season_match_plan table
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
	}
	return switcher.get(argument, "null")

season_list = []

def insert_DSLReferColumn():
	#sql = f"SELECT league_id, home_team_strength,D_Home_ranking_8, D_Away_ranking_8, away_team_strength , match_id from season_match_plan where status = 'END'";
	#mycursor.execute(sql)
	#myresult = mycursor.fetchall()
	#for result in myresult:
	#	league_id = result[0];  		
	#	sub_sql = f'select league_title from league where league_id = {league_id}';
	#	mycursor.execute(sub_sql);
	#	league_title= mycursor.fetchone();
		
	#	if ((result[1] != None ) & (result[2] != None ) & (result[3] != None ) & (result[4] != None )):
	#		DSLcontext = league_title[0] + result[1] + result[2] + " v " + result[3] + result[4];
	#		print(f"match id  - {result[5]} : {DSLcontext}");
	#		sub_sql = f'update season_match_plan set DSL_refer = "{DSLcontext}" where match_id = {result[5]}';
	#		mycursor.execute(sub_sql);
	#		mydb.commit();

	######################### inserted DSL text on season_match_plan ###################################
	
	#sql = 'SELECT DSL_refer FROM season_match_plan WHERE DSL_refer != "" GROUP BY DSL_refer';
	#mycursor.execute(sql);
	#myresult = mycursor.fetchall();
	#for result in myresult:
	#	subsql = f'INSERT into real_price_dsl (DSL_refer) values ("{result[0]}")';
	#	mycursor.execute(subsql);
	#	mydb.commit();
	#	print("  inserted one row");

	######################### inserted DSL id on price table ###################################

	sql = f"SELECT  match_id, DSL_refer from season_match_plan where DSL_refer != ''";
	mycursor.execute(sql)
	myresult = mycursor.fetchall()
	for result in myresult:
		DSL_refer = result[1];  		
		sub_sql = f'SELECT id  from real_price_dsl where DSL_refer = "{DSL_refer}"';
		mycursor.execute(sub_sql);
		DSL_refer_id= mycursor.fetchone();
		
		if id:
			print(f"match id  - {result[0]} ");
			sub_sql = f'update season_match_plan set DSL_refer_id = {DSL_refer_id[0]} where match_id = {result[0]}';
			mycursor.execute(sub_sql);
			mydb.commit();

	######################### update DSL id on season match plan ###################################
	

def insert_pricetable():
	sql = "SELECT count(*) from real_price_dsl";
	mycursor.execute(sql);
	result = mycursor.fetchone();
	id_count = result[0];
	for id in range(1, id_count+1):
		#print(id);
		sql = f"SELECT COUNT(*) as total,  \
			COUNT(CASE WHEN total_home_score + total_away_score > 2.5 THEN 1 END)AS O,  \
			COUNT(CASE WHEN total_home_score + total_away_score < 2.5 THEN 1 END)AS U,  \
			COUNT(CASE WHEN total_home_score > total_away_score THEN 1 END)AS H,  \
			COUNT(CASE WHEN total_home_score = total_away_score THEN 1 END)AS D,  \
			COUNT(CASE WHEN total_home_score < total_away_score THEN 1 END)AS A  \
			FROM season_match_plan   \
			WHERE DSL_refer_id  = {id} AND season_id <= 20";
		mycursor.execute(sql);
		result = mycursor.fetchone();
		if result:
			total = result[0];
			O_2p5 = result[1];
			U_2p5 = result[2];
			H = result[3];
			D = result[4];
			A = result[5];

			if total <= 6:
				sub_sql = f'UPDATE real_price_dsl set O_2p5 = {O_2p5}, U_2p5 = {U_2p5} , H = {H} , D = {D} , A = {A} , total = {total},	\
				O_2p5_price_6 = "No price" ,  O_2p5_price_8 = "No price" , O_2p5_price_10 = "No price" , 	\
				U_2p5_price_6 = "No price" ,  U_2p5_price_8 = "No price" , U_2p5_price_10 = "No price" , 	\
				H_price_6 = "No price" ,  H_price_8 = "No price" , H_price_10 = "No price" , 	\
				D_price_6 = "No price" ,  D_price_8 = "No price" , D_price_10 = "No price" , 	\
				A_price_6 = "No price" ,  A_price_8 = "No price" , A_price_10 = "No price" 	\
				where id = {id}';
			elif (total > 6) & (total <= 8):
				sub_sql = f'UPDATE real_price_dsl set O_2p5 = {O_2p5}, U_2p5 = {U_2p5} , H = {H} , D = {D} , A = {A} ,total = {total},	\
				O_2p5_price_6 = {round(total / O_2p5 , 2) if O_2p5 > 0 else 0} ,  O_2p5_price_8 = "No price" , O_2p5_price_10 = "No price" , 	\
				U_2p5_price_6 = {round(total / U_2p5 , 2) if U_2p5 > 0 else 0} ,  U_2p5_price_8 = "No price" , U_2p5_price_10 = "No price" , 	\
				H_price_6 = {round(total / H , 2) if H > 0 else 0} ,  H_price_8 = "No price" , H_price_10 = "No price" , 	\
				D_price_6 = {round(total / D , 2) if D > 0 else 0} ,  D_price_8 = "No price" , D_price_10 = "No price" , 	\
				A_price_6 = {round(total / A , 2) if A > 0 else 0},  A_price_8 = "No price" , A_price_10 = "No price" 	\
				where id = {id}';
			elif (total > 8) & (total <=  10):
				sub_sql = f'UPDATE real_price_dsl set O_2p5 = {O_2p5}, U_2p5 = {U_2p5} , H = {H} , D = {D} , A = {A} , total = {total},	\
				O_2p5_price_6 = {round(total / O_2p5 , 2) if O_2p5 > 0 else 0} ,  O_2p5_price_8 = {round(total / O_2p5 , 2) if O_2p5 > 0 else 0} , O_2p5_price_10 = "No price" , 	\
				U_2p5_price_6 = {round(total / U_2p5 , 2) if U_2p5 > 0 else 0} ,  U_2p5_price_8 = {round(total / U_2p5 , 2) if U_2p5 > 0 else 0} , U_2p5_price_10 = "No price" , 	\
				H_price_6 = {round(total / H , 2) if H > 0 else 0} ,  H_price_8 = {round(total / H , 2) if H > 0 else 0} , H_price_10 = "No price" , 	\
				D_price_6 = {round(total / D , 2) if D > 0 else 0} ,  D_price_8 = {round(total / D , 2) if D > 0 else 0} , D_price_10 = "No price" , 	\
				A_price_6 = {round(total / A , 2) if A > 0 else 0},  A_price_8 =  {round(total / A , 2) if A > 0 else 0} , A_price_10 = "No price" 	\
				where id = {id}';
			elif total > 10:
				sub_sql = f'UPDATE real_price_dsl set O_2p5 = {O_2p5}, U_2p5 = {U_2p5} , H = {H} , D = {D} , A = {A} ,total = {total},	\
				O_2p5_price_6 = {round(total / O_2p5 , 2) if O_2p5 > 0 else 0} ,  O_2p5_price_8 = {round(total / O_2p5 , 2) if O_2p5 > 0 else 0} , O_2p5_price_10 ={round(total / O_2p5 , 2) if O_2p5 > 0 else 0} , 	\
				U_2p5_price_6 = {round(total / U_2p5 , 2) if U_2p5 > 0 else 0} ,  U_2p5_price_8 = {round(total / U_2p5 , 2) if U_2p5 > 0 else 0} , U_2p5_price_10 = {round(total / U_2p5 , 2) if U_2p5 > 0 else 0} , 	\
				H_price_6 = {round(total / H , 2)  if H > 0 else 0} ,  H_price_8 = {round(total / H , 2)  if H > 0 else 0} , H_price_10 = {round(total / H , 2)  if H > 0 else 0} , 	\
				D_price_6 = {round(total / D , 2)  if D > 0 else 0} ,  D_price_8 = {round(total / D , 2) if D > 0 else 0} , D_price_10 = {round(total / D , 2) if D > 0 else 0} , 	\
				A_price_6 = {round(total / A , 2) if A > 0 else 0},  A_price_8 =  {round(total / A , 2) if A > 0 else 0} , A_price_10 = {round(total / A , 2) if A > 0 else 0} 	\
				where id = {id}';

			
			mycursor.execute(sub_sql);
			mydb.commit();
			print(f'inserted id  - {id}')
				

#insert_DSLReferColumn();
insert_pricetable();