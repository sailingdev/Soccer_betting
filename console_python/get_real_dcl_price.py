import requests
import datetime
import argparse
import mysql.connector
from mysql.connector import Error
import certifi
import urllib3
from collections import defaultdict
#################################################################
# Inserting Reference of Dynamic with Strength and league column in season_match_plan table
#################################################################

mydb = mysql.connector.connect(
	host="localhost",
	user="root",
	passwd="P@ssw0rd2021",
	database="soccer",
	buffered=True
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
		"2021-2022": 857,	
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
		"2021": 844
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

def get_Team_Cream_text(team_id , season_id):
	sql = f"SELECT cream_status from cream_team_list where team_id = {team_id} and season_id = {season_id}"
	
	mycursor.execute(sql)
	result = mycursor.fetchone()
	if result:
		if result[0] =="":
			return 'Non-Cream'
		else:
			return result[0]
	else:
		return 'Non-Cream'

def get_dcl_source_list(c_weeknumber):
	sql = f"SELECT b.league_title, home_team_id, away_team_id, D_Home_ranking_8, D_Away_ranking_8, total_home_score, total_away_score, season_id FROM season_match_plan as a INNER JOIN league as b on a.league_id = b.league_id WHERE STATUS = 'END' AND (a.season_id < 19 OR a.season_id = 799 OR a.season_id = 64) AND a.c_WN < {c_weeknumber}"
	mycursor.execute(sql)
	matches = mycursor.fetchall()
	source_list = []
	for match in matches:
		league_title = match[0]
		home_cream_text = get_Team_Cream_text(match[1], match[7])
		away_cream_text = get_Team_Cream_text(match[2], match[7])

		dcl_refer_txt = league_title + home_cream_text + ' v ' + away_cream_text + str(match[3]) + ' v ' + str(match[4])
		elem_list = []
		
		if match[5] > match[6]:
			val = "H"
			elem_list = [dcl_refer_txt, 1, 0 ,0]
		elif match[5] == match[6]:
			val = "D"
			elem_list = [dcl_refer_txt, 0, 1 ,0]
		else:
			val = "A"
			elem_list = [dcl_refer_txt, 0, 0 ,1]
		print(elem_list)
		source_list.append(elem_list)

	print("-----length of source list is ", len(source_list))
	return source_list

def insert_real_prcie_to_realpriceTable(C_weeknumber):
	
	##################### get all Dynamic Cream League combination################################
	source_list = get_dcl_source_list(C_weeknumber)

	##################### get merged dcl combination ###########################
	merged = defaultdict(lambda: [0, 0, 0])
	for refer_text, *values in source_list:               
		merged[refer_text] = [sum(i) for i in zip(values, merged[refer_text])]
	print("-----length of grouped list is ", len(merged))
	
	##################### inset dcl table ######################################
	count= 0
	for item in merged.items():
		refer = item[0]
		elem = item[1]
		total = elem[0] + elem[1] + elem[2]
		if total > 9:
			sql = f"insert into real_price_dcl (refer,c_week_number, total, H, D, A, H_price, D_price, A_price) " \
				f"values('{refer}',  {C_weeknumber}, {total} , {elem[0]}, {elem[1]}, {elem[2]}, {round(total / elem[0] , 2) if elem[0] > 0 else 0} ,{round(total / elem[1] , 2) if elem[1] > 0 else 0},{round(total / elem[2] , 2) if elem[2] > 0 else 0} )"
			mycursor.execute(sql);
			mydb.commit();
			count += 1
			print(f"       -week {C_weeknumber} insert item - {refer}")
	print(f" -week {C_weeknumber} inserted count is {count}")
	############################################################################
	
def update_real_price_id_toSeasonMatchPlanTable(week_number):
	print(f" - W{week_number} start !")
	count = 0
	sql = f"SELECT match_id, b.league_title, home_team_id, away_team_id, a.D_Home_ranking_8, " \
    f"a.D_Away_ranking_8 , season_id FROM season_match_plan AS a INNER JOIN league AS b ON a.league_id = b.league_id WHERE (STATUS = 'END' or STATUS = 'LIVE') AND c_WN = {week_number}"

	#sql = f"SELECT match_id, b.league_title, home_team_id, away_team_id, a.D_Home_ranking_8, a.D_Away_ranking_8 , c.season_title FROM season_match_plan AS a INNER JOIN league AS b ON a.league_id = b.league_id INNER JOIN season AS c ON a.season_id = c.season_id WHERE STATUS = 'END' AND c_WN = {week_number}"

	mycursor.execute(sql)
	results = mycursor.fetchall()
	if len(results) == 0:
		print("  There are not available games !")
	for result in results:
		league_title = result[1]
		match_id = result[0]
		home_cream_text = get_Team_Cream_text(result[2], result[6])
		away_cream_text = get_Team_Cream_text(result[3], result[6])
		dcl_refer_txt = league_title + home_cream_text + ' v ' + away_cream_text + str(result[4]) + ' v ' + str(result[5])

		query_sql = f"select id from real_price_dcl where refer = '{dcl_refer_txt}' and c_week_number = {week_number}"
		# print(query_sql)
		mycursor.execute(query_sql)
		price_id = mycursor.fetchone()

		if price_id:
			update_sql = f"update season_match_plan set DCL_refer_id = '{price_id[0]}' where match_id = {match_id}"
			mycursor.execute(update_sql)
			mydb.commit()
			print(f"  W{week_number} - update one match id {match_id}")
			count += 1

	print(f" - W{week_number} - updated {count} : END !")

def get_realprice_toRealPriceTable_perweek(weeknumber):
	# for C_weeknumber in range(275, 546):                                  # 546 = 2020-06-14 , 578 = 2021-01-21
	#     insert_real_prcie_to_realpriceTable(C_weeknumber)
    insert_real_prcie_to_realpriceTable(weeknumber)							#  param shoulb be current continuous week.

def matching_realpriceid_toSeasonMatchPlanColumn(weeknumber):		    
	update_real_price_id_toSeasonMatchPlanTable(weeknumber)					#  param shoulb be current continuous week.

def main():
	weeknumber = 615
	get_realprice_toRealPriceTable_perweek(weeknumber)
	matching_realpriceid_toSeasonMatchPlanColumn(weeknumber)
	
if __name__ == "__main__":
	main()

