import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="soccer"
)

mycursor = mydb.cursor()

def Acutal_prices_toexcel():
    
	Cols = [0,1,2,11,20, 38, 39, 40]
	df =  pd.read_excel("H:\workstation\Soccer+betting\Soccer_scrapping\console_python\\testing\\2019-2020 Back Testing.xlsx","Match_rank")
	rowcount = len(df)
	print("----------- start excel work -----------------")
	for row in range(0, rowcount):
		print(f"    ---------------- {row +1}th row data handloing ----------------")
		league = df.iat[row, 0]
		season = df.iat[row, 1]
		date = df.iat[row, 2]
		home_team_odd_name = df.iat[row, 11]
		away_team_odd_name = df.iat[row, 20]

		sql = f"SELECT wd_1, wd_x , wd_2 FROM season_match_plan AS a " \
			f"INNER JOIN season AS b ON a.`season_id` = b.`season_id` " \
			f"INNER JOIN team_list c ON a.`home_team_id` = c.`team_id` " \
			f"INNER JOIN team_list d ON a.`away_team_id` = d.`team_id` " \
			f"INNER JOIN league e ON a.`league_id` = d.`league_id` " \
			f"WHERE a.`date` = '{date}' AND b.`season_title` = '{season}' AND c.`team_name_odd` = '{home_team_odd_name}' AND d.`team_name_odd` = '{away_team_odd_name}' AND e.`league_title` = '{league}'"
		#print(sql)
		mycursor.execute(sql)
		myresult = mycursor.fetchall()
		if myresult[0][0] != "":
			df.iat[row,38] = myresult[0][0]
		if myresult[0][1] != "":
			df.iat[row,39] = myresult[0][1]
		if myresult[0][2] != "":
			df.iat[row,40] = myresult[0][2]
		
	df.to_excel("H:\workstation\Soccer+betting\Soccer_scrapping\console_python\\result_2019-2020 Back Testing.xlsx", "Match_rank", index=False, encoding = 'utf-16', header = True)
	print("----------- End excel work -----------------")
Acutal_prices_toexcel()
