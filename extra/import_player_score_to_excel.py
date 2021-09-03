import xlsxwriter
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="P@ssw0rd2021",
  database="soccer"
)
mycursor = mydb.cursor()

def get_import_to_excel():
    workbook = xlsxwriter.Workbook('Player last career.xlsx')
    worksheet = workbook.add_worksheet()
    data = ('At the beginning of the Season', 'player','team','Games started','Goals','GPGR')
    worksheet.write_row('A1', data)

    sql = f'SELECT  a.`player_id` AS id ,a.player_name AS NAME, b.team_name \
                    FROM playerlist AS a  \
                    INNER JOIN team_list AS b \
                    INNER JOIN match_team_player_info AS c ON c.`team_id`=b.`team_id` AND a.`player_id` = c.`player_id` \
                    INNER JOIN season_match_plan AS d ON c.`match_id` = d.`match_id` \
                    WHERE d.`season_id`= 5 AND d.`league_id`= 16 \
                    GROUP BY c.`player_id` ORDER BY b.team_name' 
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    rowcount = 2
    for each_player in myresult:
        id = each_player[0]
        print(id)
        player_name = each_player[1]
        
        sql = f'SELECT b.`season_title`,  c.`team_name`, a.`started`, a.`goals` FROM player_career AS a \
            INNER JOIN season AS b ON a.`season_id` = b.`season_id` \
            INNER JOIN team_list AS c ON a.team_id  = c.`team_id` \
            WHERE a.player_id = {id}'
        mycursor.execute(sql)
        wholeresult = mycursor.fetchall()
        if len(wholeresult):
            length = len(wholeresult)
            season = wholeresult[0][0]
            season_s = season.split()
            season = season_s[0]
            team = wholeresult[0][1]
            total_started = 0
            total_goals = 0
            for index in range(length-1, -1,-1):
                if wholeresult[index][0] == season:
                    break
                else:
                    total_started += wholeresult[index][2]
                    total_goals += wholeresult[index][3]
            if total_started != 0:
                GPGR = total_goals/total_started
            data = (season,player_name,team,total_started, total_goals,round(GPGR, 2))
            worksheet.write_row('A'+str(rowcount), data)
            print(f"----{rowcount-1} row added------------")
            rowcount += 1

    workbook.close()

get_import_to_excel()