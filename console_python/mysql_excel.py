import xlsxwriter
import mysql.connector


def fetch_table_data(table_name):
    # The connect() constructor creates a connection to the MySQL server and returns a MySQLConnection object.
    cnx = mysql.connector.connect(
        host='localhost',
        database='Soccer',
        user='root',
        password=''
    )

    cursor = cnx.cursor()
    sql = "SELECT e.`league_title` AS League,d.`season_title`AS Season, DATE_FORMAT(a.`date`, '%Y-%m-%d') AS DATE ,  WEEK(a.date - INTERVAL 1 DAY )+1 AS WN, " \
    " CONCAT(b.`team_name` , ' :: ', c.`team_name` ) AS Game ,  " \
    "CONCAT(a.`total_home_score` , ' :: ', a.`total_away_score`) AS Score, " \
    "b.`team_name` AS Home_team_name, " \
    "a.`Home_TGPR` AS Home_TGPR, " \
    "f.`S_H_ranking` AS Static_HRank, " \
    "a.`D_Home_RS_8` AS Dynamic_HRS_8," \
    "a.`D_Home_ranking_8` AS Dynamic_HRank_8," \
    "a.`D_Home_RS_6` AS Dynamic_HRS_6," \
    "a.`D_Home_ranking_6` AS Dynamic_HRank_6," \
    "a.`home_team_score` AS Home_score," \
    "a.`home_team_strength` AS Home_strength," \
    "c.`team_name` AS Away_team_name," \
    "a.`Away_TGPR` AS Away_TGPR," \
    "g.`S_A_ranking` AS Static_ARank," \
    "a.`D_Away_RS_8` AS Dynamic_ARS_8," \
    "a.`D_Away_ranking_8` AS Dynamic_ARank_8," \
    "a.`D_Away_RS_6` AS Dynamic_ARS_6," \
    "a.`D_Away_ranking_6` AS Dynamic_ARank_6," \
    "a.`Away_team_score` AS Away_score," \
    "a.`Away_team_strength` AS Away_strength," \
    "CONCAT(f.`S_H_ranking`, ' v ' , g.`S_A_ranking`) AS staticRank," \
    "CONCAT (a.`D_Home_ranking_8`, ' v ', a.`D_Away_ranking_8`) AS DynamicRank_8," \
    "CONCAT (a.`D_Home_ranking_6`, ' v ', a.`D_Away_ranking_6`) AS DynamicRank_6," \
    "   h7.Home AS Aver_Home," \
    "   h7.Draw AS Aver_Draw, " \
    "   h7.Away AS Aver_Away, " \
    "   h7.Over2d5 AS Aver_Over, " \
    "   h7.Under2d5 AS Aver_Under , " \
    "   h8.Home AS Highest_Home," \
    "   h8.Draw AS Highest_Draw, " \
    "   h8.Away AS Highest_Away, " \
    "   h8.Over2d5 AS Highest_Over, " \
    "   h8.Under2d5 AS Highest_Under" \
    "   FROM season_match_plan AS a" \
    "   INNER JOIN team_list AS b ON a.`home_team_id` = b.`team_id`" \
    "   INNER JOIN team_list AS c ON a.`away_team_id` = c.`team_id`" \
    "   INNER JOIN season AS d ON a.`season_id` = d.`season_id`" \
    "    INNER JOIN league AS e ON a.`league_id` = e.`league_id`" \
    "   INNER JOIN season_league_team_info AS f ON a.`season_id` = f.`season_id` AND a.`home_team_id` = f.`team_id`" \
    "  INNER JOIN season_league_team_info AS g ON a.`season_id` = g.`season_id` AND a.`away_team_id` = g.`team_id`" \
    "  LEFT JOIN odds AS h7 ON a.`match_id` = h7.`match_id` AND h7.`bookmaker_id` = (SELECT id FROM bookmakers WHERE bookmaker_name = 'Average')" \
    "  LEFT JOIN odds AS h8 ON a.`match_id` = h8.`match_id` AND h8.`bookmaker_id` = (SELECT id FROM bookmakers WHERE bookmaker_name = 'Highest')" \
    "WHERE   (a.league_id = 1 OR a.league_id = 2 OR a.league_id = 3 OR a.league_id = 4 OR a.league_id = 5 OR a.league_id = 6 OR a.league_id = 7 OR a.league_id = 8 OR a.league_id = 9 OR a.league_id = 10 OR a.league_id = 11 OR a.league_id = 12 OR a.league_id = 13 OR a.league_id = 14" \
    " OR a.league_id = 15 OR a.league_id = 16 OR a.league_id = 17 OR a.league_id = 18 OR a.league_id = 19 OR a.league_id =20) AND a.status = 'END' ORDER BY a.`date`"

    cursor.execute(sql)

    header = [row[0] for row in cursor.description]

    rows = cursor.fetchall()

    # Closing connection
    cnx.close()

    return header, rows


def export(table_name):
    # Create an new Excel file and add a worksheet.
    workbook = xlsxwriter.Workbook(table_name + '.xlsx')
    worksheet = workbook.add_worksheet('MENU')

    # Create style for cells
    header_cell_format = workbook.add_format({'bold': True, 'border': True, 'bg_color': 'green'})
    body_cell_format = workbook.add_format({'border': True})

    header, rows = fetch_table_data(table_name)

    row_index = 0
    column_index = 0

    for column_name in header:
        worksheet.write(row_index, column_index, column_name, header_cell_format)
        column_index += 1

    row_index += 1
    for row in rows:
        column_index = 0
        for column in row:
            worksheet.write(row_index, column_index, column, body_cell_format)
            column_index += 1
        row_index += 1

    print(str(row_index) + ' rows written successfully to ' + workbook.filename)

    # Closing workbook
    workbook.close()


# Tables to be exported
export('Final_ranking_20201215')