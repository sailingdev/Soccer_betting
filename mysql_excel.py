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
    "f.`S_A_ranking` AS Static_ARank," \
    "a.`D_Away_RS_8` AS Dynamic_ARS_8," \
    "a.`D_Away_ranking_8` AS Dynamic_ARank_8," \
    "a.`D_Away_RS_6` AS Dynamic_ARS_6," \
    "a.`D_Away_ranking_6` AS Dynamic_ARank_6," \
    "a.`Away_team_score` AS Away_score," \
    "a.`Away_team_strength` AS Away_strength," \
    "CONCAT(f.`S_H_ranking`, ' v ' , f.`S_A_ranking`) AS staticRank," \
    "CONCAT (a.`D_Home_ranking_8`, ' v ', a.`D_Away_ranking_8`) AS DynamicRank_8," \
    "CONCAT (a.`D_Home_ranking_6`, ' v ', a.`D_Away_ranking_6`) AS DynamicRank_6," \
    "h1.Home AS bet365_Home," \
    "h1.Draw AS bet365_Draw, " \
    "h1.Away AS bet365_Away, " \
    "   h1.Over2d5 AS bet365_Over, " \
    "  h1.Under2d5 AS bet365_Under," \
    "   h2.Home AS Betfair_Home," \
    "   h2.Draw AS Betfair_Draw, " \
    "   h2.Away AS Betfair_Away, " \
    "   h2.Over2d5 AS Betfair_Over, " \
    "  h2.Under2d5 AS Betfair_Under," \
    "   h3.Home AS Dafabet_Home," \
    "   h3.Draw AS Dafabet_Draw, " \
    "   h3.Away AS Dafabet_Away, " \
    "   h3.Over2d5 AS Dafabet_Over, " \
    "   h3.Under2d5 AS Dafabet_Under," \
    "  h4.Home AS Pncl_Home," \
    "   h4.Draw AS Pncl_Draw, " \
    "   h4.Away AS Pncl_Away, " \
    "   h4.Over2d5 AS Pncl_Over, " \
    "  h4.Under2d5 AS Pncl_Under," \
    "    h5.Home AS Sbo_Home," \
    "   h5.Draw AS Sbo_Draw, " \
    "   h5.Away AS Sbo_Away, " \
    "   h5.Over2d5 AS Sbo_Over, " \
    "   h5.Under2d5 AS Sbo_Under," \
    "   h6.Home AS Unibet_Home," \
    "   h6.Draw AS Unibet_Draw, " \
    "   h6.Away AS Unibet_Away, " \
    "   h6.Over2d5 AS Unibet_Over, " \
    "   h6.Under2d5 AS Unibet_Under" \
    "   FROM season_match_plan AS a" \
    "   INNER JOIN team_list AS b ON a.`home_team_id` = b.`team_id`" \
    "   INNER JOIN team_list AS c ON a.`away_team_id` = c.`team_id`" \
    "   INNER JOIN season AS d ON a.`season_id` = d.`season_id`" \
    "    INNER JOIN league AS e ON a.`league_id` = e.`league_id`" \
    "   INNER JOIN season_league_team_info AS f ON a.`season_id` = f.`season_id` AND a.`home_team_id` = f.`team_id`" \
    "  INNER JOIN season_league_team_info AS g ON a.`season_id` = g.`season_id` AND a.`away_team_id` = g.`team_id`" \
    "   LEFT JOIN odds AS h1 ON a.`match_id` = h1.`match_id` AND h1.`bookmaker_id` = (SELECT id FROM bookmakers WHERE bookmaker_name = 'bet365')" \
    "  LEFT JOIN odds AS h2 ON a.`match_id` = h2.`match_id` AND h2.`bookmaker_id` = (SELECT id FROM bookmakers WHERE bookmaker_name = 'Betfair')" \
    "   LEFT JOIN odds AS h3 ON a.`match_id` = h3.`match_id` AND h3.`bookmaker_id` = (SELECT id FROM bookmakers WHERE bookmaker_name = 'Dafabet')" \
    "  LEFT JOIN odds AS h4 ON a.`match_id` = h4.`match_id` AND h4.`bookmaker_id` = (SELECT id FROM bookmakers WHERE bookmaker_name = 'Pncl')" \
    "  LEFT JOIN odds AS h5 ON a.`match_id` = h5.`match_id` AND h5.`bookmaker_id` = (SELECT id FROM bookmakers WHERE bookmaker_name = 'Sbo')" \
    "  LEFT JOIN odds AS h6 ON a.`match_id` = h6.`match_id` AND h6.`bookmaker_id` = (SELECT id FROM bookmakers WHERE bookmaker_name = 'Unibet')" \
    "WHERE   (a.league_id = 1 OR a.league_id = 2 OR a.league_id = 3 OR a.league_id = 4 OR a.league_id = 5 OR a.league_id = 6 OR a.league_id = 7 OR a.league_id = 8 OR a.league_id = 9 OR a.league_id = 10 OR a.league_id = 11 OR a.league_id = 12 OR a.league_id = 13 OR a.league_id = 14" \
    " OR a.league_id = 15 OR a.league_id = 16 OR a.league_id = 17 OR a.league_id = 18 OR a.league_id = 19 OR a.league_id =20) AND  a.status = 'END' ORDER BY a.`date`"

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
export('Full_ranking')