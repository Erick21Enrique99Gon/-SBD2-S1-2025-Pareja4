import pymongo

myclient = pymongo.MongoClient("mongodb://root:admin123@localhost:27017/")
import pandas as pd
import psycopg2
conn = psycopg2.connect(database = "bd2_2s24", 
                        user = "admin", 
                        host= 'localhost',
                        password = "root1234",
                        port = 5432,
                        options="-c search_path=public")
mydb = myclient["local"]
mycol = mydb["line_scores"]

comando = """select * from line_score ip ;"""

cur = conn.cursor()


cur.execute(comando)

results =cur.fetchall()
print(results)
for result in results:
    line_score ={
        	"game_sequence" : result[0],
            "game_id" : result[1],
            "team_id_home" : result[2],
            "team_wins_losses_home" : result[3],
            "pts_qtr1_home" : result[4],
            "pts_qtr2_home" : result[5],
            "pts_qtr3_home" : result[6],
            "pts_qtr4_home" : result[7],
            "pts_ot1_home" : result[8],
            "pts_ot2_home" : result[9],
            "pts_ot3_home" : result[10],
            "pts_ot4_home" : result[11],
            "pts_ot5_home" : result[12],
            "pts_ot6_home" : result[13],
            "pts_ot7_home" : result[14],
            "pts_ot8_home" : result[15],
            "pts_ot9_home" : result[16],
            "pts_ot10_home" : result[17],
            "pts_home" : result[18],
            "team_id_away" : result[19],
            "team_wins_losses_away" : result[20],
            "pts_qtr1_away" : result[21],
            "pts_qtr2_away" : result[22],
            "pts_qtr3_away" : result[23],
            "pts_qtr4_away" : result[24],
            "pts_ot1_away" : result[25],
            "pts_ot2_away" : result[26],
            "pts_ot3_away" : result[27],
            "pts_ot4_away" : result[28],
            "pts_ot5_away" : result[29],
            "pts_ot6_away" : result[30],
            "pts_ot7_away" : result[31],
            "pts_ot8_away" : result[32],
            "pts_ot9_away" : result[33],
            "pts_ot10_away" : result[34],
            "pts_away" : result[35]
    }
    print(line_score)
    x=mycol.insert_one(line_score)
    print(x.inserted_id)


conn.commit()

cur.close()
conn.close()