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
mycol = mydb["other_stats"]

comando = """select * from other_stats os ;"""

cur = conn.cursor()


cur.execute(comando)

results =cur.fetchall()

print(results)

for result in results:
    other_stat ={
        	"game_id":result[0],
            "league_id": result[1],
            "team_id_home": result[2],
            "pts_paint_home": result[3],
            "pts_2nd_chance_home": result[4],
            "pts_fb_home": result[5],
            "largest_lead_home": result[6],
            "lead_changes": result[7],
            "times_tied": result[8],
            "team_turnovers_home": result[9],
            "total_turnovers_home": result[10],
            "team_rebounds_home": result[11],
            "pts_off_to_home": result[12],
            "team_id_away": result[13],
            "pts_paint_away": result[14],
            "pts_2nd_chance_away": result[15],
            "pts_fb_away": result[16],
            "largest_lead_away": result[17],
            "team_turnovers_away": result[18],
            "total_turnovers_away": result[19],
            "team_rebounds_away": result[20],
            "pts_off_to_away": result[21]
    }
    print(other_stat)
    x=mycol.insert_one(other_stat)
    print(x.inserted_id)


conn.commit()

cur.close()
conn.close()