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
mycol = mydb["inactive_players"]

comando = """select * from inactive_players ip ;"""

cur = conn.cursor()


cur.execute(comando)

results =cur.fetchall()

print(results)

for result in results:
    inactive_player ={
        "game_id":result[0],
        "player_id":result[1],
        "jersey_num":result[2],
        "team_id":result[3]
    }
    print(inactive_player)
    x=mycol.insert_one(inactive_player)
    print(x.inserted_id)


conn.commit()

cur.close()
conn.close()