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
mycol = mydb["victimas"]

comando = """WITH combined_results AS (
    SELECT 
        g.team_id_home AS team_id,
        'L' AS loss_location,  -- Indica que perdió como local
        g.team_id_away AS opponent_id,
        g.wl_away AS opponent_result,
        t.full_name AS team_name,
        t2.full_name AS opponent_name
    FROM game g
    JOIN team t ON t.id = g.team_id_home
    JOIN team t2 ON t2.id = g.team_id_away
    WHERE g.wl_home = 'L'
    
    UNION ALL
    
    -- Caso donde el equipo visitante perdió (wl_away = 'L')
    SELECT 
        g.team_id_away AS team_id,
        'A' AS loss_location,  -- Indica que perdió como visitante
        g.team_id_home AS opponent_id,
        g.wl_home AS opponent_result,
        t.full_name AS team_name,
        t2.full_name AS opponent_name
    FROM game g
    JOIN team t ON t.id = g.team_id_away
    JOIN team t2 ON t2.id = g.team_id_home
    WHERE g.wl_away = 'L'
)

SELECT 
    team_id,
    team_name,
    loss_location,
    opponent_id,
    opponent_name,
    opponent_result,
    COUNT(*) AS loss_count
FROM combined_results
GROUP BY team_id, team_name, loss_location, opponent_id, opponent_name, opponent_result
ORDER BY loss_count desc;"""

cur = conn.cursor()


cur.execute(comando)

results =cur.fetchall()

# print(results)

for result in results:
    victima ={
        "team_id":result[0],
        "team_name":result[1],
        "loss_location":result[2],
        "opponent_id":result[3],
        "opponent_name":result[4],
        "opponent_result":result[5],
        "loss_count":result[6]
    }
    print(victima)
    x=mycol.insert_one(victima)
    print(x.inserted_id)


conn.commit()

cur.close()
conn.close()