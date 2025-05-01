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
mycol = mydb["ganadores"]

comando = """CALL EQUIPOS_MAS_GANADORES();
SELECT * FROM equipos_ganadores_resultado;"""

cur = conn.cursor()


cur.execute(comando)

results =cur.fetchall()
# print(results)
for result in results:

    anotador ={
        "team_id":result[0],
        "team_name":result[1],
        "win_count":result[2]
    }
    print(anotador)
    x=mycol.insert_one(anotador)
    print(x.inserted_id)


conn.commit()

cur.close()
conn.close()