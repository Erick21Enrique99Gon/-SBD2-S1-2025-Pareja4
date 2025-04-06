import pandas as pd
import psycopg2

conn = psycopg2.connect(database = "bd2_2s24", 
                        user = "admin", 
                        host= 'localhost',
                        password = "root1234",
                        port = 5432,
                        options="-c search_path=public")

dataframeTD = pd.read_csv("./csv/team_details.csv", sep=',' )

cur = conn.cursor()

for row in dataframeTD.itertuples(index=False):
    comando = "CALL insertar_team_details_table({},$${}$$,$${}$$,{},$${}$$,$${}$$,{},$${}$$,$${}$$,$${}$$,$${}$$,$${}$$,$${}$$,$${}$$);".format (
        row.team_id,
        row.abbreviation,
        row.nickname,
        row.yearfounded,
        row.city,
        row.arena,
        'NULL' if pd.isna(row.arenacapacity) else row.arenacapacity,
        row.owner,
        row.generalmanager,
        row.headcoach,
        row.dleagueaffiliation,
        row.facebook,
        row.instagram,
        row.twitter)
    print(comando)
    cur.execute(comando)

conn.commit()

cur.close()
conn.close()