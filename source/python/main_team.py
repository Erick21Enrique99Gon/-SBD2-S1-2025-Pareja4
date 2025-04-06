import pandas as pd
import psycopg2

conn = psycopg2.connect(database = "bd2_2s24", 
                        user = "admin", 
                        host= 'localhost',
                        password = "root1234",
                        port = 5432,
                        options="-c search_path=public")

dataframeT = pd.read_csv("./csv/team.csv", sep=',' )

cur = conn.cursor()
cur.execute("""SELECT proname AS procedimiento, nspname AS esquema
FROM pg_proc 
JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
WHERE prokind = 'p';""")
result =cur.fetchone()[0] 

print(result)
for row in dataframeT.itertuples(index=False):
    comando = "CALL insertar_team_table({},$${}$$,$${}$$,$${}$$,$${}$$,$${}$$,{});".format (
        row.id,
        row.full_name,
        row.abbreviation,
        row.nickname,
        row.city,
        row.state,
        row.year_founded)
    print(comando)
    cur.execute(comando)

conn.commit()

cur.close()
conn.close()