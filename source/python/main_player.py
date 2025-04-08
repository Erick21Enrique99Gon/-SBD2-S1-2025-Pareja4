# import pandas as pd
# import psycopg2

# conn = psycopg2.connect(database = "bd2_2s24", 
#                         user = "admin", 
#                         host= 'localhost',
#                         password = "root1234",
#                         port = 5432,
#                         options="-c search_path=public")


# dataframeP = pd.read_csv("./csv/player.csv", sep=',' )


# dataframeP.drop(columns=['full_name'])

# cur = conn.cursor()
# cur.execute("""SELECT proname AS procedimiento, nspname AS esquema
# FROM pg_proc 
# JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
# WHERE prokind = 'p';""")
# result =cur.fetchone()[0] 

# print(result)
# for row in dataframeP.itertuples(index=False):
#     comando = "CALL insertar_player_table({},$${}$$,$${}$$,{});".format (row.id, row.first_name, row.last_name, row.is_active)
#     cur.execute(comando)

# conn.commit()

# cur.close()
# conn.close()

import pandas as pd
import psycopg2

conn = psycopg2.connect(database = "bd2_2s24", 
                        user = "admin", 
                        host= 'localhost',
                        password = "root1234",
                        port = 5432,
                        options="-c search_path=prueba2")


dataframeP = pd.read_csv("./csv/player.csv", sep=',' )


dataframeP.drop(columns=['full_name'])

cur = conn.cursor()
cur.execute("""SELECT proname AS procedimiento, nspname AS esquema
FROM pg_proc 
JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
WHERE prokind = 'p';""")
result =cur.fetchone()[0] 

print(result)
for row in dataframeP.itertuples(index=False):
    try:
        comando = "CALL insertar_player_table({},$${}$$,$${}$$,{});".format (row.id, row.first_name, row.last_name, row.is_active)
        cur.execute(comando)
    except Exception as e:
        print(f"❌ Error : {e}")
        conn.rollback()
        break

conn.commit()