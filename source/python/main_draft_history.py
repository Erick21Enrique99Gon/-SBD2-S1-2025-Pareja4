import pandas as pd
import psycopg2

conn = psycopg2.connect(database = "bd2_2s24", 
                        user = "admin", 
                        host= 'localhost',
                        password = "root1234",
                        port = 5432,
                        options="-c search_path=public")
dataframeDH = pd.read_csv("./csv/draft_history.csv", sep=',' )

cur = conn.cursor()

for row in dataframeDH.itertuples(index=False):
    fisrt = ''
    last = 'NULL'
    if( row.player_name.split(' ').__len__()>1):
        fisrt = row.player_name.split(' ')[0]
        last = row.player_name.split(' ')[1]
    else:
        fisrt = row.player_name.split(' ')[0]

    comando = "CALL insertar_draft_history_table({},$${}$$,$${}$$,{},{},{},{},$${}$$,{},$${}$$,$${}$$,$${}$$,$${}$$,$${}$$,{});".format (
       row.person_id,
       fisrt,
       last,
       row.season,
       row.round_number,
       row.round_pick,
       row.overall_pick,
       row.draft_type,
       row.team_id,
       row.team_city,
       row.team_name,
       row.team_abbreviation,
       'NULL' if pd.isna(row.organization) else row.organization,
       'NULL' if pd.isna(row.organization_type) else row.organization_type,
       row.player_profile_flag
       )
    print(comando)
    cur.execute(comando)

conn.commit()

cur.close()
conn.close()