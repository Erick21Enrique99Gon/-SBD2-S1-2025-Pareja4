import pandas as pd
import psycopg2

# Conexión a la base de datos
conn = psycopg2.connect(
    database="bd2_2s24",
    user="admin",
    host="localhost",
    password="root1234",
    port=5432,
    options="-c search_path=public"
)

df = pd.read_csv('./csv/game_summary.csv', sep=',')

cur = conn.cursor()

def safe(val):
    if pd.isna(val) or val == '':
        return 'NULL'
    return f"'{str(val).replace("'", "''")}'"

# Iterar sobre el DataFrame
for row in df.itertuples(index=False):
    try:
        game_date_est = safe(row.game_date_est)
        game_sequence =int(row.game_sequence ) if not pd.isna(row.game_sequence) else 'NULL'
        game_id = safe(row.game_id)
        game_status_id = row.game_status_id if not pd.isna(row.game_status_id) else 'NULL'
        game_status_text = safe(row.game_status_text)
        gamecode = safe(row.gamecode)
        home_team_id = safe(row.home_team_id)
        visitor_team_id = safe(row.visitor_team_id)
        season = safe(row.season)
        live_period = row.live_period if not pd.isna(row.live_period) else 'NULL'
        live_pc_time = safe(row.live_pc_time)
        nat_tv_broadcaster_abbreviation = safe(row.natl_tv_broadcaster_abbreviation)
        live_period_time_bcast = safe(row.live_period_time_bcast)
        wh_status = row.wh_status if not pd.isna(row.wh_status) else 'NULL'

        sql = f"""
        CALL insertar_game_summary(
            {game_date_est},
            {game_sequence},
            {game_id},
            {game_status_id},
            {game_status_text},
            {gamecode},
            {home_team_id},
            {visitor_team_id},
            {season},
            {live_period},
            {live_pc_time},
            {nat_tv_broadcaster_abbreviation},
            {live_period_time_bcast},
            {wh_status}
        );
        """
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"❌ Error con game_id={row.game_id}: {e}")
        conn.rollback()

cur.close()
conn.close()
print("✅ Carga de resumen de juegos completada.")
