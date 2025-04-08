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

df = pd.read_csv('./csv/inactive_players.csv', sep=',')

cur = conn.cursor()

def safe(val):
    if pd.isna(val) or val == '':
        return 'NULL'
    return f"'{str(val).replace("'", "''")}'"

# Iterar sobre el DataFrame
for row in df.itertuples(index=False):
    try:
        game_id = safe(row.game_id)
        player_id = safe(row.player_id)
        first_name = safe(row.first_name)
        last_name = safe(row.last_name)
        jersey_num = safe(row.jersey_num)
        team_id = safe(row.team_id)
        team_city = safe(row.team_city)
        team_name = safe(row.team_name)
        team_abbreviation = safe(row.team_abbreviation)

        sql = f"""
        CALL insertar_inactive_player(
            {game_id},
            {player_id},
            {first_name},
            {last_name},
            {jersey_num},
            {team_id},
            {team_city},
            {team_name},
            {team_abbreviation}
        );
        """
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"❌ Error con game_id={row.game_id}, player_id={row.player_id}: {e}")
        conn.rollback()

cur.close()
conn.close()
print("✅ Carga de jugadores inactivos completada.")