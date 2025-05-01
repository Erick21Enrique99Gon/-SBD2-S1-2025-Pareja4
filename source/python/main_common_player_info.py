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

# Leer el CSV source\python\csv\common_player_info.csv
dataframe = pd.read_csv("./csv/common_player_info.csv", sep=',')

cur = conn.cursor()

# Función para sanitizar cadenas con comillas y nulos
def safe(val):
    if pd.isna(val):
        return 'NULL'
    else:
        escaped = str(val).replace("'", "''")  # Escape de comillas simples
        return f"'{escaped}'"

for row in dataframe.itertuples(index=False):
    comando = f"""
    CALL insertar_common_player_info(
        {row.person_id},
        {safe(row.first_name)},
        {safe(row.last_name)},
        {safe(row.display_fi_last)},
        {safe(row.player_slug)},
        {safe(row.birthdate)},
        {safe(row.school)},
        {safe(row.country)},
        {safe(row.last_affiliation)},
        {safe(row.height)},
        {safe(row.weight)},
        {row.season_exp if not pd.isna(row.season_exp) else 'NULL'},
        {safe(row.jersey)},
        {safe(row.position)},
        {safe(row.rosterstatus)},
        {safe(row.games_played_current_season_flag)},
        {row.team_id if not pd.isna(row.team_id) else 'NULL'},
        {safe(row.team_name)},
        {safe(row.team_abbreviation)},
        {safe(row.team_code)},
        {safe(row.team_city)},
        {safe(row.playercode)},
        {row.from_year if not pd.isna(row.from_year) else 'NULL'},
        {row.to_year if not pd.isna(row.to_year) else 'NULL'},
        {safe(row.dleague_flag)},
        {safe(row.nba_flag)},
        {safe(row.games_played_flag)},
        {safe(row.draft_year)},
        {safe(row.draft_round)},
        {safe(row.draft_number)},
        {safe(row.greatest_75_flag)}
    );
    """
    try:
        print(comando)
        cur.execute(comando)
    except Exception as e:
        print(f"Error en fila con person_id={row.person_id}: {e}")
        conn.rollback()  # Para evitar que la conexión se bloquee
    else:
        conn.commit()

# Cerrar cursor y conexión
cur.close()
conn.close()