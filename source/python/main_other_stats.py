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

df = pd.read_csv('./csv/other_stats.csv', sep=',')

cur = conn.cursor()

def val(v):
    return 'NULL' if pd.isna(v) or v == '' else f"'{str(v).replace("'", "''")}'"

def num(v):
    return 'NULL' if pd.isna(v) or v == '' else str(int(v))

for row in df.itertuples(index=False):
    try:
        sql = f"""
        CALL insertar_other_stats(
            {val(row.game_id)},
            {num(row.league_id)},
            {val(row.team_id_home)},
            {val(row.team_abbreviation_home)},
            {val(row.team_city_home)},
            {num(row.pts_paint_home)},
            {num(row.pts_2nd_chance_home)},
            {num(row.pts_fb_home)},
            {num(row.largest_lead_home)},
            {num(row.lead_changes)},
            {num(row.times_tied)},
            {num(row.team_turnovers_home)},
            {num(row.total_turnovers_home)},
            {num(row.team_rebounds_home)},
            {num(row.pts_off_to_home)},
            {val(row.team_id_away)},
            {val(row.team_abbreviation_away)},
            {val(row.team_city_away)},
            {num(row.pts_paint_away)},
            {num(row.pts_2nd_chance_away)},
            {num(row.pts_fb_away)},
            {num(row.largest_lead_away)},
            {num(row.team_turnovers_away)},
            {num(row.total_turnovers_away)},
            {num(row.team_rebounds_away)},
            {num(row.pts_off_to_away)}
        );
        """
        
        print(sql)
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"❌ Error con game_id={row.game_id}: {e}")
        conn.rollback()

cur.close()
conn.close()
print("✅ Carga de estadísticas de juego completada.")