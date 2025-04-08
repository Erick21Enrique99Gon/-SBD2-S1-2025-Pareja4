import pandas as pd
import psycopg2

# Siempre devolver string (como texto) o 'NULL'
def val(v):
    return 'NULL' if pd.isna(v) or v == '' else f"'{str(v).replace("'", "''")}'"

def num(v):
    return 'NULL' if pd.isna(v) or v == '' else f"'{int(v)}'"

def num_float(v):
    return 'NULL' if pd.isna(v) or v == '' else f"'{float(v)}'"

# Conexión
conn = psycopg2.connect(
    database="bd2_2s24",
    user="admin",
    host="localhost",
    password="root1234",
    port=5432,
    options="-c search_path=public"
)
cur = conn.cursor()

df = pd.read_csv('./csv/line_score.csv', sep=',')

for row in df.itertuples(index=False):
    try:
        query = f"""
        CALL insertar_line_score(
            {val(row.game_id)},
            {num(row.game_sequence)},
            {val(row.team_id_home)},
            {val(row.team_abbreviation_home)},
            {val(row.team_city_name_home)},
            {val(row.team_nickname_home)},
            {val(row.team_wins_losses_home)},
            {num(row.pts_qtr1_home)},
            {num(row.pts_qtr2_home)},
            {num(row.pts_qtr3_home)},
            {num(row.pts_qtr4_home)},
            {num(row.pts_ot1_home)},
            {num(row.pts_ot2_home)},
            {num(row.pts_ot3_home)},
            {num(row.pts_ot4_home)},
            {num(row.pts_ot5_home)},
            {num(row.pts_ot6_home)},
            {num(row.pts_ot7_home)},
            {num(row.pts_ot8_home)},
            {num(row.pts_ot9_home)},
            {num(row.pts_ot10_home)},
            {num_float(row.pts_home)},
            {val(row.team_id_away)},
            {val(row.team_abbreviation_away)},
            {val(row.team_city_name_away)},
            {val(row.team_nickname_away)},
            {val(row.team_wins_losses_away)},
            {num(row.pts_qtr1_away)},
            {num(row.pts_qtr2_away)},
            {num(row.pts_qtr3_away)},
            {num(row.pts_qtr4_away)},
            {num(row.pts_ot1_away)},
            {num(row.pts_ot2_away)},
            {num(row.pts_ot3_away)},
            {num(row.pts_ot4_away)},
            {num(row.pts_ot5_away)},
            {num(row.pts_ot6_away)},
            {num(row.pts_ot7_away)},
            {num(row.pts_ot8_away)},
            {num(row.pts_ot9_away)},
            {num(row.pts_ot10_away)},
            {num_float(row.pts_away)}
        );
        """
        print(query)
        cur.execute(query)
    except Exception as e:
        print(f"[ERROR] game_id={row.game_id} -> {e}")
        conn.rollback()
        break
    else:
        conn.commit()

cur.close()
conn.close()