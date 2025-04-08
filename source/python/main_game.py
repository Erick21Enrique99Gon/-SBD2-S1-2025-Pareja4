import pandas as pd
import psycopg2

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

df = pd.read_csv('./csv/game.csv', sep=',')

for row in df.itertuples(index=False):
    try:
        query = f"""
        CALL insertar_juego(
            {val(row.game_id)},
            {val(row.team_id_home)},
            {val(row.team_abbreviation_home)},
            {val(row.team_name_home)},
            {val(row.team_id_away)},
            {val(row.team_abbreviation_away)},
            {val(row.team_name_away)},
            {val(row.season_id)},
            {val(row.game_date)},
            {val(row.matchup_home)},
            {val(row.wl_home)},
            {num(row.min)},
            {num_float(row.fgm_home)},
            {num_float(row.fga_home)},
            {num_float(row.fg_pct_home)},
            {num_float(row.fg3m_home)},
            {num_float(row.fg3a_home)},
            {num_float(row.fg3_pct_home)},
            {num_float(row.ftm_home)},
            {num_float(row.fta_home)},
            {num_float(row.ft_pct_home)},
            {num_float(row.oreb_home)},
            {num_float(row.dreb_home)},
            {num_float(row.reb_home)},
            {num_float(row.ast_home)},
            {num_float(row.stl_home)},
            {num_float(row.blk_home)},
            {num_float(row.tov_home)},
            {num_float(row.pf_home)},
            {num_float(row.pts_home)},
            {num(row.plus_minus_home)},
            {num(row.video_available_home)},
            {val(row.matchup_away)},
            {val(row.wl_away)},
            {num_float(row.fgm_away)},
            {num_float(row.fga_away)},
            {num_float(row.fg_pct_away)},
            {num_float(row.fg3m_away)},
            {num_float(row.fg3a_away)},
            {num_float(row.fg3_pct_away)},
            {num_float(row.ftm_away)},
            {num_float(row.fta_away)},
            {num_float(row.ft_pct_away)},
            {num_float(row.oreb_away)},
            {num_float(row.dreb_away)},
            {num_float(row.reb_away)},
            {num_float(row.ast_away)},
            {num_float(row.stl_away)},
            {num_float(row.blk_away)},
            {num_float(row.tov_away)},
            {num_float(row.pf_away)},
            {num_float(row.pts_away)},
            {num(row.plus_minus_away)},
            {num(row.video_available_away)},
            {val(row.season_type)}
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
print("✅ Carga de juegos completada.")