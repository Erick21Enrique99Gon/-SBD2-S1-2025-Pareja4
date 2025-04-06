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

# Leer CSV
dataframe = pd.read_csv('./csv/draft_combine_stats.csv', sep=',')

cur = conn.cursor()

# Función para sanear valores: escapa comillas simples y convierte NaN a NULL
def safe(val):
    if pd.isna(val):
        return 'NULL'
    else:
        return f"'{str(val).replace("'", "''")}'"

# Iterar sobre cada fila y ejecutar el procedimiento
for row in dataframe.itertuples(index=False):
    comando = f"""
    CALL insertar_draft_combine_stats(
        {safe(row.season)},
        {safe(row.player_id)},
        {safe(row.first_name)},
        {safe(row.last_name)},
        {safe(row.position)},
        {row.height_wo_shoes if not pd.isna(row.height_wo_shoes) else 'NULL'},
        {safe(row.height_wo_shoes_ft_in)},
        {row.height_w_shoes if not pd.isna(row.height_w_shoes) else 'NULL'},
        {safe(row.height_w_shoes_ft_in)},
        {safe(row.weight)},
        {row.wingspan if not pd.isna(row.wingspan) else 'NULL'},
        {safe(row.wingspan_ft_in)},
        {row.standing_reach if not pd.isna(row.standing_reach) else 'NULL'},
        {safe(row.standing_reach_ft_in)},
        {safe(row.body_fat_pct)},
        {safe(row.hand_length)},
        {safe(row.hand_width)},
        {row.standing_vertical_leap if not pd.isna(row.standing_vertical_leap) else 'NULL'},
        {row.max_vertical_leap if not pd.isna(row.max_vertical_leap) else 'NULL'},
        {row.lane_agility_time if not pd.isna(row.lane_agility_time) else 'NULL'},
        {row.modified_lane_agility_time if not pd.isna(row.modified_lane_agility_time) else 'NULL'},
        {row.three_quarter_sprint if not pd.isna(row.three_quarter_sprint) else 'NULL'},
        {row.bench_press if not pd.isna(row.bench_press) else 'NULL'},
        {safe(row.spot_fifteen_corner_left)},
        {safe(row.spot_fifteen_break_left)},
        {safe(row.spot_fifteen_top_key)},
        {safe(row.spot_fifteen_break_right)},
        {safe(row.spot_fifteen_corner_right)},
        {safe(row.spot_college_corner_left)},
        {safe(row.spot_college_break_left)},
        {safe(row.spot_college_top_key)},
        {safe(row.spot_college_break_right)},
        {safe(row.spot_college_corner_right)},
        {safe(row.spot_nba_corner_left)},
        {safe(row.spot_nba_break_left)},
        {safe(row.spot_nba_top_key)},
        {safe(row.spot_nba_break_right)},
        {safe(row.spot_nba_corner_right)},
        {safe(row.off_drib_fifteen_break_left)},
        {safe(row.off_drib_fifteen_top_key)},
        {safe(row.off_drib_fifteen_break_right)},
        {safe(row.off_drib_college_break_left)},
        {safe(row.off_drib_college_top_key)},
        {safe(row.off_drib_college_break_right)},
        {safe(row.on_move_fifteen)},
        {safe(row.on_move_college)}
    );
    """
    try:
        cur.execute(comando)
    except Exception as e:
        print(f"❌ Error en fila con player_id={row.player_id}: {e}")
        conn.rollback()
    else:
        conn.commit()

cur.close()
conn.close()
print("✅ Importación finalizada.")