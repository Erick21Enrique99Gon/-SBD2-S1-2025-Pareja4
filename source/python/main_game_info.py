import pandas as pd
import psycopg2
from datetime import datetime

# Conexión a la base de datos
conn = psycopg2.connect(
    database="bd2_2s24",
    user="admin",
    host="localhost",
    password="root1234",
    port=5432,
    options="-c search_path=public"
)

# Leer el CSV
df = pd.read_csv('./csv/game_info.csv', sep=',')

cur = conn.cursor()

# Función de utilidad para convertir valores a texto SQL seguro
def safe(val):
    if pd.isna(val):
        return 'NULL'
    else:
        return f"'{str(val).replace("'", "''")}'"

# Iterar sobre el DataFrame y ejecutar el procedimiento
for row in df.itertuples(index=False):
    try:
        game_id = safe(row.game_id)
        game_date = f"'{pd.to_datetime(row.game_date).strftime('%Y-%m-%d %H:%M:%S')}'" if not pd.isna(row.game_date) else 'NULL'
        attendance = int(row.attendance) if not pd.isna(row.attendance) else 'NULL'
        game_time = safe(row.game_time)

        sql = f"""
        CALL insertar_game_info_detalle(
            {game_id},
            {game_date},
            {attendance},
            {game_time}
        );
        """
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"❌ Error con game_id={row.game_id}: {e}")
        conn.rollback()

cur.close()
conn.close()
print("✅ Carga de game_info completada.")