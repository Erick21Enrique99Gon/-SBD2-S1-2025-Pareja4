import pandas as pd
import psycopg2

# Funciones de utilidad
def val(v):
    return 'NULL' if pd.isna(v) or v == '' else f"'{str(v).replace("'", "''")}'"

def num(v):
    return 'NULL' if pd.isna(v) or v == '' else str(int(v))

# Conexión a la base de datos
conn = psycopg2.connect(
    database="bd2_2s24",
    user="admin",
    host="localhost",
    password="root1234",
    port=5432,
    options="-c search_path=public"
)
cur = conn.cursor()

# Cargar CSV
df = pd.read_csv('./csv/team_history.csv', sep=',')

# Iterar e insertar fila por fila
for row in df.itertuples(index=False):
    try:
        query = f"""
        CALL insertar_team_history(
            {val(row.team_id)},
            {val(row.city)},
            {val(row.nickname)},
            {num(row.year_founded)},
            {num(row.year_active_till)}
        );
        """
        print(f"Ejecutando: script ={query }")
        cur.execute(query)
    except Exception as e:
        print(f"[ERROR] team_id={row.team_id} -> {e}")
        conn.rollback()
        break
    else:
        conn.commit()

# Cerrar conexión
cur.close()
conn.close()