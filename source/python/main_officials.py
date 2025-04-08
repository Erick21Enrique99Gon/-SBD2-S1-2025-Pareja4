# import pandas as pd
# import psycopg2

# # Conexión a la base de datos
# conn = psycopg2.connect(
#     database="bd2_2s24",
#     user="admin",
#     host="localhost",
#     password="root1234",
#     port=5432,
#     options="-c search_path=public"
# )

# # Leer el CSV
# df = pd.read_csv('./csv/officials.csv', sep=',')

# cur = conn.cursor()

# # Función de utilidad para convertir texto SQL seguro
# def safe(val):
#     if pd.isna(val):
#         return 'NULL'
#     else:
#         return f"'{str(val).replace("'", "''")}'"

# # Iterar sobre cada fila del CSV
# for row in df.itertuples(index=False):
#     try:
#         game_id = safe(row.game_id)
#         first_name = safe(row.first_name)
#         last_name = safe(row.last_name)
#         jersey_num = safe(row.jersey_num)

#         sql = f"""
#         CALL insertar_arbitro_por_partido(
#             {game_id},
#             {first_name},
#             {last_name},
#             {jersey_num}
#         );
#         """
#         print(sql)
#         cur.execute(sql)
#         conn.commit()
#     except Exception as e:
#         print(f"❌ Error con game_id={row.game_id}, árbitro={row.first_name} {row.last_name}: {e}")
#         conn.rollback()

# cur.close()
# conn.close()
# print("✅ Carga de árbitros completada.")




import pandas as pd
import psycopg2

# Conexión a la base de datos
conn = psycopg2.connect(
    database="bd2_2s24",
    user="admin",
    host="localhost",
    password="root1234",
    port=5432,
    options="-c search_path=prueba2"
)

# Leer el CSV
df = pd.read_csv('./csv/officials.csv', sep=',')

cur = conn.cursor()

# Función de utilidad para convertir texto SQL seguro
def safe(val):
    if pd.isna(val):
        return 'NULL'
    else:
        return f"'{str(val).replace("'", "''")}'"

# Iterar sobre cada fila del CSV
for row in df.itertuples(index=False):
    try:
        game_id = safe(row.game_id)
        first_name = safe(row.first_name)
        official_id = safe(row.official_id)
        last_name = safe(row.last_name)
        jersey_num = safe(row.jersey_num)

        sql = f"""
        CALL prueba2.insertar_arbitro_por_partido(
            {game_id},
            {official_id},
            {first_name},
            {last_name},
            {jersey_num}
        );
        """
        print(sql)
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"❌ Error con game_id={row.game_id}, árbitro={row.first_name} {row.last_name}: {e}")
        conn.rollback()
        break

cur.close()
conn.close()
print("✅ Carga de árbitros completada.")