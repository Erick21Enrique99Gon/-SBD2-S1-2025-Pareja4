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

# dataframeP = pd.read_csv("./csv/play_by_play.csv", sep=',' )
# df_filtrado = dataframeP.groupby(list(dataframeP.columns)).filter(lambda x: len(x) > 1)

# print(df_filtrado)
# # cur = conn.cursor()

# # def val(v):
# #     return 'NULL' if pd.isna(v) or v == '' else f"'{str(v).replace("'", "''")}'"

# # def num(v):
# #     return 'NULL' if pd.isna(v) or v == '' else str(int(v))
# # chunk_size = 10000
# # for chunk in pd.read_csv('./csv/play_by_play.csv', sep=',', chunksize=chunk_size):
# #     for row in chunk.itertuples(index=False):
# #         try:
# #             sql = f"""
# #             CALL insertar_play_by_play(
# #                 {val(row.game_id)},
# #                 {num(row.eventnum)},
# #                 {num(row.eventmsgtype)},
# #                 {num(row.eventmsgactiontype)},
# #                 {num(row.period)},
# #                 {val(row.wctimestring)},
# #                 {val(row.pctimestring)},
# #                 {val(row.homedescription)},
# #                 {val(row.neutraldescription)},
# #                 {val(row.visitordescription)},
# #                 {val(row.score)},
# #                 {val(row.scoremargin)},
# #                 {val(row.player1_id)},
# #                 {val(row.player1_name)},
# #                 {num(row.player1_team_id)},
# #                 {val(row.player1_team_city)},
# #                 {val(row.player1_team_abbreviation)},
# #                 {val(row.player2_id)},
# #                 {val(row.player2_name)},
# #                 {num(row.player2_team_id)},
# #                 {val(row.player2_team_city)},
# #                 {val(row.player2_team_abbreviation)},
# #                 {val(row.player3_id)},
# #                 {val(row.player3_name)},
# #                 {num(row.player3_team_id)},
# #                 {val(row.player3_team_city)},
# #                 {val(row.player3_team_abbreviation)}
# #             );
# #             """
# #             print(sql)
# #             cur.execute(sql)
# #             conn.commit()
# #         except Exception as e:
# #             print(f"❌ Error en game_id={row.game_id}, eventnum={row.eventnum}: {e}")
# #             conn.rollback()

# # cur.close()
# # conn.close()
# # print("✅ Carga de eventos play_by_play completada.")


# import pandas as pd
# import psycopg2
# import threading

# # Parámetros
# start_row = 2832910
# chunk_size = 10000
# num_threads = 6
# linea_Estado = 2832910
# linea_lock = threading.Lock()
# # Función de utilidad
# def val(v):
#     return 'NULL' if pd.isna(v) or v == '' else f"'{str(v).replace("'", "''")}'"

# def num(v):
#     return 'NULL' if pd.isna(v) or v == '' else str(int(v))

# # Función de carga de datos por chunk
# def process_chunk(chunk):
#     conn = psycopg2.connect(
#         database="bd2_2s24",
#         user="admin",
#         host="localhost",
#         password="root1234",
#         port=5432,
#         options="-c search_path=public"
#     )
#     cur = conn.cursor()
#     for row in chunk.itertuples(index=False):
#         try:
#             sql = f"""
#             CALL insertar_play_by_play(
#                 {val(row.game_id)},
#                 {num(row.eventnum)},
#                 {num(row.eventmsgtype)},
#                 {num(row.eventmsgactiontype)},
#                 {num(row.period)},
#                 {val(row.wctimestring)},
#                 {val(row.pctimestring)},
#                 {val(row.homedescription)},
#                 {val(row.neutraldescription)},
#                 {val(row.visitordescription)},
#                 {val(row.score)},
#                 {val(row.scoremargin)},
#                 {val(row.player1_id)},
#                 {val(row.player1_name)},
#                 {num(row.player1_team_id)},
#                 {val(row.player1_team_city)},
#                 {val(row.player1_team_abbreviation)},
#                 {val(row.player2_id)},
#                 {val(row.player2_name)},
#                 {num(row.player2_team_id)},
#                 {val(row.player2_team_city)},
#                 {val(row.player2_team_abbreviation)},
#                 {val(row.player3_id)},
#                 {val(row.player3_name)},
#                 {num(row.player3_team_id)},
#                 {val(row.player3_team_city)},
#                 {val(row.player3_team_abbreviation)}
#             );
#             """
#             with linea_lock:  # Bloqueamos para evitar condiciones de carrera
#                 linea_Estado += 1
#                 if linea_Estado % 100000 == 0:
#                     print(f"Progreso: {linea_Estado} filas procesadas")
#             cur.execute(sql)
#             conn.commit()
#         except Exception as e:
#             print(f"❌ Error en game_id={row.game_id}, eventnum={row.eventnum}: {e}")
#             conn.rollback()
#     cur.close()
#     conn.close()

# # Lectura con desplazamiento y multihilo
# reader = pd.read_csv('./csv/play_by_play.csv', sep=',', chunksize=chunk_size, skiprows=range(1, start_row), iterator=True)
# while True:
#     threads = []
#     try:
#         for _ in range(num_threads):
#             chunk = next(reader)
#             t = threading.Thread(target=process_chunk, args=(chunk,))
#             threads.append(t)
#             t.start()
#         for t in threads:
#             t.join()
#     except StopIteration:
#         break

# print("✅ Carga de eventos play_by_play completada.")




import pandas as pd
import psycopg2
import threading
from tqdm import tqdm  # pip install tqdm

# Configuración
start_row = 4152887
chunk_size = 10000
num_threads = 10

# Función para formatear valores SQL
def val(v):
    return 'NULL' if pd.isna(v) or v == '' else f"'{str(v).replace("'", "''")}'"

def num(v):
    return 'NULL' if pd.isna(v) or v == '' else str(int(v))

# Procesar un chunk
def process_chunk(chunk, chunk_number):
    conn = psycopg2.connect(
        database="bd2_2s24",
        user="admin",
        host="localhost",
        password="root1234",
        port=5432,
        options="-c search_path=public"
    )
    cur = conn.cursor()
    
    # Barra de progreso por chunk
    for row in tqdm(chunk.itertuples(index=False), 
                    total=len(chunk), 
                    desc=f"Chunk {chunk_number}", 
                    position=chunk_number % num_threads):  # Evita sobreposición de barras
        try:
            sql = f"""
            CALL insertar_play_by_play(
                {val(row.game_id)}, {num(row.eventnum)},
                {num(row.eventmsgtype)}, {num(row.eventmsgactiontype)},
                {num(row.period)}, {val(row.wctimestring)},
                {val(row.pctimestring)}, {val(row.homedescription)},
                {val(row.neutraldescription)}, {val(row.visitordescription)},
                {val(row.score)}, {val(row.scoremargin)},
                {val(row.player1_id)}, {val(row.player1_name)},
                {num(row.player1_team_id)}, {val(row.player1_team_city)},
                {val(row.player1_team_abbreviation)}, {val(row.player2_id)},
                {val(row.player2_name)}, {num(row.player2_team_id)},
                {val(row.player2_team_city)}, {val(row.player2_team_abbreviation)},
                {val(row.player3_id)}, {val(row.player3_name)},
                {num(row.player3_team_id)}, {val(row.player3_team_city)},
                {val(row.player3_team_abbreviation)}
            );
            """
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print(f"\n❌ Error en game_id={row.game_id}, eventnum={row.eventnum}: {e}")
            conn.rollback()
    
    cur.close()
    conn.close()

# Carga paralela con threads
reader = pd.read_csv('./csv/play_by_play.csv', 
                     sep=',', 
                     chunksize=chunk_size, 
                     skiprows=range(1, start_row), 
                     iterator=True)

chunk_count = 0
while True:
    threads = []
    try:
        for _ in range(num_threads):
            chunk = next(reader)
            chunk_count += 1
            t = threading.Thread(target=process_chunk, args=(chunk, chunk_count))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
    except StopIteration:
        break

print("\n✅ Carga completada.")