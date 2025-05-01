import pymongo
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import threading
import queue
import time

# Configuración
MAX_THREADS = 8
BATCH_SIZE = 1000
START_OFFSET = 889031  # Comenzar desde el registro 1,000,000
MONGO_URI = "mongodb://root:admin123@localhost:27017/"
MONGO_DB = "local"
MONGO_COL = "play_by_play"

# Variables globales
mongo_col = None
pg_conn = None
data_queue = queue.Queue()
processed_count = 0
processed_lock = threading.Lock()

def setup_connections():
    try:
        client = pymongo.MongoClient(
            MONGO_URI,
            authSource="admin",
            maxPoolSize=MAX_THREADS + 2,
            socketTimeoutMS=60000,
            connectTimeoutMS=30000
        )
        db = client[MONGO_DB]
        collection = db[MONGO_COL]

        conn = psycopg2.connect(
            database="bd2_2s24",
            user="admin",
            host='localhost',
            password="root1234",
            port=5432,
            options="-c search_path=public"
        )

        return collection, conn

    except Exception as e:
        print(f"Error en conexiones: {str(e)}")
        raise

def worker():
    global processed_count
    while True:
        batch = data_queue.get()
        if batch is None:
            break

        try:
            docs = []
            for result in batch:
                play = {
                    # MongoDB generará automáticamente _id
                    "game_id": result[0],
                    "eventnum": result[1],
                    "eventmsgtype": result[2],
                    "eventmsgactiontype": result[3],
                    "period": result[4],
                    "wctimestring": result[5],
                    "pctimestring": result[6],
                    "homedescription": result[7],
                    "neutraldescription": result[8],
                    "visitordescription": result[9],
                    "score": result[10],
                    "scoremargin": result[11],
                        "type": result[12],
                        "player_id": result[13],
                        "team_id": result[14],
                        "type": result[15],
                        "player_id": result[16],
                        "team_id": result[17],
                        "type": result[18],
                        "player_id": result[19],
                        "team_id": result[20],
                    "video_available_flag": result[21]
                }
                docs.append({k: v for k, v in play.items() if v is not None})

            if docs:
                try:
                    mongo_col.insert_many(docs, ordered=False)
                    with processed_lock:
                        processed_count += len(docs)
                except pymongo.errors.BulkWriteError as e:
                    success = len(docs) - len(e.details['writeErrors'])
                    with processed_lock:
                        processed_count += success
                    print(f"\n⚠️ Errores en lote: {len(e.details['writeErrors'])}")

        except Exception as e:
            print(f"\n❌ Error en worker: {str(e)}")
        finally:
            data_queue.task_done()

def fetch_data_in_batches(cursor, batch_size):
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield batch

def main():
    global mongo_col, pg_conn, processed_count
    
    try:
        print("🚀 Iniciando migración de play_by_play...")
        print(f"➡️ Saltando los primeros {START_OFFSET:,} registros")
        start_time = time.time()
        
        mongo_col, pg_conn = setup_connections()
        
        # Obtener conteo total para la barra de progreso
        with pg_conn.cursor() as count_cur:
            count_cur.execute("SELECT COUNT(*) FROM play_by_play")
            total_records = count_cur.fetchone()[0] - START_OFFSET
        
        # Configurar barra de progreso
        main_progress = tqdm(total=total_records, desc="📊 Progreso", unit="rec")
        
        # Configurar workers
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            workers = [executor.submit(worker) for _ in range(MAX_THREADS)]
            
            with pg_conn.cursor(name="pbp_cursor") as cur:
                cur.itersize = BATCH_SIZE * 10
                query = f"""
                SELECT 
                    game_id, eventnum, eventmsgtype, eventmsgactiontype,
                    period, wctimestring, pctimestring, homedescription,
                    neutraldescription, visitordescription, score, scoremargin,
                    person1type, player1_id, player1_team_id,
                    person2type, player2_id, player2_team_id,
                    person3type, player3_id, player3_team_id,
                    video_available_flag
                FROM play_by_play
                ORDER BY game_id, eventnum
                OFFSET {START_OFFSET} ROWS
                """
                cur.execute(query)
                
                for batch in fetch_data_in_batches(cur, BATCH_SIZE):
                    data_queue.put(batch)
                    main_progress.update(len(batch))
            
            # Señal de terminación
            for _ in range(MAX_THREADS):
                data_queue.put(None)
            
            data_queue.join()
            main_progress.close()
        
        elapsed = time.time() - start_time
        print(f"\n✅ Migración completada desde el registro {START_OFFSET:,}!")
        print(f"📝 Total eventos procesados: {processed_count:,}")
        print(f"⏱️ Tiempo total: {elapsed:.2f} segundos")
        print(f"⚡ Velocidad: {processed_count/elapsed:,.2f} registros/segundo")
        
    except Exception as e:
        print(f"\n❌ Error en main: {str(e)}")
    finally:
        if pg_conn:
            pg_conn.close()
        if mongo_col:
            mongo_col.client.close()

if __name__ == "__main__":
    main()

# import pymongo

# myclient = pymongo.MongoClient("mongodb://root:admin123@localhost:27017/")
# import pandas as pd
# import psycopg2
# conn = psycopg2.connect(database = "bd2_2s24", 
#                         user = "admin", 
#                         host= 'localhost',
#                         password = "root1234",
#                         port = 5432,
#                         options="-c search_path=public")
# mydb = myclient["local"]
# mycol = mydb["play_by_play"]

# comando = """select * from play_by_play pbp
# order by pbp.game_id,pbp.eventnum;"""

# cur = conn.cursor()


# cur.execute(comando)

# results =cur.fetchall()

# #print(results)

# for result in results:
#     play_by_play ={
#         		"game_id" : result[0],
#                 "eventnum" : result[1],
#                 "eventmsgtype" : result[2],
#                 "eventmsgactiontype" : result[3],
#                 "period" : result[4],
#                 "wctimestring" : result[5],
#                 "pctimestring" : result[6],
#                 "homedescription" : result[7],
#                 "neutraldescription" : result[8],
#                 "visitordescription" : result[9],
#                 "score" : result[10],
#                 "scoremargin" : result[11],
#                 "person1type" : result[12],
#                 "player1_id" : result[13],
#                 "player1_team_id" : result[14],
#                 "person2type" : result[15],
#                 "player2_id" : result[16],
#                 "player2_team_id" : result[17],
#                 "person3type" : result[18],
#                 "player3_id" : result[19],
#                 "player3_team_id" : result[20],
#                 "video_available_flag" : result[21]
#     }
#     print(play_by_play)
#     x=mycol.insert_one(play_by_play)
#     print(x.inserted_id)


# conn.commit()

# cur.close()
# conn.close()