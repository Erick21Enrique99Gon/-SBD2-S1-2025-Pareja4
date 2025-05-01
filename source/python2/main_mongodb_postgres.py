# import pymongo

# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# import pandas as pd
# import psycopg2
# conn = psycopg2.connect(database = "bd2_2s24", 
#                         user = "admin", 
#                         host= 'localhost',
#                         password = "root1234",
#                         port = 5432,
#                         options="-c search_path=public")
# mydb = myclient["local"]
# mycol = mydb["players"]

# print('hola mundo')
# comando = """select * from player p
# full join player_active pa on pa.id = p.id
# full join player_school ps on ps.person_id = p.id
# full join school s on ps.school_id = s.school_id
# full join common_player_info cpi on cpi.person_id = p.id
# full join country c on c.country_id = cpi.country_id
# full join last_affiliation la on la.last_affiliation_id = cpi.last_affiliation_id
# full join draft_history dh on dh.person_id = p.id
# full join draft d on d.draft_id = dh.draft_id
# full join "Organization" o on o.organization_id = dh.organization_id
# full join "Organization_type" ot on ot.organization_type_id = o.organization_type_id;"""
# cur = conn.cursor()
# cur.execute(comando)



# conn.commit()
# resultTodos =cur.fetchall()

# for result in resultTodos:
#     player = {
#         "id":result[0],
#         "first_name":result[1],
#         "last_name":result[2],
#         "id":result[3],
#         "s_active":result[4],
#         "school_id":result[5],
#         "person_id":result[6],
#         "school_id":result[7],
#         "school":result[8],
#         "person_id":result[9],
#         "display_fi_last":result[10],
#         "player_slug":result[11],
#         "birthdate":result[12],
#         "country_id": result[13] ,
#         "last_affiliation_id":result[14],
#         "height":result[15],
#         "weight":result[16],
#         "season_exp":result[17],
#         "jersey":result[18],
#         "position_id":result[19],
#         "rosterstatus":result[20],
#         "games_played_current_season_flag":result[21],
#         "team_id":result[22],
#         "playercode":result[23],
#         "from_year":result[24],
#         "to_year":result[25],
#         "dleague_flag":result[26],
#         "nba_flag":result[27],
#         "games_played_flag":result[28],
#         "draft_year":result[29],
#         "draft_round":result[30],
#         "draft_number":result[31],
#         "greatest_75_flag":result[32],
#         "country_id":result[33],
#         "country":result[34],
#         "last_affiliation_id":result[35],
#         "last_affiliation":result[36],
#         "person_id":result[37],
#         "season":result[38],
#         "round_number":result[39],
#         "round_pick":result[40],
#         "overall_pick":result[41],
#         "draft_id":result[42],
#         "team_id":result[43],
#         "organization_id":result[44],
#         "player_profile_flag":result[45],
#         "draft_id":result[46],
#         "draft_type":result[47],
#         "organization_id":result[48],
#         "organization":result[49],
#         "organization_type_id":result[50],
#         "organization_type_id":result[51],
#         "organization_type":result[52]
#         }
#     x = mycol.insert_one(player)

#     print(x.inserted_id)

# print(player)

# cur.close()
# conn.close()


import pymongo
import pandas as pd
import psycopg2
from tqdm import tqdm
import threading
from queue import Queue

# Configuración de conexiones
def setup_connections():
    try:
        # Conexión a MongoDB
        myclient = pymongo.MongoClient("mongodb://root:admin123@localhost:27017/")
        mydb = myclient["local"]
        mycol = mydb["players"]
        
        # Conexión a PostgreSQL
        conn = psycopg2.connect(
            database="bd2_2s24",
            user="admin",
            host='localhost',
            password="root1234",
            port=5432,
            options="-c search_path=public"
        )
        
        return mycol, conn
    
    except Exception as e:
        print(f"Error al establecer conexiones: {e}")
        raise

# Función para procesar un lote de registros
def process_batch(batch, collection, progress_bar):
    try:
        for result in batch:
            player = {
                "id": result[0],
                "first_name": result[1],
                "last_name": result[2],
                "s_active": result[4],
                "school_id": result[5],
                "school": result[8],
                "display_fi_last": result[10],
                "player_slug": result[11],
                "birthdate": result[12],
                "country_id": result[13],
                "last_affiliation_id": result[14],
                "height": result[15],
                "weight": result[16],
                "season_exp": result[17],
                "jersey": result[18],
                "position_id": result[19],
                "rosterstatus": result[20],
                "games_played_current_season_flag": result[21],
                "team_id": result[22],
                "playercode": result[23],
                "from_year": result[24],
                "to_year": result[25],
                "dleague_flag": result[26],
                "nba_flag": result[27],
                "games_played_flag": result[28],
                "draft_year": result[29],
                "draft_round": result[30],
                "draft_number": result[31],
                "greatest_75_flag": result[32],
                "country": result[34],
                "last_affiliation": result[36],
                "season": result[38],
                "round_number": result[39],
                "round_pick": result[40],
                "overall_pick": result[41],
                "draft_id": result[42],
                "organization_id": result[44],
                "player_profile_flag": result[45],
                "draft_type": result[47],
                "organization": result[49],
                "organization_type": result[52]
            }
            
            # Eliminar valores None o vacíos
            player = {k: v for k, v in player.items() if v is not None}
            
            collection.insert_one(player)
            progress_bar.update(1)
            
    except Exception as e:
        print(f"Error en el procesamiento por lotes: {e}")

# Función principal
def main():
    try:
        print("Iniciando migración de datos...")
        
        # Configurar conexiones
        mycol, conn = setup_connections()
        
        # Consulta SQL
        comando = """select * from player p
        full join player_active pa on pa.id = p.id
        full join player_school ps on ps.person_id = p.id
        full join school s on ps.school_id = s.school_id
        full join common_player_info cpi on cpi.person_id = p.id
        full join country c on c.country_id = cpi.country_id
        full join last_affiliation la on la.last_affiliation_id = cpi.last_affiliation_id
        full join draft_history dh on dh.person_id = p.id
        full join draft d on d.draft_id = dh.draft_id
        full join "Organization" o on o.organization_id = dh.organization_id
        full join "Organization_type" ot on ot.organization_type_id = o.organization_type_id;"""
        
        # Ejecutar consulta
        cur = conn.cursor()
        cur.execute(comando)
        conn.commit()
        
        # Obtener todos los resultados
        resultTodos = cur.fetchall()
        total_records = len(resultTodos)
        
        print(f"Total de registros a migrar: {total_records}")
        
        # Configurar barra de progreso
        progress_bar = tqdm(total=total_records, desc="Migrando datos")
        
        # Configurar hilos
        num_threads = 4
        batch_size = 100
        threads = []
        
        # Dividir los datos en lotes
        batches = [resultTodos[i:i + batch_size] for i in range(0, total_records, batch_size)]
        
        # Procesar cada lote en un hilo separado
        for batch in batches:
            thread = threading.Thread(
                target=process_batch,
                args=(batch, mycol, progress_bar) )
            threads.append(thread)
            thread.start()
        
        # Esperar a que todos los hilos terminen
        for thread in threads:
            thread.join()
        
        # Cerrar conexiones
        cur.close()
        conn.close()
        progress_bar.close()
        
        print("Migración completada exitosamente!")
       
    except Exception as e:
        print(f"Error en la ejecución principal: {e}")
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main()