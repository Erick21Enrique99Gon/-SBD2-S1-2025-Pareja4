import pymongo
import psycopg2
from tqdm import tqdm

def setup_connections():
    try:
        # Conexión a MongoDB (sin autenticación en este ejemplo)
        myclient = pymongo.MongoClient("mongodb://root:admin123@localhost:27017/")
        mydb = myclient["local"]  # Cambia al nombre de tu BD MongoDB
        mycol = mydb["games_info"]    # Colección donde guardaremos los datos

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

def main():
    try:
        print("Iniciando migración de datos de juegos...")
        mycol, conn = setup_connections()

        # Consulta SQL completa con todos los joins
        comando = """
        SELECT 
            gi.*,
            ga.*,
            oog.*,
            o.*,
            gd.*
        FROM game_info gi
        FULL JOIN game_attendance ga ON ga.game_id = gi.game_id
        FULL JOIN official_of_game oog ON oog.game_id = gi.game_id
        FULL JOIN officials o ON oog.official_id = o.official_id
        FULL JOIN game_date gd ON gd.game_id = gi.game_id
        FULL join game_summary gs on gs.game_id = gi.game_id
        """
        
        cur = conn.cursor()
        cur.execute(comando)
        resultTodos = cur.fetchall()
        
        print(f"Total de registros a procesar: {len(resultTodos)}")
        
        # Procesar cada registro y crear documento denormalizado
        for result in tqdm(resultTodos, desc="Migrando datos"):
            game = {
                # Datos de game_info
                "game_id": result[0],
                
                # Datos de game_attendance
                "attendance": result[1],
                "game_time": result[2],
                
                # Datos de official_of_game (relación)
                "official_game_id": result[3],
                "official_id_rel": result[4],
                
                # Datos de officials
                "official_id": result[5],
                "official_first_name": result[6],
                "official_last_name": result[7],
                "official_jersey_num": result[8],
                
                # Datos de game_date
                "game_date": result[9],

                # Datos de game_summary
                	"game_date_est": result[9],
                    "game_sequence": result[9],
                    "game_id": result[9],
                    "game_status_id": result[9],
                    "game_status_text": result[9],
                    "gamecode": result[9],
                    "home_team_id": result[9],
                    "visitor_team_id": result[9],
                    "season": result[9],
                    "live_period": result[9],
                    "live_pc_time": result[9],
                    "natl_tv_broadcaster_abbreviation": result[9],
                    "live_period_time_bcast": result[9],
                    "wh_status": result[9]
            }
            
            # Insertar en MongoDB (usamos game_id como _id para evitar duplicados)
            mycol.update_one(
                {"_id": game["game_id"]},
                {"$set": game},
                upsert=True
            )

        print("Migración completada exitosamente!")

    except Exception as e:
        print(f"Error en la ejecución principal: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()