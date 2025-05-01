import pymongo
import psycopg2

def main():
    try:
        print("Iniciando migración de equipos...")
        
        # 1. Configurar conexiones
        # MongoDB
        mongo_client = pymongo.MongoClient(
            "mongodb://root:admin123@localhost:27017/",
            authSource="admin"
        )
        mongo_db = mongo_client["local"]
        mongo_col = mongo_db["teams_denormalized"]
        
        # PostgreSQL
        pg_conn = psycopg2.connect(
            database="bd2_2s24",
            user="admin",
            host='localhost',
            password="root1234",
            port=5432,
            options="-c search_path=public"
        )
        
        # 2. Ejecutar consulta SQL
        with pg_conn.cursor() as cur:
            query = """
            SELECT 
                t.id as team_id,
                t.full_name,
                t.abbreviation,
                t.nickname,
                t.city as city_id,
                t.state as state_id,
                t.year_founded,
                t.team_code,
                td.arena,
                td.arenacapacity,
                td.owner,
                td.generalmanager,
                td.headcoach,
                td.dleagueaffiliation,
                td.facebook,
                td.instagram,
                td.twitter,
                c.city as city_name,
                s.state as state_name
            FROM team t
            LEFT JOIN team_details td ON td.team_id = t.id
            LEFT JOIN cities c ON c.id = t.city
            LEFT JOIN states s ON s.id = t.state
            WHERE t.id IS NOT NULL
            """
            cur.execute(query)
            
            # 3. Procesar e insertar los datos
            count = 0
            while True:
                batch = cur.fetchmany(1000)  # Traer 1000 registros a la vez
                if not batch:
                    break
                
                docs = []
                for result in batch:
                    team = {
                        "team_id": result[0],
                        "full_name": result[1],
                        "abbreviation": result[2],
                        "nickname": result[3],
                        "city_id": result[4],
                        "state_id": result[5],
                        "year_founded": result[6],
                        "team_code": result[7],
                        "arena": result[8],
                        "arena_capacity": result[9],
                        "owner": result[10],
                        "general_manager": result[11],
                        "head_coach": result[12],
                        "dleague_affiliation": result[13],
                        "facebook": result[14],
                        "instagram": result[15],
                        "twitter": result[16],
                        "city_name": result[17],
                        "state_name": result[18]
                    }
                    # Limpiar valores None
                    team = {k: v for k, v in team.items() if v is not None}
                    docs.append(team)
                
                # Insertar en MongoDB
                if docs:
                    mongo_col.insert_many(docs, ordered=False)
                    count += len(docs)
                    print(f"Registros insertados: {count}", end='\r')
        
        print(f"\nMigración completada! Total de equipos migrados: {count}")
        
    except Exception as e:
        print(f"\nError durante la migración: {str(e)}")
    finally:
        # Cerrar conexiones
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_client' in locals():
            mongo_client.close()

if __name__ == "__main__":
    main()