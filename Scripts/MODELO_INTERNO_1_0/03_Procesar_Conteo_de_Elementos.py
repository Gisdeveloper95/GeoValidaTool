import os
import sqlite3
import time
import logging
import sys
sys.stdout.reconfigure(encoding='utf-8')

def configurar_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

def obtener_ruta_base():
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_proyecto = os.path.abspath(os.path.join(ruta_actual, '..', '..'))
    
    ruta_gpkg = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_INTERNO_1_0", "GPKG_ORIGINAL")
    
    if not os.path.exists(ruta_gpkg):
        raise Exception(
            "No se encontró la ruta del GPKG.\n"
            "Verifique que existe la siguiente estructura:\n"
            "- GeoValidaTool/Files/Temporary_Files/MODELO_INTERNO_1_0/GPKG_ORIGINAL"
        )
    
    return ruta_proyecto

def obtener_gpkg_path():
    ruta_base = obtener_ruta_base()
    ruta_gpkg = os.path.join(ruta_base, "Files", "Temporary_Files", "MODELO_INTERNO_1_0", "GPKG_ORIGINAL")
    
    archivos_gpkg = [f for f in os.listdir(ruta_gpkg) if f.endswith('.gpkg')]
    if not archivos_gpkg:
        raise Exception("No se encontró ningún archivo GPKG en el directorio")
    
    return os.path.join(ruta_gpkg, archivos_gpkg[0])

def crear_ruta_sqlite():
    ruta_base = obtener_ruta_base()
    sqlite_dir = os.path.join(ruta_base, "Files", "Temporary_Files", "MODELO_INTERNO_1_0", "db")
    
    if not os.path.exists(sqlite_dir):
        os.makedirs(sqlite_dir)
    
    sqlite_path = os.path.join(sqlite_dir, "conteo_elementos.db")
    
    if os.path.exists(sqlite_path):
        try:
            os.remove(sqlite_path)
            for ext in ['-wal', '-shm']:
                temp_file = sqlite_path + ext
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        except Exception as e:
            logging.error(f"Error al eliminar la base de datos existente: {str(e)}")
            raise
    
    return sqlite_path

def contar_registros_gpkg():
    gpkg_path = obtener_gpkg_path()
    resultados = {}
    
    try:
        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()
        
        # Obtener todas las tablas del GPKG, no solo las marcadas como features
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name NOT LIKE 'sqlite_%'
            AND name NOT LIKE 'gpkg_%'
            AND name NOT LIKE 'rtree_%'
            AND name NOT LIKE 'T_Il2DB_%'
        """)
        
        tablas = [row[0] for row in cursor.fetchall()]
        logging.info(f"Tablas encontradas en GPKG: {len(tablas)}")
        
        for tabla in tablas:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM \"{tabla}\"")
                count = cursor.fetchone()[0]
                resultados[tabla] = count
                logging.info(f"Tabla {tabla}: {count} registros")
            except Exception as e:
                logging.error(f"Error contando registros en {tabla}: {str(e)}")
                resultados[tabla] = 0
        
    except Exception as e:
        logging.error(f"Error procesando GPKG: {str(e)}")
        raise
    finally:
        conn.close()
    
    return resultados
def crear_base_sqlite(sqlite_path, resultados):
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    
    try:
        conn.execute("BEGIN TRANSACTION")
        
        # Crear una única tabla con todas las capas como columnas
        columnas = [f"{tabla} INTEGER" for tabla in resultados.keys()]
        create_table_sql = f"CREATE TABLE conteos ({', '.join(columnas)})"
        cursor.execute(create_table_sql)
        
        # Insertar los valores
        valores = list(resultados.values())
        placeholders = ','.join(['?' for _ in valores])
        insert_sql = f"INSERT INTO conteos VALUES ({placeholders})"
        cursor.execute(insert_sql, valores)
        
        conn.commit()
        logging.info("Base de datos SQLite creada exitosamente")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error creando base SQLite: {str(e)}")
        raise
    finally:
        conn.close()

def main():
    try:
        configurar_logging()
        tiempo_inicio = time.time()
        
        logging.info("=== INICIANDO PROCESO DE ANÁLISIS ===")
        
        sqlite_path = crear_ruta_sqlite()
        resultados = contar_registros_gpkg()
        crear_base_sqlite(sqlite_path, resultados)
        
        tiempo_total = time.time() - tiempo_inicio
        
        # Calcular estadísticas
        total_registros = sum(resultados.values())
        capas_con_registros = sum(1 for count in resultados.values() if count > 0)
        capas_sin_registros = sum(1 for count in resultados.values() if count == 0)
        
        logging.info("\n=== RESUMEN DEL PROCESO ===")
        logging.info(f"Tiempo total de ejecución: {tiempo_total:.2f} segundos")
        logging.info(f"Total de capas procesadas: {len(resultados)}")
        logging.info(f"Total de registros: {total_registros:,}")
        logging.info(f"Capas con registros: {capas_con_registros}")
        logging.info(f"Capas sin registros: {capas_sin_registros}")
        logging.info(f"Base de datos creada en: {sqlite_path}")
        logging.info("=== PROCESO COMPLETADO ===")
        
    except Exception as e:
        logging.error(f"\nERROR CRÍTICO: {str(e)}")
        logging.error("El proceso no se completó correctamente")

if __name__ == "__main__":
    main()