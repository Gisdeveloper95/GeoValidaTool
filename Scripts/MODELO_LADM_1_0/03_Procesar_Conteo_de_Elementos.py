import arcpy
import os
import sqlite3
import time
import logging
import ast
import sys
sys.stdout.reconfigure(encoding='utf-8')

def configurar_logging():
    """Configura el sistema de logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

def obtener_ruta_base():
    """
    Obtiene la ruta base del proyecto verificando la estructura de directorios esperada.
    """
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_proyecto = os.path.abspath(os.path.join(ruta_actual, '..', '..'))
    
    rutas_requeridas = [
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_LADM_1_2"),
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "array_config.txt"),
        os.path.join(ruta_proyecto, "Scripts")
    ]
    
    if all(os.path.exists(ruta) for ruta in rutas_requeridas):
        logging.info(f"Ruta base del proyecto encontrada en: {ruta_proyecto}")
        return ruta_proyecto
    
    print("\nVerificando rutas:")
    for ruta in rutas_requeridas:
        print(f"Ruta: {ruta}")
        print(f"¿Existe?: {os.path.exists(ruta)}")
    
    raise Exception(
        "No se encontró la raíz del proyecto.\n"
        "Verifique que está ejecutando el script desde dentro del proyecto "
        "y que existe la siguiente estructura:\n"
        "- Files/Temporary_Files/MODELO_LADM_1_2\n"
        "- Files/Temporary_Files/array_config.txt\n"
        "- Files/Temporary_Files/MODELO_LADM_1_2/db (se creará automáticamente)\n"
        "- Scripts"
    )

def verificar_conexion_gdb(gdb_path):
    """Verifica que la conexión a la geodatabase es válida"""
    try:
        desc = arcpy.Describe(gdb_path)
        logging.info(f"Conexión exitosa a GDB: {desc.dataType}")
        
        arcpy.env.workspace = gdb_path
        datasets = arcpy.ListDatasets()
        
        if not datasets:
            logging.error("No se encontraron datasets en la geodatabase")
            return False
            
        logging.info(f"Datasets encontrados: {len(datasets)}")
        return True
    except Exception as e:
        logging.error(f"Error verificando conexión a GDB: {str(e)}")
        return False

def leer_datasets_configurados():
    """Lee el archivo de configuración para determinar qué datasets procesar"""
    ruta_base = obtener_ruta_base()
    archivo_config = os.path.join(ruta_base, "Files", "Temporary_Files", "array_config.txt")
    
    try:
        with open(archivo_config, 'r') as f:
            contenido = f.read()
            datasets = ast.literal_eval(contenido)
            return [ds for ds in datasets if not ds.startswith('#')]
    except Exception as e:
        logging.error(f"Error al leer archivo de configuración: {str(e)}")
        raise

def crear_ruta_sqlite():
    """Crea la ruta relativa para el archivo SQLite y limpia si existe"""
    ruta_base = obtener_ruta_base()
    sqlite_dir = os.path.join(ruta_base, "Files", "Temporary_Files", "MODELO_LADM_1_2", "db")
    
    if not os.path.exists(sqlite_dir):
        os.makedirs(sqlite_dir)
        logging.info(f"Directorio creado: {sqlite_dir}")
    
    sqlite_path = os.path.join(sqlite_dir, "conteo_elementos.db")
    
    if os.path.exists(sqlite_path):
        try:
            logging.info("Se encontró una base de datos existente. Procediendo a eliminarla...")
            os.remove(sqlite_path)
            logging.info("Base de datos anterior eliminada correctamente")
            
            for ext in ['-wal', '-shm']:
                temp_file = sqlite_path + ext
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    logging.info(f"Archivo temporal eliminado: {os.path.basename(temp_file)}")
        except Exception as e:
            logging.error(f"Error al eliminar la base de datos existente: {str(e)}")
            raise
    
    return sqlite_path

def contar_registros_fc(fc_path):
    """Cuenta registros de un feature class"""
    fc_name = os.path.basename(fc_path)
    try:
        logging.info(f"Contando registros en: {fc_name}")
        
        if not arcpy.Exists(fc_path):
            logging.error(f"Feature class no encontrado: {fc_path}")
            return fc_name, 0
        
        try:
            # Usar GetCount_management en lugar de cursor
            result = arcpy.GetCount_management(fc_path)
            count = int(result.getOutput(0))
            
            logging.info(f"Feature Class {fc_name}: {count} registros")
            return fc_name, count
            
        except arcpy.ExecuteError as ee:
            logging.error(f"Error de ArcPy en {fc_name}: {str(ee)}")
            logging.error(f"Mensajes de ArcPy: {arcpy.GetMessages(2)}")
            return fc_name, 0
            
    except Exception as e:
        logging.error(f"Error contando registros en {fc_name}: {str(e)}")
        return fc_name, 0

def contar_features_relacionadas(gdb_path, dataset_principal):
    """
    Cuenta los elementos en features específicas de datasets relacionados
    basado en el dataset principal.
    """
    resultados = {}
    
    # Definir las relaciones entre datasets y sus features específicas
    relaciones = {
        'URBANO_CTM12': {
            'dataset': 'ZONA_HOMOGENEA_URBANO_CTM12',
            'features': [
                'U_ZONA_HOMO_GEOECONOMICA_CTM12',
                'U_ZONA_HOMOGENEA_FISICA_CTM12'
            ]
        },
        'RURAL_CTM12': {
            'dataset': 'ZONA_HOMOGENEA_RURAL_CTM12',
            'features': [
                'R_ZONA_HOMO_GEOECONOMICA_CTM12',
                'R_ZONA_HOMOGENEA_FISICA_CTM12'
            ]
        }
    }
    
    # Verificar si el dataset principal tiene relaciones
    if dataset_principal not in relaciones:
        return resultados
        
    config = relaciones[dataset_principal]
    dataset_relacionado_path = os.path.join(gdb_path, config['dataset'])
    
    # Si el dataset relacionado no existe, retornar conteos en 0
    if not arcpy.Exists(dataset_relacionado_path):
        for fc in config['features']:
            resultados[fc] = 0
        return resultados
    
    # Contar elementos en las features específicas
    for fc in config['features']:
        fc_path = os.path.join(dataset_relacionado_path, fc)
        if arcpy.Exists(fc_path):
            try:
                result = arcpy.GetCount_management(fc_path)
                resultados[fc] = int(result.getOutput(0))
                logging.info(f"Conteo en feature relacionada {fc}: {resultados[fc]}")
            except:
                resultados[fc] = 0
                logging.warning(f"No se pudo contar elementos en {fc}, estableciendo en 0")
        else:
            resultados[fc] = 0
            logging.info(f"Feature class relacionada {fc} no existe, estableciendo en 0")
    
    return resultados

def procesar_dataset(dataset_path, dataset_name, gdb_path):
    """Procesa un dataset completo incluyendo features relacionadas"""
    resultados = {}
    try:
        logging.info(f"\nProcesando Dataset: {dataset_name}")
        
        if not arcpy.Exists(dataset_path):
            logging.error(f"Dataset no encontrado: {dataset_path}")
            return dataset_name, resultados
        
        # Limpiar y configurar workspace
        arcpy.env.workspace = None
        arcpy.ClearWorkspaceCache_management()
        arcpy.env.workspace = dataset_path
        
        # Listar feature classes
        fcs = arcpy.ListFeatureClasses()
        if not fcs:
            logging.warning(f"No se encontraron feature classes en {dataset_name}")
            return dataset_name, resultados
            
        logging.info(f"Feature Classes encontradas en {dataset_name}: {len(fcs)}")
        
        # Procesar cada feature class secuencialmente
        for fc in fcs:
            fc_path = os.path.join(dataset_path, fc)
            fc_name, count = contar_registros_fc(fc_path)
            resultados[fc_name] = count
        
        # Contar elementos en features relacionadas si aplica
        features_relacionadas = contar_features_relacionadas(gdb_path, dataset_name)
        resultados.update(features_relacionadas)
        
        features_procesadas = len(resultados)
        features_con_datos = sum(1 for count in resultados.values() if count > 0)
        logging.info(f"Dataset {dataset_name} procesado: {features_procesadas} features, "
                    f"{features_con_datos} con datos")
        
    except Exception as e:
        logging.error(f"Error procesando dataset {dataset_name}: {str(e)}")
    finally:
        try:
            arcpy.env.workspace = None
            arcpy.ClearWorkspaceCache_management()
        except:
            pass
    
    return dataset_name, resultados

def analizar_gdb(gdb_path, datasets_permitidos):
    """Analiza solo los datasets especificados en la configuración"""
    logging.info("\nIniciando análisis de la geodatabase...")
    arcpy.env.workspace = gdb_path
    
    datasets = [ds for ds in datasets_permitidos if arcpy.Exists(os.path.join(gdb_path, ds))]
    logging.info(f"Datasets a procesar: {len(datasets)}")
    
    resultados = {}
    total_datasets = len(datasets)
    procesados = 0
    
    for ds in datasets:
        dataset_path = os.path.join(gdb_path, ds)
        dataset_name, dataset_results = procesar_dataset(dataset_path, ds, gdb_path)
        resultados[dataset_name] = dataset_results
        procesados += 1
        logging.info(f"Progreso: {procesados}/{total_datasets} datasets procesados")
    
    logging.info("Análisis de geodatabase completado")
    return resultados

def optimizar_sqlite(sqlite_path):
    """Aplica optimizaciones a la base de datos SQLite"""
    logging.info("Aplicando optimizaciones a SQLite...")
    conn = sqlite3.connect(sqlite_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn

def crear_base_sqlite(sqlite_path, resultados):
    """Crea la base de datos SQLite con optimizaciones"""
    logging.info("\nCreando base de datos SQLite...")
    conn = optimizar_sqlite(sqlite_path)
    cursor = conn.cursor()
    
    try:
        conn.execute("BEGIN TRANSACTION")
        total_datasets = len(resultados)
        procesados = 0
        
        for dataset, features in resultados.items():
            procesados += 1
            logging.info(f"Creando tabla para dataset {dataset} ({procesados}/{total_datasets})")
            
            columnas = [f"{fc} INTEGER" for fc in features.keys()]
            columnas_str = ", ".join(columnas)
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {dataset} ({columnas_str})"
            cursor.execute(create_table_sql)
            
            valores = list(features.values())
            valores_str = ", ".join(["?" for _ in valores])
            insert_sql = f"INSERT INTO {dataset} VALUES ({valores_str})"
            cursor.execute(insert_sql, valores)
            
            logging.info(f"Datos insertados en tabla {dataset}")
        
        conn.commit()
        logging.info("Transacción completada exitosamente")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error en la transacción: {str(e)}")
        raise e
    finally:
        conn.close()
        logging.info("Conexión a base de datos cerrada")

def main():
    try:
        configurar_logging()
        tiempo_inicio = time.time()
        
        logging.info("=== INICIANDO PROCESO DE ANÁLISIS ===")
        
        # Obtener ruta base y construir ruta a la GDB
        ruta_base = obtener_ruta_base()
        gdb_path = os.path.join(ruta_base, "Files", "Temporary_Files", "MODELO_LADM_1_2")
        
        # Buscar la primera GDB en el directorio
        for item in os.listdir(gdb_path):
            if item.endswith('.gdb'):
                gdb_path = os.path.join(gdb_path, item)
                break
        else:
            raise Exception("No se encontró ninguna geodatabase en el directorio")
        
        # Verificar conexión a la GDB
        if not verificar_conexion_gdb(gdb_path):
            raise Exception("No se pudo establecer una conexión válida con la geodatabase")
        
        # Leer configuración de datasets a procesar
        datasets_permitidos = leer_datasets_configurados()
        logging.info(f"Datasets configurados para procesar: {datasets_permitidos}")
        
        sqlite_path = crear_ruta_sqlite()
        resultados = analizar_gdb(gdb_path, datasets_permitidos)
        crear_base_sqlite(sqlite_path, resultados)
        
        tiempo_total = time.time() - tiempo_inicio
        
        # Calcular totales para el resumen
        total_registros = 0
        features_con_registros = 0
        features_sin_registros = 0
        
        logging.info("\n=== RESUMEN DEL PROCESO ===")
        logging.info(f"Tiempo total de ejecución: {tiempo_total:.2f} segundos")
        logging.info(f"Datasets procesados: {len(resultados)}")
        logging.info(f"Base de datos creada en: {sqlite_path}")
        
        # Calcular y mostrar estadísticas por dataset
        logging.info("\n=== ESTADÍSTICAS DE REGISTROS ===")
        for dataset, features in resultados.items():
            dataset_total = sum(features.values())
            features_count = len(features)
            empty_features = sum(1 for count in features.values() if count == 0)
            filled_features = features_count - empty_features
            
            total_registros += dataset_total
            features_con_registros += filled_features
            features_sin_registros += empty_features
            
            if dataset_total > 0:
                logging.info(f"\nDataset: {dataset}")
                logging.info(f"  Total registros: {dataset_total:,}")
                logging.info(f"  Features con datos: {filled_features}")
                logging.info(f"  Features vacías: {empty_features}")
        
        logging.info("\n=== TOTALES GENERALES ===")
        logging.info(f"Total de registros en la geodatabase: {total_registros:,}")
        logging.info(f"Feature Classes con registros: {features_con_registros}")
        logging.info(f"Feature Classes sin registros: {features_sin_registros}")
        logging.info("=== PROCESO COMPLETADO ===")
        
    except Exception as e:
        logging.error(f"\nERROR CRÍTICO: {str(e)}")
        logging.error("El proceso no se completó correctamente")

if __name__ == "__main__":
    main()