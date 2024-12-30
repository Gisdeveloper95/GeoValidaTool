import arcpy
import os
from datetime import datetime
import time
import json
import re
import sys
sys.stdout.reconfigure(encoding='utf-8')
"""Esta version SI contempla las zonas homogeneas dentro de las capas urbano_ctm12 ni rural_Ctm12,
ya que para evaluarlas implica eliminarlas de  su dataset original...

por lo tantno no se pueden crear topologias de 
#"ZONA_HOMOGENEA_URBANO_CTM12",
#"ZONA_HOMOGENEA_RURAL_CTM12",
"""

# Lista de datasets a procesar

# Carga inicial de DATASETS_TO_PROCESS desde archivoS

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
    config_path = os.path.join(proyecto_dir,"Files", "Temporary_Files","array_config.txt")

    # Leer el archivo y filtrar solo los datasets activos
    DATASETS_TO_PROCESS = []
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()  # Limpiar espacios alrededor
            if line and not line.startswith('#'):
                # Eliminar posibles comillas, comas o corchetes en el nombre del dataset
                dataset_name = line.strip('",[]').strip()
                if dataset_name:  # Solo agregar si no está vacío
                    DATASETS_TO_PROCESS.append(dataset_name)

    # Imprimir el contenido de DATASETS_TO_PROCESS para depuración
    print("\nConfiguración de datasets cargada:")
    print("--------------------------------")
    print("Datasets que serán procesados:")
    for ds in DATASETS_TO_PROCESS:
        print(f"  - {ds}")
    print("--------------------------------\n")

except Exception as e:
    print(f"Error al cargar configuración: {str(e)}")
    # Configuración por defecto en caso de error
    DATASETS_TO_PROCESS = ["URBANO_CTM12", "RURAL_CTM12"]
    print("\nUsando configuración por defecto:")
    print("--------------------------------")
    print("Datasets que serán procesados:")
    for ds in DATASETS_TO_PROCESS:
        print(f"  - {ds}")
    print("--------------------------------\n")



def log_message(message):
    """
    Imprime un mensaje con marca de tiempo
    """
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

def copy_homogeneous_zones(gdb_path):
    """
    Mueve los feature classes de zonas homogéneas según los datasets activos.
    """
    try:
        log_message("Iniciando proceso de movimiento de zonas homogéneas...")
        
        # Definir las relaciones entre datasets y sus feature classes
        dataset_mappings = {
            "URBANO_CTM12": {
                "source_dataset": "ZONA_HOMOGENEA_URBANO_CTM12",
                "feature_classes": [
                    "U_ZONA_HOMO_GEOECONOMICA_CTM12",
                    "U_ZONA_HOMOGENEA_FISICA_CTM12"
                ]
            },
            "RURAL_CTM12": {
                "source_dataset": "ZONA_HOMOGENEA_RURAL_CTM12",
                "feature_classes": [
                    "R_ZONA_HOMO_GEOECONOMICA_CTM12",
                    "R_ZONA_HOMOGENEA_FISICA_CTM12"
                ]
            },
            "URBANO":{
                "source_dataset": "ZONA_HOMOGENEA_URBANO",
                "feature_classes": [
                    "U_ZONA_HOMOGENEA_GEOECONOMICA",
                    "U_ZONA_HOMOGENEA_FISICA"
                ]

            },
            "RURAL":{
                "source_dataset": "ZONA_HOMOGENEA_RURAL",
                "feature_classes": [
                    "R_ZONA_HOMOGENEA_GEOECONOMICA",
                    "R_ZONA_HOMOGENEA_FISICA"
                ]

            }
        }
        
        # Procesar cada dataset activo
        for dataset in DATASETS_TO_PROCESS:
            if dataset in dataset_mappings:
                mapping = dataset_mappings[dataset]
                source_dataset = mapping["source_dataset"]
                feature_classes = mapping["feature_classes"]
                
                log_message(f"Procesando zonas homogéneas para dataset: {dataset}")
                
                # Mover cada feature class
                for fc in feature_classes:
                    source_path = os.path.join(gdb_path, source_dataset, fc)
                    target_path = os.path.join(gdb_path, dataset, fc)
                    temp_path = os.path.join(gdb_path, f"TEMP_{fc}")  # Ruta temporal
                    
                    # Verificar si el feature class existe en el origen
                    if arcpy.Exists(source_path):
                        log_message(f"  Moviendo {fc} de {source_dataset} a {dataset}")
                        
                        try:
                            # 1. Primero, copiar a una ubicación temporal con nombre único
                            arcpy.CopyFeatures_management(source_path, temp_path)
                            
                            # 2. Eliminar el feature class original
                            arcpy.Delete_management(source_path)
                            
                            # 3. Si existe el feature class en el destino, eliminarlo
                            if arcpy.Exists(target_path):
                                arcpy.Delete_management(target_path)
                            
                            # 4. Mover de la ubicación temporal al destino final
                            arcpy.CopyFeatures_management(temp_path, target_path)
                            
                            # 5. Eliminar el temporal
                            arcpy.Delete_management(temp_path)
                            
                            log_message(f"  Feature class {fc} movido exitosamente")
                            
                        except arcpy.ExecuteError as e:
                            log_message(f"  Error al mover {fc}: {arcpy.GetMessages(2)}")
                            # Intentar limpiar archivos temporales si hubo error
                            if arcpy.Exists(temp_path):
                                arcpy.Delete_management(temp_path)
                    else:
                        log_message(f"  ADVERTENCIA: Feature class {fc} no encontrado en {source_dataset}")
                        
    except arcpy.ExecuteError:
        log_message("ERROR DE ARCPY en movimiento de zonas homogéneas:")
        log_message(arcpy.GetMessages(2))
    except Exception as e:
        log_message(f"ERROR GENERAL en movimiento de zonas homogéneas: {str(e)}")
def count_homogeneous_zones(gdb_path):
    """
    Cuenta los registros de las zonas homogéneas y los guarda en SQLite
    """
    try:
        log_message("Contando registros de zonas homogéneas...")
        
        # Configurar ruta SQLite
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
        db_path = os.path.join(proyecto_dir, "Files", "Temporary_Files","MODELO_IGAC", "db", "conteo_elementos.db")
        
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Dataset mappings (ya definido)
        
        dataset_mappings = {
            "URBANO_CTM12": {
                "source_dataset": "ZONA_HOMOGENEA_URBANO_CTM12",
                "feature_classes": [
                    "U_ZONA_HOMO_GEOECONOMICA_CTM12",
                    "U_ZONA_HOMOGENEA_FISICA_CTM12"
                ]
            },
            "RURAL_CTM12": {
                "source_dataset": "ZONA_HOMOGENEA_RURAL_CTM12",
                "feature_classes": [
                    "R_ZONA_HOMO_GEOECONOMICA_CTM12",
                    "R_ZONA_HOMOGENEA_FISICA_CTM12"
                ]
            },
            "URBANO":{
                "source_dataset": "ZONA_HOMOGENEA_URBANO",
                "feature_classes": [
                    "U_ZONA_HOMOGENEA_GEOECONOMICA",
                    "U_ZONA_HOMOGENEA_FISICA"
                ]

            },
            "RURAL":{
                "source_dataset": "ZONA_HOMOGENEA_RURAL",
                "feature_classes": [
                    "R_ZONA_HOMOGENEA_GEOECONOMICA",
                    "R_ZONA_HOMOGENEA_FISICA"
                ]

            }
        }
        
        # Procesar cada dataset activo
        for dataset in DATASETS_TO_PROCESS:
            if dataset in dataset_mappings:
                mapping = dataset_mappings[dataset]
                feature_classes = mapping["feature_classes"]
                
                # Verificar si la tabla existe
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{dataset}'")
                table_exists = cursor.fetchone() is not None
                
                if table_exists:
                    # Obtener columnas existentes
                    cursor.execute(f"PRAGMA table_info({dataset})")
                    existing_columns = [row[1] for row in cursor.fetchall()]
                    
                    # Agregar columnas nuevas si no existen
                    for fc in feature_classes:
                        if fc not in existing_columns:
                            cursor.execute(f"ALTER TABLE {dataset} ADD COLUMN {fc} INTEGER")
                    
                    # Contar registros
                    counts = {}
                    for fc in feature_classes:
                        fc_path = os.path.join(gdb_path, dataset, fc)
                        if arcpy.Exists(fc_path):
                            count = int(arcpy.GetCount_management(fc_path)[0])
                            counts[fc] = count
                        else:
                            counts[fc] = 0
                    
                    # Actualizar la tabla
                    update_sql = f"UPDATE {dataset} SET "
                    update_sql += ", ".join([f"{fc} = ?" for fc in feature_classes])
                    values = [counts[fc] for fc in feature_classes]
                    
                    cursor.execute(update_sql, values)
                    conn.commit()
                    
                    log_message(f"Actualizada tabla {dataset} con conteos de zonas homogéneas")
        
        conn.close()
        log_message("Proceso de conteo de zonas homogéneas completado")
        
    except sqlite3.Error as e:
        log_message(f"Error SQLite: {str(e)}")
    except Exception as e:
        log_message(f"Error general: {str(e)}")

def get_topology_name(dataset):
    """
    Obtiene el nombre correcto de la topología basado en el dataset
    """
    dataset_upper = dataset.upper()
    if "ZONA_HOMOGENEA" in dataset_upper:
        # Manejar primero los casos CTM12
        if "URBANO_CTM12" in dataset_upper:
            return "ZONA_HOMOGENEA_URBANO_CTM12_Topology"
        elif "RURAL_CTM12" in dataset_upper:
            return "ZONA_HOMOGENEA_RURAL_CTM12_Topology"
        elif "URBANO" in dataset_upper:
            return "ZONA_HOMOGENEA_URBANO_Topology"
        elif "RURAL" in dataset_upper:
            return "ZONA_HOMOGENEA_RURAL_Topology"
    else:
        # Manejar los casos no ZONA_HOMOGENEA
        if "URBANO_CTM12" in dataset_upper:
            return "URBANO_CTM12_Topology"
        elif "RURAL_CTM12" in dataset_upper:
            return "RURAL_CTM12_Topology"
        elif "URBANO" in dataset_upper:
            return "URBANO_Topology"
        elif "RURAL" in dataset_upper:
            return "RURAL_Topology"
    return None

def delete_topologies(gdb_path, dataset, dataset_mappings):
    """
    Elimina las topologías tanto del dataset principal como de su dataset asociado
    antes de cualquier movimiento de feature classes.
    """
    try:
        log_message(f"Iniciando eliminación de topologías para dataset: {dataset}")
        topologies_removed = 0
        fc_removed = 0

        # 1. Eliminar topologías del dataset principal
        dataset_path = os.path.join(gdb_path, dataset)
        if arcpy.Exists(dataset_path):
            arcpy.env.workspace = dataset_path
            log_message(f"  Buscando y eliminando topologías en: {dataset}")
            
            # Eliminar topologías
            for item in arcpy.ListDatasets("*", "Topology"):
                log_message(f"    Eliminando topología: {item}")
                arcpy.Delete_management(os.path.join(dataset_path, item))
                topologies_removed += 1
            
            # Eliminar feature classes con "Topology"
            for fc in arcpy.ListFeatureClasses("*Topology*"):
                log_message(f"    Eliminando feature class: {fc}")
                arcpy.Delete_management(os.path.join(dataset_path, fc))
                fc_removed += 1

        # 2. Eliminar topologías del dataset asociado si existe
        if dataset in dataset_mappings:
            associated_dataset = dataset_mappings[dataset]["source_dataset"]
            associated_dataset_path = os.path.join(gdb_path, associated_dataset)
            
            if arcpy.Exists(associated_dataset_path):
                arcpy.env.workspace = associated_dataset_path
                log_message(f"  Buscando y eliminando topologías en dataset asociado: {associated_dataset}")
                
                # Eliminar topologías
                for item in arcpy.ListDatasets("*", "Topology"):
                    log_message(f"    Eliminando topología asociada: {item}")
                    arcpy.Delete_management(os.path.join(associated_dataset_path, item))
                    topologies_removed += 1
                
                # Eliminar feature classes con "Topology"
                for fc in arcpy.ListFeatureClasses("*Topology*"):
                    log_message(f"    Eliminando feature class asociado: {fc}")
                    arcpy.Delete_management(os.path.join(associated_dataset_path, fc))
                    fc_removed += 1

        log_message(f"  Total de elementos eliminados: {topologies_removed} topologías y {fc_removed} feature classes")
        return True

    except arcpy.ExecuteError as e:
        log_message(f"Error de ArcPy al eliminar topologías: {arcpy.GetMessages(2)}")
        return False
    except Exception as e:
        log_message(f"Error general al eliminar topologías: {str(e)}")
        return False

def repair_geometries(gdb_path, dataset_name):
    """
    Repara las geometrías de todos los feature classes en un dataset
    directamente, sin crear temporales
    """
    try:
        workspace = os.path.join(gdb_path, dataset_name)
        arcpy.env.workspace = workspace
        
        # Obtener lista de feature classes en el dataset
        feature_classes = [fc for fc in arcpy.ListFeatureClasses() if not fc.startswith("TEMP_")]
        
        log_message(f"Iniciando reparación de geometrías para dataset {dataset_name}")
        
        # Reparar cada feature class
        for fc in feature_classes:
            try:
                fc_path = os.path.join(workspace, fc)
                log_message(f"  Reparando geometría de {fc}")
                arcpy.RepairGeometry_management(fc_path, "DELETE_NULL")
                log_message(f"  Geometría reparada para {fc}")
                
            except Exception as e:
                log_message(f"  Error reparando {fc}: {str(e)}")
                continue
                
        return True
        
    except Exception as e:
        log_message(f"Error en reparación de geometrías: {str(e)}")
        return False

def process_topology():
    """
    Procesa las topologías en una geodatabase
    """
    try:
        start_time = time.time()
        log_message("Iniciando proceso de gestión de topologías...")
        
        # Configurar rutas
        log_message("Configurando rutas del proyecto...")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
        temp_dir = os.path.join(proyecto_dir, "Files", "Temporary_Files","MODELO_IGAC")
        
        # Encontrar la geodatabase
        log_message("Buscando geodatabase en directorio temporal...")
        gdb_path = None
        for file in os.listdir(temp_dir):
            if file.endswith(".gdb"):
                gdb_path = os.path.join(temp_dir, file)
                log_message(f"Geodatabase encontrada: {file}")
                break
                
        if not gdb_path:
            raise Exception("No se encontró ninguna geodatabase en el directorio temporal.")
        # Definir dataset_mappings
        dataset_mappings = {
            "URBANO_CTM12": {
                "source_dataset": "ZONA_HOMOGENEA_URBANO_CTM12",
                "feature_classes": [
                    "U_ZONA_HOMO_GEOECONOMICA_CTM12",
                    "U_ZONA_HOMOGENEA_FISICA_CTM12"
                ]
            },
            "RURAL_CTM12": {
                "source_dataset": "ZONA_HOMOGENEA_RURAL_CTM12",
                "feature_classes": [
                    "R_ZONA_HOMO_GEOECONOMICA_CTM12",
                    "R_ZONA_HOMOGENEA_FISICA_CTM12"
                ]
            },
            "URBANO":{
                "source_dataset": "ZONA_HOMOGENEA_URBANO",
                "feature_classes": [
                    "U_ZONA_HOMOGENEA_GEOECONOMICA",
                    "U_ZONA_HOMOGENEA_FISICA"
                ]

            },
            "RURAL":{
                "source_dataset": "ZONA_HOMOGENEA_RURAL",
                "feature_classes": [
                    "R_ZONA_HOMOGENEA_GEOECONOMICA",
                    "R_ZONA_HOMOGENEA_FISICA"
                ]

            }
        }
        
        # Obtener lista de datasets
        log_message("Obteniendo lista de datasets...")
        arcpy.env.workspace = gdb_path
        all_datasets = arcpy.ListDatasets("*", "Feature")
        
        # Filtrar solo los datasets que queremos procesar
        datasets = [ds for ds in all_datasets if ds in DATASETS_TO_PROCESS]
        total_datasets = len(datasets)
        
        log_message(f"Se encontraron {len(all_datasets)} datasets en total")
        log_message(f"Se procesarán {total_datasets} datasets según la configuración")
        
        # PRIMERO: Eliminar todas las topologías necesarias
        for dataset in datasets:
            if not delete_topologies(gdb_path, dataset, dataset_mappings):
                log_message(f"Error al eliminar topologías para {dataset}, saltando al siguiente dataset")
                continue
        
        # SEGUNDO: Mover los feature classes
        copy_homogeneous_zones(gdb_path)
        count_homogeneous_zones(gdb_path)
        
        # TERCERO: Reparar geometrías
        for dataset in datasets:
            if not repair_geometries(gdb_path, dataset):
                log_message(f"Error al reparar geometrías en {dataset}")
                continue
        
        # CUARTO: Crear las topologías e incluir los features
        successful_topologies = []
        for index, dataset in enumerate(datasets, 1):
            try:
                log_message(f"\nProcesando dataset {index}/{total_datasets}: {dataset}")
                
                topology_name = get_topology_name(dataset)
                if not topology_name:
                    log_message(f"  Dataset {dataset} no requiere topología - Omitiendo")
                    continue
                
                dataset_path = os.path.join(gdb_path, dataset)
                arcpy.env.workspace = dataset_path
                
                # Crear nueva topología
                log_message(f"  Creando nueva topología: {topology_name}")
                topology_path = os.path.join(dataset_path, topology_name)
                
                arcpy.CreateTopology_management(
                    dataset_path,
                    topology_name,
                    0.001
                )
                
                # Añadir feature classes a la topología
                log_message("  Añadiendo feature classes...")
                fc_count = 0
                for fc in arcpy.ListFeatureClasses():
                    if not fc.startswith("TEMP_") and "Topology" not in fc:
                        try:
                            arcpy.AddFeatureClassToTopology_management(
                                topology_path,
                                fc,
                                1
                            )
                            fc_count += 1
                            log_message(f"    Feature class añadido: {fc}")
                        except Exception as e:
                            log_message(f"    Error añadiendo feature class {fc}: {str(e)}")
                            continue
                
                log_message(f"  Se añadieron {fc_count} feature classes a la topología")
                
                # Guardar información de la topología creada
                successful_topologies.append({
                    "dataset": dataset,
                    "topology_name": topology_name,
                    "topology_path": topology_path
                })
                
            except Exception as e:
                log_message(f"  Error procesando dataset {dataset}: {str(e)}")
                continue
        
        end_time = time.time()
        total_time = end_time - start_time
        log_message(f"\nProceso completado en {total_time:.2f} segundos")
        
        return successful_topologies
        
    except Exception as e:
        log_message(f"ERROR GENERAL: {str(e)}")
        return []
if __name__ == "__main__":
    successful_topologies = process_topology()
    log_message(f"Topologías creadas exitosamente: {len(successful_topologies)}")
    log_message("Ejecute la parte No.4 para aplicar las reglas topológicas")