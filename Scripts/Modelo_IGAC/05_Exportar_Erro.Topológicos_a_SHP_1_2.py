import arcpy
import os
from pathlib import Path
import logging
from datetime import datetime
import json
import re
import sys
sys.stdout.reconfigure(encoding='utf-8')
# Configuración de datasets a procesar

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    proyecto_dir = os.path.abspath(os.path.join(script_dir, '..', '..'))
    config_path = os.path.join(proyecto_dir, "Files", "Temporary_Files", "array_config.txt")

    # Leer el archivo y filtrar solo los datasets activos
    DATASETS_TO_PROCESS = []
    with open(config_path, 'r') as f:
        contenido = f.read().strip()
        # Evaluar el contenido como lista de Python si está en formato de lista
        if contenido.startswith('[') and contenido.endswith(']'):
            import ast
            datasets = ast.literal_eval(contenido)
        else:
            # Si no está en formato de lista, procesar línea por línea
            datasets = [line.strip() for line in contenido.split('\n')]
        
        DATASETS_TO_PROCESS = [ds for ds in datasets if ds and not ds.startswith('#')]

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


    
def setup_logging():
    """Configura el sistema de logging para el script"""
    # Encontrar la ruta base del proyecto
    # Empezar desde el directorio del script actual
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    
    # Subir dos niveles para llegar a la raíz del proyecto (desde Modelo_IGAC/script.py hasta GeoValidaTool)
    ruta_proyecto = os.path.abspath(os.path.join(ruta_actual, '..', '..'))


    
    # Construir la ruta completa para los logs
    log_directory = os.path.join(ruta_proyecto,  "Files", "Temporary_Files","MODELO_IGAC", "logs")
    
    # Crear el directorio si no existe
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_directory, f"topology_export_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def find_project_root():
    """
    Encuentra la raíz del proyecto verificando la estructura de directorios esperada.
    """
    # Empezar desde el directorio del script actual
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    
    # Subir dos niveles para llegar a la raíz del proyecto (desde Modelo_IGAC/script.py hasta GeoValidaTool)
    ruta_proyecto = os.path.abspath(os.path.join(ruta_actual, '..', '..'))
    
    # Verifica la estructura característica del proyecto
    rutas_requeridas = [
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC"),
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "array_config.txt"),
        os.path.join(ruta_proyecto, "Scripts")
    ]
    
    # Para debug, imprimir las rutas que está verificando
    print("\nVerificando rutas del proyecto:")
    for ruta in rutas_requeridas:
        existe = os.path.exists(ruta)
        print(f"Ruta: {ruta}")
        print(f"¿Existe?: {existe}")
    
    if all(os.path.exists(ruta) for ruta in rutas_requeridas):
        print(f"\nRaíz del proyecto encontrada en: {ruta_proyecto}")
        
        # Crear directorios necesarios si no existen
        logs_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "logs")
        topology_errors_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "Topology_Errors")
        
        for directory in [logs_dir, topology_errors_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                print(f"Directorio creado: {directory}")
        
        return ruta_proyecto
    
    raise Exception(
        "No se encontró la raíz del proyecto.\n"
        "Verifique que está ejecutando el script desde dentro del proyecto "
        "y que existe la siguiente estructura:\n"
        "- Files/Temporary_Files/MODELO_IGAC\n"
        "- Files/Temporary_Files/array_config.txt\n"
        "- Files/Temporary_Files/MODELO_IGAC/logs (se creará automáticamente)\n"
        "- Files/Temporary_Files/MODELO_IGAC/Topology_Errors (se creará automáticamente)"
    )
def find_geodatabases(root_path):
    """Busca geodatabases en el directorio Temporary_Files"""
    try:
        temp_files_path = os.path.join(root_path, "Files", "Temporary_Files", "MODELO_IGAC")
        gdbs = []
        
        print(f"\nBuscando GDBs en: {temp_files_path}")
        
        # Buscar archivos .gdb en el directorio
        for item in os.listdir(temp_files_path):
            if item.endswith('.gdb'):
                gdb_path = os.path.join(temp_files_path, item)
                if os.path.isdir(gdb_path):
                    print(f"GDB encontrada: {item}")
                    gdbs.append(gdb_path)
        
        if not gdbs:
            print("WARNING: No se encontraron geodatabases (.gdb)")
            
        return gdbs
    except Exception as e:
        raise Exception(f"Error buscando geodatabases: {str(e)}")

def export_topology_errors(gdb, dataset_name, logger):
    try:
        dataset_path = os.path.join(gdb, dataset_name)
        print(f"\nProcesando Dataset: {dataset_name}")
        print(f"Ruta: {dataset_path}")
        
        if not arcpy.Exists(dataset_path):
            print(f"WARNING: Dataset {dataset_name} no encontrado")
            logger.warning(f"Dataset {dataset_name} no encontrado en {gdb}")
            return
        
        arcpy.env.workspace = dataset_path
        topologies = arcpy.ListDatasets("*", "Topology")
        
        if not topologies:
            print(f"WARNING: No se encontró topología en {dataset_name}")
            logger.warning(f"No se encontró topología en el dataset {dataset_name}")
            return
            
        topology_name = topologies[0]
        print(f"Topología encontrada: {topology_name}")
        
        output_folder = os.path.join(os.path.dirname(gdb), "Topology_Errors")
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            print(f"Carpeta de salida creada: {output_folder}")
        
        date_stamp = datetime.now().strftime("%Y%m%d")
        output_gdb = os.path.join(output_folder, f"topology_errors_{date_stamp}.gdb")
        
        if not arcpy.Exists(output_gdb):
            arcpy.CreateFileGDB_management(output_folder, f"topology_errors_{date_stamp}")
            print(f"GDB de salida creada: {output_gdb}")
        
        
        output_basename = f"{dataset_name}_errors"
        print(f"Exportando errores...")
        
        topology_path = os.path.join(dataset_path, topology_name)
        print(f"Validando topología...")
        arcpy.ValidateTopology_management(topology_path)
        arcpy.ExportTopologyErrors_management(topology_path, output_gdb, output_basename)

        # Analizar los errores exportados
        error_poly = os.path.join(output_gdb, f"{dataset_name}_errors_poly")
        print("\nAnálisis de errores topológicos:")
        with arcpy.da.SearchCursor(error_poly, ["OriginObjectClassName", "DestinationObjectClassName", "RuleType", "RuleDescription"]) as cursor:
            unique_rules = set()
            """
            for row in cursor:
                if row[0] == "U_UNIDAD_CTM12" or row[0] == "R_UNIDAD_CTM12":
                    print(f"Origen: {row[0]}")
                    print(f"Destino: {row[1]}")
                    print(f"Tipo: {row[2]}")
                    print(f"Descripción: {row[3]}")
                    print("-" * 50)
            """
        error_types = ["_point", "_line", "_poly"]
        total_errors = 0
        
        print("\nResumen de errores exportados:")
        print("-" * 30)
        
        for error_type in error_types:
            error_fc = os.path.join(output_gdb, output_basename + error_type)
            if arcpy.Exists(error_fc):
                if error_type == "_poly":
                    with arcpy.da.UpdateCursor(error_fc, ["OriginObjectClassName", "DestinationObjectClassName"]) as cursor:
                        for row in cursor:
                            if row[0] == "U_MANZANA_CTM12" or row[1] == "U_MANZANA_CTM12":
                                row = (row[1], row[0])
                                cursor.updateRow(row)
                
                count = int(arcpy.GetCount_management(error_fc)[0])
                total_errors += count
                print(f"Errores tipo {error_type}: {count}")
                
                try:
                    arcpy.AddSpatialIndex_management(error_fc)
                    print(f"Índice espacial creado para {error_type}")
                except:
                    print(f"WARNING: No se pudo crear índice espacial en {error_type}")
        
        print(f"\nTotal de errores encontrados: {total_errors}")
        print("=" * 50)
        
    except Exception as e:
        logger.error(f"Error procesando dataset {dataset_name}: {str(e)}")
        logger.error("Detalles completos:", exc_info=True)
        print(f"\nERROR: {str(e)}")
             
def main():
    try:
        # Configuración inicial
        logger = setup_logging()
        logger.info("Iniciando proceso de exportación de errores topológicos")
        
        print("\n" + "="*50)
        print("INICIO DE PROCESO DE EXPORTACIÓN DE ERRORES TOPOLÓGICOS")
        print("="*50)
        
        arcpy.env.overwriteOutput = True
        
        # Encontrar raíz del proyecto y geodatabases
        project_root = find_project_root()
        gdbs = find_geodatabases(project_root)
        
        if not gdbs:
            print("\nWARNING: No se encontraron geodatabases para procesar")
            logger.warning("No se encontraron geodatabases para procesar")
            return
        
        # Procesar cada geodatabase
        for gdb in gdbs:
            print(f"\nProcesando GDB: {os.path.basename(gdb)}")
            print("-" * 50)
            
            # Procesar solo los datasets configurados
            for dataset_name in DATASETS_TO_PROCESS:
                if dataset_name.startswith("#"):
                    continue
                export_topology_errors(gdb, dataset_name, logger)
        
        print("\n" + "="*50)
        print("PROCESO COMPLETADO EXITOSAMENTE")
        print("="*50)
        logger.info("Proceso completado exitosamente")
        
    except Exception as e:
        logger.error(f"Error crítico en el proceso: {str(e)}")
        logger.error("Detalles completos:", exc_info=True)
        print(f"\nERROR CRÍTICO: {str(e)}")

if __name__ == "__main__":
    main()