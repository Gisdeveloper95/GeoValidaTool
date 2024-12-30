import arcpy
import os
import sqlite3
from pathlib import Path
import logging
from datetime import datetime
import json
import shutil

import sys
import subprocess

sys.stdout.reconfigure(encoding='utf-8')
"""NOTA:

SI SE TERMINA EL MODELO DE EXCEPCION POR TOPOLOGIAS, ESTE CODIGO DEBE CONECTAR CON LA GDB GENERADA EN EL PASO 4
YA QUE EL CODIGO CONTINUARA CON LA GEODATABASE de errores en la carpeta Topology_Errors/topology_Errors_***_.gdb."""

# Datasets que pueden existir
POSSIBLE_DATASETS = [
    "ZONA_HOMOGENEA_RURAL",
    "ZONA_HOMOGENEA_URBANO",
    "RURAL_CTM12",
    "URBANO",
    "ZONA_HOMOGENEA_URBANO_CTM12",
    "ZONA_HOMOGENEA_RURAL_CTM12",
    "URBANO_CTM12",
    "RURAL"
]

# Campos a excluir
EXCLUDED_FIELDS = ['SHAPE', 'SHAPE_Length', 'SHAPE_Area', 'SHAPE.STLength()', 'SHAPE.STArea()']

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
        db_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "db")
        topology_errors_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "Topology_Errors")
        logs_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "logs")
        
        for directory in [db_dir, topology_errors_dir, logs_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                print(f"Directorio creado: {directory}")
        
        return Path(ruta_proyecto)
    
    raise Exception(
        "No se encontró la raíz del proyecto.\n"
        "Verifique que está ejecutando el script desde dentro del proyecto "
        "y que existe la siguiente estructura:\n"
        "- Files/Temporary_Files/MODELO_IGAC\n"
        "- Files/Temporary_Files/array_config.txt\n"
        "- Files/Temporary_Files/MODELO_IGAC/db\n"
        "- Files/Temporary_Files/MODELO_IGAC/Topology_Errors"
    )

def find_latest_topology_gdb(root_path):
    """Encuentra la GDB de topología más reciente"""
    try:
        topology_path = root_path / "Files" / "Temporary_Files" / "MODELO_IGAC" / "Topology_Errors"
        print(f"\nBuscando GDBs en: {topology_path}")
        
        if not topology_path.exists():
            raise Exception(f"No se encontró la carpeta Topology_Errors en {topology_path}")
            
        gdbs = list(topology_path.glob("topology_errors_*.gdb"))
        if not gdbs:
            raise Exception(f"No se encontraron GDBs de topología en {topology_path}")
            
        # Ordenar por fecha de modificación y tomar la más reciente
        latest_gdb = max(gdbs, key=lambda x: x.stat().st_mtime)
        print(f"GDB más reciente encontrada: {latest_gdb}")
        return str(latest_gdb)
        
    except Exception as e:
        print(f"Error buscando GDB de topología: {str(e)}")
        raise

def setup_logging():
    """Configura el sistema de logging"""
    try:
        # Encontrar la raíz del proyecto
        project_root = find_project_root()
        
        # Crear la ruta para los logs
        log_directory = project_root / "Files" / "Temporary_Files" / "MODELO_IGAC" / "logs"
        log_directory.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_directory / f"topology_to_sqlite_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(str(log_file)),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger(__name__)
        logger.info(f"Log file creado en: {log_file}")
        return logger
        
    except Exception as e:
        print(f"Error configurando logging: {str(e)}")
        raise

def get_field_type_sql(field_type):
    """Convierte tipos de campo de ArcGIS a SQLite"""
    type_mapping = {
        'Integer': 'INTEGER',
        'SmallInteger': 'INTEGER',
        'Double': 'REAL',
        'Single': 'REAL',
        'String': 'TEXT',
        'Date': 'TEXT',
        'OID': 'INTEGER'
    }
    return type_mapping.get(field_type, 'TEXT')

def create_sqlite_table(cursor, table_name, fields):
    """Crea una tabla en SQLite con los campos especificados"""
    field_definitions = []
    
    for field in fields:
        if field.name not in EXCLUDED_FIELDS and not field.name.upper().startswith('SHAPE'):
            field_type = get_field_type_sql(field.type)
            # Manejar específicamente el campo isException
            if field.name == 'isException':
                field_type = 'INTEGER'
            field_definitions.append(f"{field.name} {field_type}")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(field_definitions)}
    )
    """
    cursor.execute(create_table_sql)


def process_feature_class(cursor, feature_class, table_name):
    """Procesa un feature class y guarda sus registros en SQLite"""
    try:
        # Obtener los campos
        fields = arcpy.ListFields(feature_class)
        field_names = [field.name for field in fields 
                      if field.name not in EXCLUDED_FIELDS and 
                      not field.name.upper().startswith('SHAPE')]
        
        # Crear la sentencia INSERT
        placeholders = ','.join(['?' for _ in field_names])
        insert_sql = f"INSERT INTO {table_name} ({','.join(field_names)}) VALUES ({placeholders})"
        
        # Procesar los registros
        print(f"Procesando registros de {os.path.basename(feature_class)}...")
        count = 0
        
        # Conjunto para rastrear registros ya insertados
        inserted_records = set()
        
        with arcpy.da.SearchCursor(feature_class, field_names) as cursor_arcpy:
            for row in cursor_arcpy:
                # Convertir valores según sea necesario
                processed_row = []
                for i, value in enumerate(row):
                    if field_names[i] == 'isException':
                        processed_row.append(int(value) if value is not None else 0)
                    elif isinstance(value, (bytes, bytearray)):
                        processed_row.append(str(value))
                    else:
                        processed_row.append(value)
                
                # Crear una clave única excluyendo OBJECTID
                key_fields = [field_names[i] for i in range(len(field_names)) 
                            if field_names[i] not in ['OBJECTID', 'isException']]
                key_values = tuple(processed_row[i] for i in range(len(processed_row)) 
                                 if field_names[i] in key_fields)
                
                # Si el registro no existe o es una excepción, insertarlo
                if key_values not in inserted_records:
                    cursor.execute(insert_sql, tuple(processed_row))
                    inserted_records.add(key_values)
                    count += 1
        
        print(f"Total de registros procesados: {count}")
        return count
        
    except Exception as e:
        print(f"Error procesando feature class {feature_class}: {str(e)}")
        return 0
       
def main():
    try:
        # Encontrar la raíz del proyecto primero
        project_root = find_project_root()
        
        # Configuración inicial
        logger = setup_logging()
        logger.info("Iniciando proceso de exportación a SQLite")
        print("\nINICIANDO PROCESO DE EXPORTACIÓN A SQLITE")
        print("="*50)
        
        # Encontrar las rutas necesarias
        topology_gdb = find_latest_topology_gdb(project_root)
        
        print(f"GDB de topología encontrada: {topology_gdb}")
        
        # Crear directorio para la base de datos
        db_dir = project_root / "Files" / "Temporary_Files"/ "MODELO_IGAC" / "db"
        db_dir.mkdir(parents=True, exist_ok=True)
        
        db_path = db_dir / "registro_errores.db"
        print(f"Base de datos SQLite: {db_path}")
        
        # Conectar a SQLite
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            
            # Procesar cada dataset posible
            arcpy.env.workspace = topology_gdb
            total_records = 0
            
            for dataset in POSSIBLE_DATASETS:
                print(f"\nBuscando errores para dataset: {dataset}")
                
                # Buscar feature classes relacionados
                for fc_type in ['_poly', '_line', '_point']:
                    fc_name = f"{dataset}_errors{fc_type}"
                    if arcpy.Exists(fc_name):
                        print(f"Encontrado: {fc_name}")
                        
                        fields = arcpy.ListFields(fc_name)
                        create_sqlite_table(cursor, dataset, fields)
                        
                        records = process_feature_class(cursor, fc_name, dataset)
                        total_records += records
                        
                        conn.commit()
            
            print("\n" + "="*50)
            print(f"PROCESO COMPLETADO")
            print(f"Total de registros procesados: {total_records}")
            print("="*50)
          
    except Exception as e:
        logger.error(f"Error crítico en el proceso: {str(e)}")
        logger.error("Detalles completos:", exc_info=True)
        print(f"\nERROR CRÍTICO: {str(e)}")
        
if __name__ == "__main__":
    main()