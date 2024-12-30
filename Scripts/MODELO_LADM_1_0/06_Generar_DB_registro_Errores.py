import arcpy
import os
import sqlite3
from pathlib import Path
import logging
from datetime import datetime

# Campos a excluir
EXCLUDED_FIELDS = ['SHAPE', 'SHAPE_Length', 'SHAPE_Area', 'SHAPE.STLength()', 'SHAPE.STArea()']

# Directorios rurales específicos
RURAL_DIRECTORIES = [
    'DERECHO_DOMINIO_RU',
    'INCONSISTENCIAS_SUBMODELO_CARTOGRAFIA_RURAL',
    'INCONSITENCIAS_TOPOLOGICAS_RURALES',
    'UNIDADES_RURALES_SUPERPUESTAS',
    'INCONSITENCIAS_ZONAS_RURALES'
]

def load_dataset_configuration(project_root):
    """Carga la configuración de datasets desde array_config.txt"""
    config_path = project_root / "Files" / "Temporary_Files" / "array_config.txt"
    
    DATASETS_TO_PROCESS = []
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                dataset_name = line.strip('",[]').strip()
                if dataset_name:
                    DATASETS_TO_PROCESS.append(dataset_name)
    
    print("\nConfiguración de datasets cargada:")
    print("--------------------------------")
    print("Datasets que serán procesados:")
    for ds in DATASETS_TO_PROCESS:
        print(f"  - {ds}")
    print("--------------------------------\n")
    
    return DATASETS_TO_PROCESS

def find_project_root():
    """Encuentra la raíz del proyecto verificando la estructura de directorios esperada."""
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_proyecto = os.path.abspath(os.path.join(ruta_actual, '..', '..'))
    
    rutas_requeridas = [
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_LADM_1_2"),
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "array_config.txt"),
        os.path.join(ruta_proyecto, "Scripts")
    ]
    
    print("\nVerificando rutas del proyecto:")
    for ruta in rutas_requeridas:
        existe = os.path.exists(ruta)
        print(f"Ruta: {ruta}")
        print(f"¿Existe?: {existe}")
    
    if all(os.path.exists(ruta) for ruta in rutas_requeridas):
        print(f"\nRaíz del proyecto encontrada en: {ruta_proyecto}")
        
        # Crear directorios necesarios si no existen
        db_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_LADM_1_2", "db")
        logs_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_LADM_1_2", "logs")
        
        for directory in [db_dir, logs_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                print(f"Directorio creado: {directory}")
        
        return Path(ruta_proyecto)
    
    raise Exception(
        "No se encontró la raíz del proyecto.\n"
        "Verifique que está ejecutando el script desde dentro del proyecto "
        "y que existe la siguiente estructura:\n"
        "- Files/Temporary_Files/MODELO_LADM_1_2\n"
        "- Files/Temporary_Files/array_config.txt"
    )

def setup_logging():
    """Configura el sistema de logging"""
    try:
        project_root = find_project_root()
        log_directory = project_root / "Files" / "Temporary_Files" / "MODELO_LADM_1_2" / "logs"
        log_directory.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_directory / f"shapefile_to_sqlite_{timestamp}.log"
        
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
            field_definitions.append(f"{field.name} {field_type}")
    
    safe_table_name = table_name.replace('-', '_').replace(' ', '_')
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{safe_table_name}" (
        {', '.join(field_definitions)}
    )
    """
    cursor.execute(create_table_sql)

def process_shapefile(cursor, shapefile_path, table_name):
    """Procesa un shapefile y guarda sus registros en SQLite"""
    try:
        fields = arcpy.ListFields(shapefile_path)
        field_names = [field.name for field in fields 
                      if field.name not in EXCLUDED_FIELDS and 
                      not field.name.upper().startswith('SHAPE')]
        
        safe_table_name = table_name.replace('-', '_').replace(' ', '_')
        
        placeholders = ','.join(['?' for _ in field_names])
        insert_sql = f"""INSERT INTO "{safe_table_name}" ({','.join(field_names)}) 
                        VALUES ({placeholders})"""
        
        print(f"Procesando registros de {os.path.basename(shapefile_path)}...")
        count = 0
        
        with arcpy.da.SearchCursor(shapefile_path, field_names) as cursor_arcpy:
            for row in cursor_arcpy:
                processed_row = []
                for value in row:
                    if isinstance(value, (bytes, bytearray)):
                        processed_row.append(str(value))
                    else:
                        processed_row.append(value)
                
                cursor.execute(insert_sql, tuple(processed_row))
                count += 1
        
        print(f"Total de registros procesados: {count}")
        return count
        
    except Exception as e:
        print(f"Error procesando shapefile {shapefile_path}: {str(e)}")
        return 0

def find_shapefiles(directory, is_rural=False):
    """Encuentra todos los shapefiles en el directorio y sus subdirectorios"""
    shapefiles = []
    for root, dirs, files in os.walk(directory):
        # Si es rural, solo procesar los directorios específicos
        if is_rural:
            current_dir = os.path.basename(root)
            parent_dir = os.path.basename(os.path.dirname(root))
            if current_dir not in RURAL_DIRECTORIES and parent_dir not in RURAL_DIRECTORIES:
                continue
        # Si es urbano, excluir los directorios rurales
        else:
            current_dir = os.path.basename(root)
            parent_dir = os.path.basename(os.path.dirname(root))
            if current_dir in RURAL_DIRECTORIES or parent_dir in RURAL_DIRECTORIES:
                continue
                
        for file in files:
            if file.endswith('.shp'):
                shapefiles.append(os.path.join(root, file))
    return shapefiles

def process_dataset(dataset_name, validaciones_dir, db_dir, logger):
    """Procesa un dataset específico (URBANO_CTM12 o RURAL_CTM12)"""
    is_rural = dataset_name == "RURAL_CTM12"
    db_name = "registro_errores_rural.db" if is_rural else "registro_errores_urbano.db"
    db_path = db_dir / db_name
    
    print(f"\nProcesando dataset: {dataset_name}")
    print(f"Base de datos: {db_path}")
    
    # Encontrar shapefiles según el tipo de dataset
    shapefiles = find_shapefiles(validaciones_dir, is_rural)
    print(f"Shapefiles encontrados: {len(shapefiles)}")
    
    if not shapefiles:
        print(f"No se encontraron shapefiles para procesar en {dataset_name}")
        return
    
    # Procesar los shapefiles
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()
        total_records = 0
        
        for shapefile_path in shapefiles:
            print(f"\nProcesando shapefile: {shapefile_path}")
            
            table_name = Path(shapefile_path).stem
            fields = arcpy.ListFields(shapefile_path)
            create_sqlite_table(cursor, table_name, fields)
            
            records = process_shapefile(cursor, shapefile_path, table_name)
            total_records += records
            
            conn.commit()
        
        print(f"\nDataset {dataset_name} completado:")
        print(f"Total de shapefiles procesados: {len(shapefiles)}")
        print(f"Total de registros procesados: {total_records}")

def main():
    try:
        # Configuración inicial
        logger = setup_logging()
        logger.info("Iniciando proceso de exportación de shapefiles a SQLite")
        print("\nINICIANDO PROCESO DE EXPORTACIÓN DE SHAPEFILES A SQLITE")
        print("="*50)
        
        # Encontrar la raíz del proyecto
        project_root = find_project_root()
        
        # Cargar configuración de datasets
        datasets_to_process = load_dataset_configuration(project_root)
        
        # Definir directorios
        validaciones_dir = project_root / "Files" / "Temporary_Files" / "MODELO_LADM_1_2" / "Validaciones_Calidad"
        db_dir = project_root / "Files" / "Temporary_Files" / "MODELO_LADM_1_2" / "db"
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # Procesar cada dataset configurado
        for dataset in datasets_to_process:
            process_dataset(dataset, validaciones_dir, db_dir, logger)
        
        print("\n" + "="*50)
        print("PROCESO COMPLETADO")
        print("="*50)
          
    except Exception as e:
        logger.error(f"Error crítico en el proceso: {str(e)}")
        logger.error("Detalles completos:", exc_info=True)
        print(f"\nERROR CRÍTICO: {str(e)}")
        
if __name__ == "__main__":
    main()