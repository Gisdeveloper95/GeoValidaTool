import os
import arcpy
import sqlite3
import shutil
from pathlib import Path
import json
import sys
sys.stdout.reconfigure(encoding='utf-8')
def find_project_root(current_dir):
    """
    Encuentra la raíz del proyecto basándose en la estructura interna de carpetas,
    independientemente del nombre del directorio raíz.
    """
    current = Path(current_dir).resolve()
    
    while current.parent != current:
        # Verifica la estructura característica del proyecto
        expected_structure = [
            current / "Files" / "Temporary_Files" / "MODELO_IGAC",
            current / "Files" / "Temporary_Files" / "array_config.txt"
        ]
        
        # Si encuentra la estructura esperada, este es el directorio raíz
        if all(path.exists() for path in expected_structure):
            arcpy.AddMessage(f"Raíz del proyecto encontrada en: {current}")
            return current
        
        # Si no es la raíz, sube un nivel
        current = current.parent
    
    raise ValueError(
        "No se encontró la raíz del proyecto. "
        "Verifique que está ejecutando el script desde dentro del proyecto y que existe "
        "la estructura: Files/Temporary_Files/MODELO_IGAC y array_config.txt"
    )

def create_db_structure(root_path):
    """
    Crea la estructura de directorios y la base de datos SQLite.
    Incluye manejo de errores mejorado y verificaciones adicionales.
    """
    try:
        # Crear el directorio para la base de datos
        db_path = root_path / "Files" / "Temporary_Files" / "MODELO_IGAC" / "db"
        db_path.mkdir(parents=True, exist_ok=True)
        
        db_file = db_path / "errores_consistencia_formato.db"
        
        # Verificar si hay conexiones activas a la base de datos
        if db_file.exists():
            try:
                # Intentar eliminar la base de datos existente
                arcpy.AddMessage("Intentando eliminar base de datos existente...")
                db_file.unlink()
                arcpy.AddMessage("Base de datos existente eliminada exitosamente")
            except PermissionError:
                arcpy.AddError("No se puede eliminar la base de datos. Puede estar en uso.")
                arcpy.AddMessage("Por favor, cierre todas las conexiones a la base de datos e intente nuevamente.")
                raise
            except Exception as e:
                arcpy.AddError(f"Error inesperado al eliminar base de datos: {str(e)}")
                raise
        
        # Verificar permisos de escritura en el directorio
        if not os.access(db_path, os.W_OK):
            raise PermissionError(f"No tiene permisos de escritura en el directorio: {db_path}")
        
        arcpy.AddMessage(f"Creando nueva base de datos en: {db_file}")
        
        # Verificar que se puede crear la base de datos
        try:
            conn = sqlite3.connect(str(db_file))
            conn.close()
        except sqlite3.Error as e:
            arcpy.AddError(f"Error al crear la base de datos SQLite: {str(e)}")
            raise
        
        return db_file
    
    except Exception as e:
        arcpy.AddError(f"Error en create_db_structure: {str(e)}")
        raise
def get_shape_column_mapping():
    """Define el mapeo de nombres de shapefiles a columnas de la base de datos."""
    return {
        "URBANO_CTM12": {
            "1.U_BARRIO_CTM12_atributos_mal_calculados.shp": "U_BARRIO_CTM12",
            "2.U_SECTOR_CTM12_atributos_mal_calculados.shp": "U_SECTOR_CTM12",
            "3.U_MANZANA_CTM12_atributos_mal_calculados.shp": "U_MANZANA_CTM12",
            "4.U_TERRENO_CTM12_atributos_mal_calculados.shp": "U_TERRENO_CTM12",
            "5.U_CONSTRUCCION_CTM12_atributos_mal_calculados.shp": "U_CONSTRUCCION_CTM12",
            "6.U_UNIDAD_CTM12_atributos_mal_calculados.shp": "U_UNIDAD_CTM12",
            "7.U_NOMEN_DOMICILIARIA_CTM12_atributos_mal_calculados.shp": "U_NOMEN_DOMICILIARIA_CTM12",
            "8.U_NOMENCLATURA_VIAL_CTM12_atributos_mal_calculados.shp": "U_NOMENCLATURA_VIAL_CTM12",
            "9.U_SECTOR_CTM12_atributos_NO_coinciden_con_U_MANZANA_CTM12.shp": "U_MANZANA_CTM12_U_SECTOR_CTM12",
            "10.U_MANZANA_CTM12_atributos_NO_coinciden_con_U_TERRENO_CTM12.shp": "U_TERRENO_CTM12_U_MANZANA_CTM12",
            "11.U_TERRENO_CTM12_atributos_NO_coinciden_con_U_CONSTRUCCION_CTM12.shp": "U_CONSTRUCCION_CTM12_U_TERRENO_CTM12",
            "12.U_CONSTRUCCION_CTM12_atributos_NO_coinciden_con_U_UNIDAD_CTM12.shp": "U_UNIDAD_CTM12_U_CONSTRUCCION_CTM12",
            "13.U_TERRENO_CTM12_atributos_NO_coinciden_con_U_UNIDAD_CTM12.shp": "U_UNIDAD_CTM12_U_TERRENO_CTM12",
            "14.U_TERRENO_CTM12_atributos_NO_coinciden_con_U_NOMEN_DOMICILIARIA_CTM12.shp": "U_NOMEN_DOMICILIARIA_CTM12_U_TERRENO_CTM12"


        },
        "RURAL_CTM12": {
            "1.R_SECTOR_CTM12_atributos_mal_calculados.shp": "R_SECTOR_CTM12",
            "2.R_VEREDA_CTM12_atributos_mal_calculados.shp": "R_VEREDA_CTM12",
            "3.R_TERRENO_CTM12_atributos_mal_calculados.shp": "R_TERRENO_CTM12",
            "4.R_CONSTRUCCION_CTM12_atributos_mal_calculados.shp": "R_CONSTRUCCION_CTM12",
            "5.R_UNIDAD_CTM12_atributos_mal_calculados.shp": "R_UNIDAD_CTM12",
            "6.R_NOMEN_DOMICILIARIA_CTM12_atributos_mal_calculados.shp": "R_NOMEN_DOMICILIARIA_CTM12",
            "7.R_NOMENCLATURA_VIAL_CTM12_atributos_mal_calculados.shp": "R_NOMENCLATURA_VIAL_CTM12",
            "8.R_SECTOR_CTM12_atributos_NO_coinciden_con_R_VEREDA_CTM12.shp": "R_VEREDA_CTM12_R_SECTOR_CTM12",
            "9.R_VEREDA_CTM12_atributos_NO_coinciden_con_R_TERRENO_CTM12.shp": "R_TERRENO_CTM12_R_VEREDA_CTM12",
            "10.R_TERRENO_CTM12_atributos_NO_coinciden_con_R_CONSTRUCCION_CTM12.shp": "R_CONSTRUCCION_CTM12_R_TERRENO_CTM12",
            "11.R_CONSTRUCCION_CTM12_atributos_NO_coinciden_con_R_UNIDAD_CTM12.shp": "R_UNIDAD_CTM12_R_CONSTRUCCION_CTM12",
            "12.R_TERRENO_CTM12_atributos_NO_coinciden_con_R_UNIDAD_CTM12.shp": "R_UNIDAD_CTM12_R_TERRENO_CTM12",
            "13.R_TERRENO_CTM12_atributos_NO_coinciden_con_R_NOMEN_DOMICILIARIA_CTM12.shp": "R_NOMEN_DOMICILIARIA_CTM12_R_TERRENO_CTM12",

        },
        "URBANO":{
            "1.U_BARRIO_atributos_mal_calculados.shp": "U_BARRIO",
            "2.U_BARRIO_atributos_mal_calculados.shp": "U_SECTOR",
            "3.U_SECTOR_atributos_mal_calculados.shp": "U_MANZANA",
            "4.U_TERRENO_atributos_mal_calculados.shp": "U_TERRENO",
            "5.U_CONSTRUCCION_atributos_mal_calculados.shp": "U_CONSTRUCCION",
            "6.U_UNIDAD_atributos_mal_calculados.shp": "U_UNIDAD",
            "7.U_NOMENCLATURA_DOMICILIARIA_atributos_mal_calculados.shp": "U_NOMENCLATURA_DOMICILIARIA",
            "8.U_NOMENCLATURACLATURA_VIAL_atributos_mal_calculados.shp": "U_NOMENCLATURACLATURA_VIAL",
            "9.U_SECTOR_atributos_NO_coinciden_con_U_MANZANA.shp": "U_MANZANA_U_SECTOR",
            "10.U_MANZANA_atributos_NO_coinciden_con_U_TERRENO.shp": "U_TERRENO_U_MANZANA",
            "11.U_TERRENO_atributos_NO_coinciden_con_U_CONSTRUCCION.shp": "U_CONSTRUCCION_U_TERRENO",
            "12.U_CONSTRUCCION_atributos_NO_coinciden_con_U_UNIDAD.shp": "U_UNIDAD_U_CONSTRUCCION",
            "13.U_TERRENO_atributos_NO_coinciden_con_U_UNIDAD.shp": "U_UNIDAD_U_TERRENO",
            "14.U_TERRENO_atributos_NO_coinciden_con_U_NOMENCLATURA_DOMICILIARIA.shp": "U_NOMENCLATURA_DOMICILIARIA_U_TERRENO"

        },
        "RURAL":{
            "1.R_SECTOR_atributos_mal_calculados.shp": "R_SECTOR",
            "2.R_VEREDA_atributos_mal_calculados.shp": "R_VEREDA",
            "3.R_TERRENO_atributos_mal_calculados.shp": "R_TERRENO",
            "4.R_CONSTRUCCION_atributos_mal_calculados.shp": "R_CONSTRUCCION",
            "5.R_UNIDAD_atributos_mal_calculados.shp": "R_UNIDAD",
            "6.R_NOMENCLATURA_DOMICILIARIA_atributos_mal_calculados.shp": "R_NOMENCLATURA_DOMICILIARIA",
            "7.R_NOMENCLATURACLATURA_VIAL_atributos_mal_calculados.shp": "R_NOMENCLATURACLATURA_VIAL",
            "8.R_SECTOR_atributos_NO_coinciden_con_R_VEREDA.shp": "R_VEREDA_R_SECTOR",
            "9.R_VEREDA_atributos_NO_coinciden_con_R_TERRENO.shp": "R_TERRENO_R_VEREDA",
            "10.R_TERRENO_atributos_NO_coinciden_con_R_CONSTRUCCION.shp": "R_CONSTRUCCION_R_TERRENO",
            "11.R_CONSTRUCCION_atributos_NO_coinciden_con_R_UNIDAD.shp": "R_UNIDAD_R_CONSTRUCCION",
            "12.R_TERRENO_atributos_NO_coinciden_con_R_UNIDAD.shp": "R_UNIDAD_R_TERRENO",
            "13.R_TERRENO_atributos_NO_coinciden_con_R_NOMENCLATURA_DOMICILIARIA.shp": "R_NOMENCLATURA_DOMICILIARIA_R_TERRENO"

        }
    }
    
def count_shapefile(shapefile_path):
    """
    Cuenta registros de un shapefile usando una copia temporal en el mismo directorio.
    """
    try:
        # Crear directorio temporal dentro del directorio del shapefile
        parent_dir = shapefile_path.parent
        temp_dir = parent_dir / 'temp_count'
        temp_dir.mkdir(exist_ok=True)

        # Crear nombre simple reemplazando puntos por guiones bajos
        simple_name = f"temp_{shapefile_path.stem.replace('.', '_')}"
        temp_shp = temp_dir / f"{simple_name}.shp"
        
        # Copiar archivos relacionados
        base_path = str(shapefile_path).rsplit('.', 1)[0]
        temp_base = str(temp_shp).rsplit('.', 1)[0]
        
        for ext in ['.shp', '.dbf', '.shx', '.prj']:
            src = base_path + ext
            if os.path.exists(src):
                shutil.copy2(src, temp_base + ext)

        # Contar registros usando capa en memoria
        mem_layer = f"in_memory\\{simple_name}"
        arcpy.CopyFeatures_management(str(temp_shp), mem_layer)
        count = int(arcpy.GetCount_management(mem_layer).getOutput(0))
        arcpy.Delete_management(mem_layer)
        
        return count

    except Exception as e:
        arcpy.AddWarning(f"Error al contar {shapefile_path.name}: {str(e)}")
        return 0
        
    finally:
        # Limpiar archivos temporales
        if 'temp_dir' in locals() and temp_dir.exists():
            try:
                for file in temp_dir.glob('*'):
                    file.unlink()
                temp_dir.rmdir()
            except Exception as e:
                arcpy.AddWarning(f"No se pudieron eliminar algunos archivos temporales: {str(e)}")

def process_dataset_counts(base_path, dataset_name, shape_column_mapping, conn):
    """Procesa los conteos de registros para un dataset y los guarda en la BD."""
    if dataset_name not in shape_column_mapping:
        arcpy.AddWarning(f"No hay mapeo definido para el dataset {dataset_name}")
        return

    mapping = shape_column_mapping[dataset_name]
    cursor = conn.cursor()
    
    total_counts = {col: 0 for col in set(mapping.values())}
    has_valid_files = False

    arcpy.AddMessage(f"Verificando directorio: {base_path}")
    if not base_path.exists():
        arcpy.AddWarning(f"El directorio {base_path} no existe")
        return
    
    # Buscar shapefiles en el directorio
    shp_files = list(base_path.glob("*.shp"))
    arcpy.AddMessage(f"\nEncontrados {len(shp_files)} shapefiles en {base_path}")
    
    for shapefile in shp_files:
        shapefile_name = shapefile.name
        arcpy.AddMessage(f"\nProcesando: {shapefile_name}")
        
        if shapefile_name in mapping:
            count = count_shapefile(shapefile)
            if count > 0:
                has_valid_files = True
                column_name = mapping[shapefile_name]
                total_counts[column_name] = count
                arcpy.AddMessage(f"  → Conteo exitoso: {count} registros")
    
    # Guardar resultados en la BD
    if has_valid_files:
        columns = list(total_counts.keys())
        values = list(total_counts.values())
        
        if any(v > 0 for v in values):
            insert_sql = f"""
            INSERT INTO {dataset_name} 
            ({', '.join(columns)})
            VALUES ({', '.join(['?' for _ in values])})
            """
            
            cursor.execute(insert_sql, values)
            conn.commit()
            
            arcpy.AddMessage("\nResumen de conteos guardados:")
            for col, count in total_counts.items():
                if count > 0:
                    arcpy.AddMessage(f"  {col}: {count}")
    else:
        arcpy.AddMessage(f"No se encontraron registros para guardar en el dataset {dataset_name}")

def create_tables(conn, shape_column_mapping):
    """Crea las tablas en la base de datos para cada dataset."""
    cursor = conn.cursor()
    
    for dataset, mappings in shape_column_mapping.items():
        columns = list(set(mappings.values()))
        columns_sql = ", ".join([f"{col} INTEGER DEFAULT 0" for col in columns])
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {dataset} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            {columns_sql}
        )
        """
        cursor.execute(create_table_sql)
    
    conn.commit()

def main():
    try:
        arcpy.env.overwriteOutput = True
        
        project_root = find_project_root(os.getcwd())
        arcpy.AddMessage(f"Raíz del proyecto encontrada: {project_root}")

        db_path = create_db_structure(project_root)
        arcpy.AddMessage(f"Base de datos creada en: {db_path}")
        
        conn = sqlite3.connect(str(db_path))
        
        shape_column_mapping = get_shape_column_mapping()
        
        create_tables(conn, shape_column_mapping)
        arcpy.AddMessage("Tablas creadas en la base de datos")

        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
            config_path = os.path.join(proyecto_dir,  "Files", "Temporary_Files", "array_config.txt")

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

        except Exception as e:
            print(f"Error al cargar configuración: {str(e)}")
            DATASETS_TO_PROCESS = ["URBANO_CTM12", "RURAL_CTM12"]
            print("\nUsando configuración por defecto:")
            print("--------------------------------")
            print("Datasets que serán procesados:")
            for ds in DATASETS_TO_PROCESS:
                print(f"  - {ds}")
            print("--------------------------------\n")

        for dataset in DATASETS_TO_PROCESS:
            arcpy.AddMessage(f"\nProcesando dataset: {dataset}")
            base_path = project_root / "Files" / "Temporary_Files" / "MODELO_IGAC" / "03_INCONSISTENCIAS" / "CONSISTENCIA_FORMATO" / dataset
            process_dataset_counts(base_path, dataset, shape_column_mapping, conn)

        conn.close()
        arcpy.AddMessage("\nProceso completado exitosamente")

    except Exception as e:
        arcpy.AddError(f"Error general: {str(e)}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()