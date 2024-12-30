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
    Encuentra la raíz del proyecto basándose en su estructura interna,
    independientemente del nombre del directorio.
    """
    current = Path(current_dir).resolve()
    
    while current.parent != current:
        # Verifica la estructura característica del proyecto
        required_paths = [
            current / "Files" / "Temporary_Files" / "MODELO_IGAC",
            current / "Files" / "Temporary_Files" / "array_config.txt",
            current / "Files" / "Temporary_Files" / "MODELO_IGAC" / "03_INCONSISTENCIAS",
            current / "Files" / "Temporary_Files" / "MODELO_IGAC" / "db"
        ]
        
        if all(path.exists() for path in required_paths):
            arcpy.AddMessage(f"Raíz del proyecto encontrada en: {current}")
            return current
        
        current = current.parent
    
    raise ValueError(
        "No se encontró la raíz del proyecto.\n"
        "Verifique que está ejecutando el script desde dentro del proyecto "
        "y que existe la siguiente estructura:\n"
        "- Files/Temporary_Files/MODELO_IGAC\n"
        "- Files/Temporary_Files/array_config.txt\n"
        "- Files/Temporary_Files/MODELO_IGAC/03_INCONSISTENCIAS\n"
        "- Files/Temporary_Files/MODELO_IGAC/db"
    )

def create_db_structure(root_path):
    """
    Crea la estructura de directorios y la base de datos SQLite.
    Incluye verificaciones de permisos y manejo de errores mejorado.
    """
    try:
        # Crear directorio si no existe
        db_path = root_path / "Files" / "Temporary_Files" / "MODELO_IGAC" / "db"
        db_path.mkdir(parents=True, exist_ok=True)
        
        # Verificar permisos de escritura
        if not os.access(db_path, os.W_OK):
            raise PermissionError(f"No tiene permisos de escritura en: {db_path}")
        
        # Ruta completa de la base de datos
        db_file = db_path / "excepciones_consistencia_formato.db"
        
        # Si la base de datos existe, intentar eliminarla
        if db_file.exists():
            try:
                # Verificar si hay conexiones activas
                test_conn = sqlite3.connect(str(db_file))
                test_conn.close()
                
                db_file.unlink()
                arcpy.AddMessage("Base de datos existente eliminada exitosamente")
            except PermissionError:
                raise PermissionError("No se puede eliminar la base de datos. Puede estar en uso.")
            except Exception as e:
                raise Exception(f"Error al eliminar base de datos existente: {str(e)}")
        
        arcpy.AddMessage(f"Creando nueva base de datos en: {db_file}")
        return db_file
        
    except Exception as e:
        arcpy.AddError(f"Error en create_db_structure: {str(e)}")
        raise

def get_active_datasets(root_path):
    """
    Lee y retorna los datasets activos del archivo de configuración.
    Incluye mejor manejo de errores y validaciones.
    """
    try:
        config_path = root_path / "Files" / "Temporary_Files" / "array_config.txt"
        
        if not config_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo de configuración en: {config_path}")
            
        if not os.access(config_path, os.R_OK):
            raise PermissionError(f"No hay permisos de lectura para: {config_path}")
        
        datasets = []
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    dataset_name = line.strip('",[]').strip()
                    if dataset_name:
                        datasets.append(dataset_name)
        
        if not datasets:
            arcpy.AddWarning("No se encontraron datasets activos. Usando configuración por defecto.")
            return ["URBANO_CTM12", "RURAL_CTM12"]
            
        arcpy.AddMessage("\nDatasets activos encontrados:")
        for ds in datasets:
            arcpy.AddMessage(f"  - {ds}")
            
        return datasets
        
    except Exception as e:
        arcpy.AddWarning(f"Error al leer configuración: {str(e)}")
        arcpy.AddMessage("Usando configuración por defecto")
        return ["URBANO_CTM12", "RURAL_CTM12"]

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



def copy_and_count_shapefile(shapefile_path):
    """
    Crea una copia temporal del shapefile con nombre simplificado, cuenta registros y elimina la copia.
    """
    try:
        original_name = shapefile_path.name
        temp_name = original_name.split('.', 1)[0] + '.shp'  # Solo toma el número inicial
        temp_files = []
        count = 0
        
        # Crear directorio temporal si no existe
        temp_dir = shapefile_path.parent / 'temp_count'
        temp_dir.mkdir(exist_ok=True)
        
        # Copiar todos los archivos relacionados
        base_name = shapefile_path.stem
        temp_base = temp_name.replace('.shp', '')
        
        # Copiar cada archivo relacionado (.shp, .dbf, .shx, etc.)
        for related_file in shapefile_path.parent.glob(f"{base_name}.*"):
            temp_file = temp_dir / (temp_base + related_file.suffix)
            shutil.copy2(related_file, temp_file)
            temp_files.append(temp_file)
        
        try:
            # Contar registros usando el archivo temporal
            temp_shapefile = temp_dir / temp_name
            if temp_shapefile.exists():
                try:
                    with arcpy.da.SearchCursor(str(temp_shapefile), ['OID@']) as cursor:
                        for _ in cursor:
                            count += 1
                    arcpy.AddMessage(f"Conteo exitoso para {original_name}: {count}")
                except Exception as cursor_error:
                    arcpy.AddMessage(f"Error en cursor para {original_name}: {str(cursor_error)}")
                    try:
                        result = arcpy.GetCount_management(str(temp_shapefile))
                        count = int(result.getOutput(0))
                        arcpy.AddMessage(f"Conteo alternativo exitoso para {original_name}: {count}")
                    except:
                        arcpy.AddMessage(f"No se pudo contar registros en {original_name}")
        
        finally:
            # Limpiar archivos temporales
            for temp_file in temp_files:
                if temp_file.exists():
                    temp_file.unlink()
            
            # Eliminar directorio temporal si está vacío
            if temp_dir.exists():
                try:
                    temp_dir.rmdir()
                except:
                    pass
        
        return count
    
    except Exception as e:
        arcpy.AddWarning(f"Error procesando {shapefile_path.name}: {str(e)}")
        return 0

def count_shapefile(shapefile_path):
    """
    Cuenta registros de un shapefile excluyendo aquellos que tienen contenido
    en la columna Error_Descripcion.
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

        # Contar registros excluyendo aquellos con Excepcion_
        valid_count = 0
        total_count = 0
        
        # Crear capa en memoria para mejor rendimiento
        mem_layer = f"in_memory\\{simple_name}"
        arcpy.CopyFeatures_management(str(temp_shp), mem_layer)
        
        # Verificar si existe la columna Excepcion_
        field_names = [field.name for field in arcpy.ListFields(mem_layer)]
        has_error_desc = 'Excepcion_' in field_names
        
        if has_error_desc:
            with arcpy.da.SearchCursor(mem_layer, ['OID@', 'Excepcion_']) as cursor:
                for row in cursor:
                    total_count += 1
                    # Verificar si Excepcion_ está vacío o es None
                    error_desc = row[1]
                    if error_desc is None or (isinstance(error_desc, str) and error_desc.strip() == ''):
                        valid_count += 1
        else:
            # Si no existe la columna, contar todos los registros
            with arcpy.da.SearchCursor(mem_layer, ['OID@']) as cursor:
                for _ in cursor:
                    valid_count += 1
                    total_count += 1

        arcpy.Delete_management(mem_layer)
        
        arcpy.AddMessage(f"  → Total registros: {total_count}")
        arcpy.AddMessage(f"  → Registros sin descripción de error: {valid_count}")
        arcpy.AddMessage(f"  → Registros excluidos por tener descripción: {total_count - valid_count}")
        
        return valid_count

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
    
    # Buscar shapefiles directamente en el directorio base
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
                arcpy.AddMessage(f"  → Guardando {count} registros para {column_name}")
    
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
        # Crear lista de columnas únicas
        columns = list(set(mappings.values()))
        
        # Crear la tabla (sin carpeta_origen)
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
        # Configurar el entorno de arcpy
        arcpy.env.overwriteOutput = True
        
        # Encontrar la raíz del proyecto
        project_root = find_project_root(os.getcwd())
        arcpy.AddMessage(f"Raíz del proyecto encontrada: {project_root}")
        
        # Obtener datasets activos
        datasets_to_process = get_active_datasets(project_root)
        
        # Crear estructura de BD
        db_file = create_db_structure(project_root)
        
        # Conectar a la BD
        with sqlite3.connect(str(db_file)) as conn:
            # Obtener mapeo de columnas
            shape_column_mapping = get_shape_column_mapping()
            
            # Crear tablas
            create_tables(conn, shape_column_mapping)
            arcpy.AddMessage("Tablas creadas en la base de datos")
            
            # Procesar cada dataset
            for dataset in datasets_to_process:
                if dataset in shape_column_mapping:
                    arcpy.AddMessage(f"\nProcesando dataset: {dataset}")
                    base_path = (project_root / "Files" / "Temporary_Files" / "MODELO_IGAC" / 
                               "03_INCONSISTENCIAS" / "CONSISTENCIA_FORMATO" / dataset)
                    process_dataset_counts(base_path, dataset, shape_column_mapping, conn)
                else:
                    arcpy.AddWarning(f"No hay mapeo definido para el dataset: {dataset}")
        
        arcpy.AddMessage("\nProceso completado exitosamente")
        
    except Exception as e:
        arcpy.AddError(f"Error general: {str(e)}")

if __name__ == "__main__":
    main()