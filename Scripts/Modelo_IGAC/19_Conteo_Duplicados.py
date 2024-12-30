import os
import arcpy
import pandas as pd
from pathlib import Path
import sys
sys.stdout.reconfigure(encoding='utf-8')
def find_gdb(root_path):
    """Encuentra la primera GDB en el directorio especificado."""
    for item in os.listdir(root_path):
        if item.endswith('.gdb'):
            return os.path.join(root_path, item)
    return None

def get_project_root():
    """
    Obtiene la raíz del proyecto verificando la estructura de directorios esperada.
    """
    current_dir = Path.cwd().resolve()
    
    while current_dir.parent != current_dir:
        # Verifica la estructura característica del proyecto
        required_paths = [
            current_dir / "Files" / "Temporary_Files" / "MODELO_IGAC",
            current_dir / "Files" / "Temporary_Files" / "array_config.txt"
        ]
        
        if all(path.exists() for path in required_paths):
            print(f"Raíz del proyecto encontrada en: {current_dir}")
            return current_dir
        
        current_dir = current_dir.parent
    
    return None

def load_active_datasets():
    """Carga los datasets activos desde el archivo de configuración."""
    try:
        project_root = get_project_root()
        if not project_root:
            raise Exception("No se encontró la carpeta raíz del proyecto")
        
        config_path = project_root / "Files" / "Temporary_Files" / "array_config.txt"
        
        active_datasets = []
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    dataset_name = line.strip('",[]').strip()
                    if dataset_name:
                        active_datasets.append(dataset_name)
        
        return active_datasets
    except Exception as e:
        print(f"Error al cargar configuración: {str(e)}")
        return ["URBANO_CTM12", "RURAL_CTM12"]

def process_duplicates():
    try:
        # Obtener la ruta raíz del proyecto
        project_root = get_project_root()
        if not project_root:
            raise Exception("No se encontró la carpeta raíz del proyecto")
        
        # Definir rutas
        gdb_folder = project_root / "Files" / "Temporary_Files" / "MODELO_IGAC" / "consistencia_formato_temp"
        output_folder = project_root / "Files" / "Temporary_Files" / "MODELO_IGAC" / "Omision_comision_temp"
        output_excel = output_folder / "9_Duplicados.xlsx"
        
        # Crear directorio para shapefiles
        shp_output_folder = output_folder / "9_Shp_Duplicados"
        shp_output_folder.mkdir(parents=True, exist_ok=True)
        
        # Encontrar la GDB
        gdb_path = find_gdb(str(gdb_folder))
        if not gdb_path:
            raise Exception("No se encontró ninguna GDB en el directorio especificado")
        
        # Cargar datasets activos
        active_datasets = load_active_datasets()
        
        # Lista para almacenar los resultados
        results = []
        
        # Procesar cada dataset
        arcpy.env.workspace = gdb_path
        for dataset_name in active_datasets:
            dataset_path = os.path.join(gdb_path, dataset_name)
            
            # Verificar si el dataset existe
            if not arcpy.Exists(dataset_path):
                print(f"Dataset {dataset_name} no encontrado, continuando con el siguiente...")
                continue
            
            # Obtener feature classes en el dataset
            feature_classes = arcpy.ListFeatureClasses(feature_dataset=dataset_name)
            
            # Procesar cada feature class
            for fc_name in feature_classes:
                fc_path = os.path.join(dataset_path, fc_name)
                
                # Verificar si existe la columna Error_Descripcion
                fields = [f.name for f in arcpy.ListFields(fc_path)]
                if 'Error_Descripcion' not in fields:
                    continue
                
                # Crear la consulta
                where_clause = "Error_Descripcion = 'Error: Registro Duplicado'"
                
                # Campos a recuperar para el Excel
                fields_to_retrieve = ['CODIGO', 'Error_Descripcion'] if 'CODIGO' in fields else ['Error_Descripcion']
                
                # Contador para duplicados en este feature class
                duplicate_count = 0
                
                # Buscar registros duplicados para el Excel
                with arcpy.da.SearchCursor(fc_path, fields_to_retrieve, where_clause) as cursor:
                    for row in cursor:
                        codigo = row[0] if 'CODIGO' in fields else ''
                        results.append({
                            'CODIGO': codigo,
                            'Featureclass': fc_name,
                            'Dataset': dataset_name
                        })
                        duplicate_count += 1
                
                # Si hay duplicados, exportar a shapefile
                if duplicate_count > 0:
                    # Crear nombre del shapefile de salida
                    output_shp_name = f"9_Duplicados_{fc_name}.shp"
                    output_shp_path = str(shp_output_folder / output_shp_name)
                    
                    # Crear una capa temporal con los registros duplicados
                    temp_layer = f"temp_{fc_name}"
                    arcpy.MakeFeatureLayer_management(fc_path, temp_layer, where_clause)
                    
                    # Exportar a shapefile
                    arcpy.CopyFeatures_management(temp_layer, output_shp_path)
                    print(f"Shapefile creado: {output_shp_path}")
                    
                    # Limpiar la capa temporal
                    arcpy.Delete_management(temp_layer)
        
        # Crear el DataFrame y guardar el Excel
        if results:
            df = pd.DataFrame(results)
            with pd.ExcelWriter(str(output_excel), engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Duplicados', index=False)
            print(f"Excel creado exitosamente en: {output_excel}")
        else:
            print("No se encontraron registros duplicados para procesar")
            
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")

if __name__ == "__main__":
    process_duplicates()