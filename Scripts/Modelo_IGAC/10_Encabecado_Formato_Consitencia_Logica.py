import os
import arcpy
import sqlite3
import openpyxl
from datetime import datetime
import warnings
from pathlib import Path
import sys
sys.stdout.reconfigure(encoding='utf-8')
class MunicipalityDataProcessor:
    def __init__(self):
        """
        Inicializa el procesador utilizando rutas relativas desde la raíz del proyecto.
        """
        self.base_path = self.get_project_root()
        self.temp_files_path = os.path.join(self.base_path, 'Files', 'Temporary_Files', 'MODELO_IGAC')
        self.municipios_db = os.path.join(self.base_path, 'Files', 'Municipios', 'municipios.db')
        self.temp_files_path_config = os.path.join(self.base_path, 'Files', 'Temporary_Files')
        self.array_config = os.path.join(self.temp_files_path_config, 'array_config.txt')
        self.topology_path = os.path.join(self.temp_files_path, '02_TOPOLOGIA')

    def get_project_root(self):
        """
        Detecta la raíz del proyecto verificando la estructura de directorios esperada.
        """
        current = Path(os.path.abspath(os.getcwd())).resolve()
        
        while current.parent != current:
            # Verifica la estructura característica del proyecto
            required_paths = [
                current / "Files" / "Temporary_Files" / "MODELO_IGAC",
                current / "Files" / "Temporary_Files" / "array_config.txt",
                current / "Files" / "Municipios" / "municipios.db",
                current / "Files" / "Temporary_Files" / "MODELO_IGAC" / "02_TOPOLOGIA"
            ]
            
            if all(path.exists() for path in required_paths):
                print(f"Raíz del proyecto encontrada en: {current}")
                return str(current)
            
            current = current.parent
        
        raise Exception(
            "No se encontró la raíz del proyecto. "
            "Verifique que está ejecutando el script desde dentro del proyecto y que existe "
            "la siguiente estructura:\n"
            "- Files/Temporary_Files/MODELO_IGAC\n"
            "- Files/Temporary_Files/array_config.txt\n"
            "- Files/Municipios/municipios.db\n"
            "- Files/Temporary_Files/MODELO_IGAC/02_TOPOLOGIA"
        )

    def get_active_datasets(self):
        """Lee y retorna los datasets activos del archivo de configuración."""
        try:
            # Verificar que el archivo existe y es accesible
            if not os.path.exists(self.array_config):
                raise FileNotFoundError(f"No se encontró el archivo: {self.array_config}")
            
            if not os.access(self.array_config, os.R_OK):
                raise PermissionError(f"No hay permisos de lectura para: {self.array_config}")
            
            active_datasets = []
            with open(self.array_config, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        dataset_name = line.strip('",[]').strip()
                        if dataset_name:
                            active_datasets.append(dataset_name)

            if not active_datasets:
                warnings.warn("No se encontraron datasets activos en el archivo de configuración")
                
            print("\nDatasets activos encontrados:")
            for ds in active_datasets:
                print(f"  - {ds}")
            return active_datasets

        except Exception as e:
            print(f"Error al leer array_config.txt: {str(e)}")
            return []

    def get_feature_class_name(self, dataset_name):
        """Determina el nombre del feature class basado en el dataset."""
        feature_class_mapping = {
            'URBANO_CTM12': 'U_MANZANA_CTM12',
            'URBANO': 'U_MANZANA',
            'RURAL_CTM12': 'R_VEREDA_CTM12',
            'RURAL': 'R_VEREDA'
        }
        
        for key in feature_class_mapping:
            if key in dataset_name:
                return feature_class_mapping[key]
        return None

    def find_geodatabase(self):
        """Encuentra y retorna la ruta de la geodatabase en la carpeta temporal."""
        try:
            if not os.path.exists(self.temp_files_path):
                raise FileNotFoundError(f"No se encontró el directorio: {self.temp_files_path}")
                
            gdbs = [f for f in os.listdir(self.temp_files_path) if f.endswith('.gdb')]
            if not gdbs:
                raise FileNotFoundError("No se encontró ninguna geodatabase (.gdb)")
                
            gdb_path = os.path.join(self.temp_files_path, gdbs[0])
            print(f"Geodatabase encontrada: {gdb_path}")
            return gdb_path
            
        except Exception as e:
            print(f"Error al buscar geodatabase: {str(e)}")
            return None

    def get_municipality_code(self, workspace, dataset, fc_name):
        """Obtiene el código de municipio del feature class especificado."""
        try:
            fc_path = os.path.join(workspace, dataset, fc_name)
            if not arcpy.Exists(fc_path):
                return None

            codes = []
            with arcpy.da.SearchCursor(fc_path, ['codigo_municipio']) as cursor:
                for i, row in enumerate(cursor):
                    if i >= 4:  # Limitar a 4 muestras
                        break
                    if row[0]:  # Si no es None o vacío
                        codes.append(row[0])

            if not codes:
                return None

            # Encontrar el código más común
            from collections import Counter
            return Counter(codes).most_common(1)[0][0]

        except Exception as e:
            print(f"Error al obtener código de municipio: {str(e)}")
            return None

    def get_municipality_info(self, mun_code):
        """Obtiene información del municipio desde la base de datos SQLite."""
        try:
            conn = sqlite3.connect(self.municipios_db)
            cursor = conn.cursor()
            
            query = "SELECT DEPARTAMENTO, MUNICIPIO FROM municipios WHERE CODIGO_DANE = ?"
            cursor.execute(query, (str(mun_code),))
            result = cursor.fetchone()
            
            conn.close()
            return result if result else (None, None)
        except Exception as e:
            print(f"Error al consultar base de datos de municipios: {str(e)}")
            return None, None

    def update_excel_files(self, depto, municipio):
        try:
            today = datetime.now().strftime("%d-%m-%Y")
            
            for folder in os.listdir(self.topology_path):
                folder_path = os.path.join(self.topology_path, folder)
                if not os.path.isdir(folder_path):
                    continue

                excel_files = [f for f in os.listdir(folder_path) if f.endswith(('.xlsx', '.xls'))]
                if not excel_files:
                    warnings.warn(f"No se encontró archivo Excel en {folder_path}")
                    continue

                excel_path = os.path.join(folder_path, excel_files[0])
                workbook = openpyxl.load_workbook(excel_path)
                
                # Mapeo de carpetas a nombres de hojas
                sheet_mapping = {
                    'URBANO_CTM12': 'Consis.Topologica URBANO',
                    'RURAL_CTM12': 'Consis.Topologica RURAL',
                    'URBANO': 'Consis.Topologica URBANO',
                    'RURAL': 'Consis.Topologica RURAL',

                }
                
                # Actualizar solo la hoja correspondiente a la carpeta
                if folder in sheet_mapping and sheet_mapping[folder] in workbook.sheetnames:
                    sheet = workbook[sheet_mapping[folder]]
                    sheet['D2'] = depto
                    sheet['D3'] = municipio
                    sheet['H3'] = today
                    workbook.save(excel_path)
                    print(f"Actualizado archivo Excel en: {excel_path}")
                else:
                    warnings.warn(f"Hoja '{sheet_mapping.get(folder, 'desconocida')}' no encontrada en {excel_files[0]}")
                        
        except Exception as e:
            print(f"Error al actualizar archivos Excel: {str(e)}")

    def process(self):
        """Ejecuta el proceso completo de obtención y actualización de datos."""
        try:
            print("Iniciando procesamiento de datos municipales...")
            
            # Encontrar geodatabase
            gdb_path = self.find_geodatabase()
            if not gdb_path:
                raise Exception("No se encontró archivo .gdb")
            print(f"Usando geodatabase: {gdb_path}")

            # Obtener datasets activos
            active_datasets = self.get_active_datasets()
            if not active_datasets:
                raise Exception("No se encontraron datasets activos")

            # Buscar código de municipio
            mun_code = None
            for dataset in active_datasets:
                fc_name = self.get_feature_class_name(dataset)
                if not fc_name:
                    continue

                print(f"Buscando en dataset {dataset} con feature class {fc_name}")
                mun_code = self.get_municipality_code(gdb_path, dataset, fc_name)
                if mun_code:
                    print(f"Código de municipio encontrado: {mun_code}")
                    break

            if not mun_code:
                raise Exception("No se pudo obtener un código de municipio válido")

            # Obtener información del municipio
            depto, municipio = self.get_municipality_info(mun_code)
            if not depto or not municipio:
                raise Exception(f"No se encontró información para el código: {mun_code}")

            print(f"Información encontrada - Departamento: {depto}, Municipio: {municipio}")
            
            # Actualizar archivos Excel
            self.update_excel_files(depto, municipio)
            
            print("Proceso completado exitosamente")
            return True

        except Exception as e:
            print(f"Error durante el procesamiento: {str(e)}")
            return False

def main():
    try:
        processor = MunicipalityDataProcessor()
        if processor.process():
            print("Proceso completado exitosamente")
        else:
            print("El proceso no se completó correctamente")
    except Exception as e:
        print(f"Error en la ejecución principal: {str(e)}")

if __name__ == "__main__":
    main()