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
        self.base_path = self.get_project_root()
        self.temp_files_path = os.path.join(self.base_path, 'Files', 'Temporary_Files', 'MODELO_INTERNO_1_0')
        self.municipios_db = os.path.join(self.base_path, 'Files', 'Municipios', 'municipios.db')
        self.topology_path = os.path.join(self.temp_files_path, '02_TOPOLOGIA')

    def get_project_root(self):
        current = Path(os.path.abspath(os.getcwd())).resolve()
        
        while current.parent != current:
            required_paths = [
                current / "Files" / "Temporary_Files" / "MODELO_INTERNO_1_0",
                current / "Files" / "Municipios" / "municipios.db",
                current / "Files" / "Temporary_Files" / "MODELO_INTERNO_1_0" / "02_TOPOLOGIA"
            ]
            
            if all(path.exists() for path in required_paths):
                print(f"Raíz del proyecto encontrada en: {current}")
                return str(current)
            
            current = current.parent
        
        raise Exception(
            "No se encontró la raíz del proyecto.\n"
            "Verifique que está ejecutando el script desde dentro del proyecto y que existe "
            "la siguiente estructura necesaria"
        )

    def find_geodatabase(self):
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
  
    def get_municipality_code(self, workspace):
        """
        Busca el código de municipio en feature classes específicos dentro de URBANO_CTM12 y RURAL_CTM12.
        """
        try:
            target_datasets = ['URBANO_CTM12', 'RURAL_CTM12']
            target_features = ['_UNIDAD_', '_TERRENO_', '_MANZANA_', '_VEREDA_', '_CONSTRUCCION_']
            
            arcpy.env.workspace = workspace
            
            for dataset in arcpy.ListDatasets('*', 'Feature'):
                if dataset not in target_datasets:
                    continue
                
                print(f"\nBuscando en dataset: {dataset}")
                
                for fc in arcpy.ListFeatureClasses('*', feature_dataset=dataset):
                    if not any(target in fc.upper() for target in target_features):
                        continue
                        
                    fc_path = os.path.join(workspace, dataset, fc)
                    
                    if 'codigo_municipio' not in [field.name for field in arcpy.ListFields(fc_path)]:
                        continue
                    
                    print(f"Revisando feature class: {fc}")
                    
                    result = arcpy.GetCount_management(fc_path)
                    if int(result[0]) == 0:
                        print(f"No hay registros en {fc}")
                        continue
                    
                    codes = []
                    with arcpy.da.SearchCursor(fc_path, ['codigo_municipio']) as cursor:
                        for i, row in enumerate(cursor):
                            if i >= 4:
                                break
                            if row[0]:
                                codes.append(row[0])
                    
                    if codes:
                        from collections import Counter
                        most_common = Counter(codes).most_common(1)[0][0]
                        print(f"Código de municipio encontrado en {fc}: {most_common}")
                        return most_common
                    
                print(f"No se encontraron códigos válidos en el dataset {dataset}")
            
            print("No se encontraron códigos de municipio en ningún dataset")
            return None

        except Exception as e:
            print(f"Advertencia al buscar código de municipio: {str(e)}")
            return None

    def get_excel_files(self):
        """Obtiene todos los archivos Excel en el directorio de topología"""
        try:
            excel_files = [f for f in os.listdir(self.topology_path) if f.endswith(('.xlsx', '.xls'))]
            if not excel_files:
                raise FileNotFoundError(f"No se encontraron archivos Excel en {self.topology_path}")
            
            return [os.path.join(self.topology_path, f) for f in excel_files]
        except Exception as e:
            print(f"Error al buscar archivos Excel: {str(e)}")
            return []

    def update_excel_files(self, depto, municipio):
        try:
            today = datetime.now().strftime("%d-%m-%Y")
            excel_files = self.get_excel_files()
            
            if not excel_files:
                print("No se encontraron archivos Excel para actualizar")
                return

            print(f"\nActualizando archivos Excel con:")
            print(f"Departamento: {depto}")
            print(f"Municipio: {municipio}")
            print(f"Fecha: {today}")

            for excel_path in excel_files:
                try:
                    print(f"\nProcesando archivo: {os.path.basename(excel_path)}")
                    workbook = openpyxl.load_workbook(excel_path)
                    
                    sheet_name = 'Consistencia Topologica'
                    if sheet_name in workbook.sheetnames:
                        sheet = workbook[sheet_name]
                        
                        try:
                            sheet['D3'] = depto  # Primera celda del merge D3:F3
                            sheet['D4'] = municipio  # Primera celda del merge D4:F4
                            sheet['H4'] = today  # Primera celda del merge H4:K4
                            
                            print(f"Actualizada hoja: {sheet_name}")
                        except Exception as e:
                            print(f"Advertencia al escribir en celdas: {str(e)}")

                    workbook.save(excel_path)
                    print(f"Guardado archivo Excel: {excel_path}")
                    
                except Exception as e:
                    print(f"Error procesando archivo {excel_path}: {str(e)}")
                    continue
                
        except Exception as e:
            print(f"Advertencia al actualizar archivos Excel: {str(e)}")
       
    def process(self):
        try:
            print("Iniciando procesamiento de datos municipales...")
            
            gdb_path = self.find_geodatabase()
            if not gdb_path:
                print("No se encontró archivo .gdb")
                return False

            print(f"Usando geodatabase: {gdb_path}")

            # Obtener código de municipio
            mun_code = self.get_municipality_code(gdb_path)
            if not mun_code:
                print("No se encontró código de municipio en los datasets")
                self.update_excel_files("", "")  # Actualizar con valores vacíos
                return True

            # Obtener información del municipio
            depto, municipio = self.get_municipality_info(mun_code)
            if not depto or not municipio:
                print(f"No se encontró información para el código: {mun_code}")
                self.update_excel_files("", "")  # Actualizar con valores vacíos
                return True

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