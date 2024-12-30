import os
import sqlite3
import shutil
import openpyxl
from pathlib import Path
import sys
sys.stdout.reconfigure(encoding='utf-8')

class TopologyAnalyzer:
    def __init__(self):
        self.base_path = self.get_project_root()
        self.templates_path = os.path.join(self.base_path, 'Files', 'Templates')
        self.temp_files_path = os.path.join(self.base_path, 'Files', 'Temporary_Files', 'MODELO_LADM_1_2')
        self.datasets_to_process = self.load_dataset_configuration()
        
        # Configuración de rangos de celdas para cada sección
        self.cell_ranges = {
            'Consistencia Topologica': [
                ('I6', 'I21', 'J6', 'R_1', 'R_16'),
                ('I23', 'I45', 'J23', 'R_17', 'R_39'),
                ('I47', 'I63', 'J47', 'R_40', 'R_56'),
                ('I65', 'I70', 'J65', 'R_57', 'R_62')
            ],
            'Consistencia Formato-CartoCatas': [
                ('G6', 'G23', 'H6', 'R_63', 'R_79')
            ]
        }

    def load_dataset_configuration(self):
        """Carga la configuración de datasets desde array_config.txt"""
        config_path = os.path.join(self.base_path, 'Files', 'Temporary_Files', 'array_config.txt')
        datasets = []
        
        try:
            with open(config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        dataset_name = line.strip('",[]').strip()
                        if dataset_name:
                            datasets.append(dataset_name)
            
            print("\nDatasets configurados para procesar:")
            for ds in datasets:
                print(f"  - {ds}")
            print()
            
            return datasets
        except Exception as e:
            print(f"Error leyendo configuración de datasets: {str(e)}")
            return []

    def get_project_root(self):
        current_dir = os.path.abspath(os.getcwd())
        current_path = Path(current_dir)
        
        while current_path.parent != current_path:
            required_paths = [
                current_path / "Files" / "Templates" / "MODELO_LADM_1_2" / "02_TOPOLOGIA",
                current_path / "Files" / "Temporary_Files" / "MODELO_LADM_1_2",
                current_path / "Files" / "Temporary_Files" / "array_config.txt",
                current_path / "Files" / "Temporary_Files" / "MODELO_LADM_1_2" / "db",
                current_path / "Files" / "Temporary_Files" / "MODELO_LADM_1_2" / "02_TOPOLOGIA"
            ]
            
            if all(required_path.parent.exists() for required_path in required_paths):
                print(f"Raíz del proyecto encontrada en: {current_path}")
                return str(current_path)
            
            current_path = current_path.parent
        
        raise Exception(
            "No se encontró la raíz del proyecto.\n"
            "Verifique que está ejecutando el script desde dentro del proyecto "
            "y que existe la estructura necesaria"
        )

    def copy_excel_template(self):
        """Copia el archivo Excel template según los datasets configurados."""
        template_topologia_path = os.path.join(self.templates_path, 'MODELO_LADM_1_2', '02_TOPOLOGIA')
        dest_topologia_path = os.path.join(self.temp_files_path, '02_TOPOLOGIA')
        copied_files = []
        
        # Asegurarse de que el directorio destino existe
        os.makedirs(dest_topologia_path, exist_ok=True)
        
        # Buscar archivo Excel en la carpeta de templates
        excel_files = [f for f in os.listdir(template_topologia_path) if f.endswith(('.xlsx', '.xls'))]
        
        if not excel_files:
            raise Exception("No se encontró archivo Excel template en la carpeta Templates")
        
        template_excel = os.path.join(template_topologia_path, excel_files[0])
        base_name = os.path.splitext(excel_files[0])[0]
        extension = os.path.splitext(excel_files[0])[1]
        
        # Copiar el archivo para cada dataset configurado
        for dataset in self.datasets_to_process:
            if dataset == "URBANO_CTM12":
                dest_name = f"{base_name}_Urbano{extension}"
                dest_path = os.path.join(dest_topologia_path, dest_name)
                print(f"Copiando template Excel urbano a: {dest_path}")
                shutil.copy2(template_excel, dest_path)
                copied_files.append(("urbano", dest_path))
                
            elif dataset == "RURAL_CTM12":
                dest_name = f"{base_name}_Rural{extension}"
                dest_path = os.path.join(dest_topologia_path, dest_name)
                print(f"Copiando template Excel rural a: {dest_path}")
                shutil.copy2(template_excel, dest_path)
                copied_files.append(("rural", dest_path))
        
        return copied_files

    def setup_folders(self):
        print("Configurando estructura de carpetas...")
        
        # Crear directorios necesarios
        topology_template_path = os.path.join(self.templates_path, 'MODELO_LADM_1_2', '02_TOPOLOGIA')
        os.makedirs(topology_template_path, exist_ok=True)
        
        dest_topology_path = os.path.join(self.temp_files_path, '02_TOPOLOGIA')
        os.makedirs(dest_topology_path, exist_ok=True)
        
        # Copiar los archivos Excel template
        try:
            copied_files = self.copy_excel_template()
            if copied_files:
                print("Templates Excel copiados exitosamente:")
                for dataset_type, path in copied_files:
                    print(f"  - {dataset_type.upper()}: {path}")
            else:
                print("No se copiaron templates Excel (no hay datasets configurados)")
            return copied_files
        except Exception as e:
            print(f"Error al copiar los templates Excel: {str(e)}")
            return []

    def get_table_names(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'R_%'")
        return [row[0] for row in cursor.fetchall()]

    def get_cell_row_number(self, cell_ref):
        return int(''.join(filter(str.isdigit, cell_ref)))

    def analyze_topology_for_dataset(self, excel_path, db_path, dataset_type):
        """Analiza la topología para un dataset específico"""
        if not os.path.exists(db_path):
            print(f"Base de datos no encontrada para {dataset_type}: {db_path}")
            return
        
        try:
            print(f"\nProcesando análisis para dataset {dataset_type.upper()}")
            print(f"Excel: {excel_path}")
            print(f"Base de datos: {db_path}")
            
            workbook = openpyxl.load_workbook(excel_path)
            conn = sqlite3.connect(db_path)
            tables = self.get_table_names(conn)
            
            processed_tables = set()
            
            for sheet_name, ranges in self.cell_ranges.items():
                if sheet_name not in workbook.sheetnames:
                    print(f"Advertencia: No se encontró la hoja '{sheet_name}'")
                    continue
                    
                sheet = workbook[sheet_name]
                print(f"\nProcesando hoja: {sheet_name}")
                
                for start_count, end_count, start_except, start_rule, end_rule in ranges:
                    start_row = self.get_cell_row_number(start_count)
                    end_row = self.get_cell_row_number(end_count)
                    except_row = self.get_cell_row_number(start_except)
                    
                    count_col = ''.join(filter(str.isalpha, start_count))
                    except_col = ''.join(filter(str.isalpha, start_except))
                    
                    current_rule_num = int(start_rule.split('_')[1])
                    end_rule_num = int(end_rule.split('_')[1])
                    
                    for row in range(start_row, end_row + 1):
                        rule_number = current_rule_num + (row - start_row)
                        if rule_number > end_rule_num:
                            break
                        
                        rule_prefix = f'R_{rule_number}_'
                        matching_tables = [t for t in tables if t.startswith(rule_prefix)]
                        
                        if matching_tables:
                            total_count = 0
                            exception_count = 0
                            
                            print(f"\nProcesando regla {rule_number}:")
                            for table_name in matching_tables:
                                processed_tables.add(table_name)
                                
                                cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                                table_count = cursor.fetchone()[0]
                                total_count += table_count
                                
                                cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE isExceptio != 0")
                                table_exceptions = cursor.fetchone()[0]
                                exception_count += table_exceptions
                                
                                print(f"  - Tabla {table_name}: {table_count} registros, {table_exceptions} excepciones")
                            
                            count_cell = f"{count_col}{row}"
                            except_cell = f"{except_col}{row}"
                            
                            sheet[count_cell] = total_count
                            sheet[except_cell] = exception_count
                            
                            print(f"  Total para R_{rule_number}: {total_count} registros, {exception_count} excepciones")
                        else:
                            sheet[f"{count_col}{row}"] = 0
                            sheet[f"{except_col}{row}"] = 0
                            print(f"No se encontraron tablas para R_{rule_number}")
            
            workbook.save(excel_path)
            conn.close()
            
            print(f"\nRESUMEN DE PROCESAMIENTO PARA {dataset_type.upper()}:")
            print("="*50)
            print(f"Total de tablas procesadas: {len(processed_tables)}")
            print("\nTablas procesadas:")
            for table in sorted(processed_tables):
                print(f"  - {table}")
            print("="*50)
            
        except Exception as e:
            print(f"Error procesando dataset {dataset_type}: {str(e)}")

    def analyze_topology(self):
        """Analiza la topología para todos los datasets configurados"""
        for dataset in self.datasets_to_process:
            if dataset == "URBANO_CTM12":
                excel_path = os.path.join(self.temp_files_path, '02_TOPOLOGIA', 
                                        self.get_excel_file_by_suffix('_Urbano.xlsx'))
                db_path = os.path.join(self.temp_files_path, 'db', 'registro_errores_urbano.db')
                self.analyze_topology_for_dataset(excel_path, db_path, "urbano")
                
            elif dataset == "RURAL_CTM12":
                excel_path = os.path.join(self.temp_files_path, '02_TOPOLOGIA', 
                                        self.get_excel_file_by_suffix('_Rural.xlsx'))
                db_path = os.path.join(self.temp_files_path, 'db', 'registro_errores_rural.db')
                self.analyze_topology_for_dataset(excel_path, db_path, "rural")

    def get_excel_file_by_suffix(self, suffix):
        """Encuentra el archivo Excel con el sufijo especificado"""
        topology_path = os.path.join(self.temp_files_path, '02_TOPOLOGIA')
        excel_files = [f for f in os.listdir(topology_path) if f.endswith(suffix)]
        if not excel_files:
            raise Exception(f"No se encontró archivo Excel con sufijo {suffix}")
        return excel_files[0]

    def run(self):
        try:
            print("Iniciando proceso...")
            copied_files = self.setup_folders()
            
            if not copied_files:
                print("No hay datasets configurados para procesar")
                return
            
            print("\nAnalizando topología y actualizando archivos Excel...")
            self.analyze_topology()
            
            print("\nProceso completado exitosamente")
        except Exception as e:
            print(f"Error durante la ejecución: {str(e)}")

if __name__ == "__main__":
    analyzer = TopologyAnalyzer()
    analyzer.run()