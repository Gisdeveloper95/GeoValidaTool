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
        self.temp_files_path = os.path.join(self.base_path, 'Files', 'Temporary_Files', 'MODELO_IGAC')
        self.db_path = os.path.join(self.temp_files_path, 'db', 'registro_errores.db')
        
        # Mapping for English-Spanish rule descriptions
        self.rule_descriptions = {
            'Must Not Have Gaps': 'No debe tener espacios',
            'Must Not Overlap': 'No debe superponerse',
            'Must Be Covered By Feature Class Of': 'Debe ser cubierto por la clase de entidad de',
            'Must Cover Each Other': 'Deben cubrirse entre ellos'
        }
        
        # Configuración combinada de reglas para ambos tipos
        self.rules_config = {
        'URBANO_CTM12': [
                # Must Not Have Gaps
                ('U_TERRENO_CTM12', 'U_TERRENO_CTM12', 'Must Not Have Gaps', 'H5', 'I5'),
                ('U_TERRENO_INFORMAL', 'U_TERRENO_INFORMAL', 'Must Not Have Gaps', 'H6', 'I6'),
                ('U_MANZANA_CTM12', 'U_MANZANA_CTM12', 'Must Not Have Gaps', 'H7', 'I7'),
                ('U_SECTOR_CTM12', 'U_SECTOR_CTM12', 'Must Not Have Gaps', 'H8', 'I8'),
                ('U_BARRIO_CTM12', 'U_BARRIO_CTM12', 'Must Not Have Gaps', 'H9', 'I9'),
                ('U_PERIMETRO_CTM12', 'U_PERIMETRO_CTM12', 'Must Not Have Gaps', 'H10', 'I10'),
                ('U_ZONA_HOMOGENEA_FISICA_CTM12', 'U_ZONA_HOMOGENEA_FISICA_CTM12', 'Must Not Have Gaps', 'H11', 'I11'),
                ('U_ZONA_HOMO_GEOECONOMICA_CTM12', 'U_ZONA_HOMO_GEOECONOMICA_CTM12', 'Must Not Have Gaps', 'H12', 'I12'),
                
                # Must Not Overlap (ya están completos, se mantienen igual)
                ('U_TERRENO_CTM12', 'U_TERRENO_CTM12', 'Must Not Overlap', 'H14', 'I14'),
                ('U_TERRENO_INFORMAL', 'U_TERRENO_INFORMAL', 'Must Not Overlap', 'H15', 'I15'),
                ('U_MANZANA_CTM12', 'U_MANZANA_CTM12', 'Must Not Overlap', 'H16', 'I16'),
                ('U_SECTOR_CTM12', 'U_SECTOR_CTM12', 'Must Not Overlap', 'H17', 'I17'),
                ('U_BARRIO_CTM12', 'U_BARRIO_CTM12', 'Must Not Overlap', 'H18', 'I18'),
                ('U_PERIMETRO_CTM12', 'U_PERIMETRO_CTM12', 'Must Not Overlap', 'H19', 'I19'),
                ('U_CONSTRUCCION_CTM12', 'U_CONSTRUCCION_CTM12', 'Must Not Overlap', 'H20', 'I20'),
                ('U_CONSTRUCCION_INFORMAL', 'U_CONSTRUCCION_INFORMAL', 'Must Not Overlap', 'H21', 'I21'),
                ('U_UNIDAD_CTM12', 'U_UNIDAD_CTM12', 'Must Not Overlap', 'H22', 'I22'),
                ('U_UNIDAD_INFORMAL', 'U_UNIDAD_INFORMAL', 'Must Not Overlap', 'H23', 'I23'),
                ('U_NOMEN_DOMICILIARIA_CTM12', 'U_NOMEN_DOMICILIARIA_CTM12', 'Must Not Overlap', 'H24', 'I24'),
                ('U_ZONA_HOMOGENEA_FISICA_CTM12', 'U_ZONA_HOMOGENEA_FISICA_CTM12', 'Must Not Overlap', 'H25', 'I25'),
                ('U_ZONA_HOMO_GEOECONOMICA_CTM12', 'U_ZONA_HOMO_GEOECONOMICA_CTM12', 'Must Not Overlap', 'H26', 'I26'),
                
                # Must Be Covered By Feature Class Of
                ('U_SECTOR_CTM12', 'U_PERIMETRO_CTM12', 'Must Be Covered By Feature Class Of', 'H28', 'I28'),
                ('U_BARRIO_CTM12', 'U_SECTOR_CTM12', 'Must Be Covered By Feature Class Of', 'H29', 'I29'),
                ('U_MANZANA_CTM12', 'U_BARRIO_CTM12', 'Must Be Covered By Feature Class Of', 'H30', 'I30'),
                ('U_TERRENO_CTM12', 'U_MANZANA_CTM12', 'Must Be Covered By Feature Class Of', 'H31', 'I31'),
                ('U_CONSTRUCCION_CTM12', 'U_TERRENO_CTM12', 'Must Be Covered By Feature Class Of', 'H32', 'I32'),
                ('U_CONSTRUCCION_INFORMAL', 'U_TERRENO_INFORMAL', 'Must Be Covered By Feature Class Of', 'H33', 'I33'),
                ('U_TERRENO_INFORMAL', 'U_TERRENO_CTM12', 'Must Be Covered By Feature Class Of', 'H34', 'I34'),
                ('U_TERRENO_CTM12', 'U_ZONA_HOMO_GEOECONOMICA_CTM12', 'Must Be Covered By Feature Class Of', 'H35', 'I35'),
                ('U_TERRENO_CTM12', 'U_ZONA_HOMOGENEA_FISICA_CTM12', 'Must Be Covered By Feature Class Of', 'H36', 'I36'),
                
                # Must Cover Each Other
                ('U_TERRENO_CTM12', 'U_MANZANA_CTM12', 'Must Cover Each Other', 'H38', 'I38'),
                ('U_UNIDAD_CTM12', 'U_CONSTRUCCION_CTM12', 'Must Cover Each Other', 'H39', 'I39'),
                ('U_UNIDAD_INFORMAL', 'U_CONSTRUCCION_INFORMAL', 'Must Cover Each Other', 'H40', 'I40'),
                ('U_ZONA_HOMO_GEOECONOMICA_CTM12', 'U_ZONA_HOMOGENEA_FISICA_CTM12', 'Must Cover Each Other', 'H41', 'I41')
                ],
        'RURAL_CTM12': [
                # Must Not Have Gaps
                ('R_TERRENO_CTM12', 'R_TERRENO_CTM12', 'Must Not Have Gaps', 'H5', 'I5'),
                ('R_TERRENO_INFORMAL', 'R_TERRENO_INFORMAL', 'Must Not Have Gaps', 'H6', 'I6'),
                ('R_VEREDA_CTM12', 'R_VEREDA_CTM12', 'Must Not Have Gaps', 'H7', 'I7'),
                ('R_SECTOR_CTM12', 'R_SECTOR_CTM12', 'Must Not Have Gaps', 'H8', 'I8'),
                ('R_ZONA_HOMOGENEA_FISICA_CTM12', 'R_ZONA_HOMOGENEA_FISICA_CTM12', 'Must Not Have Gaps', 'H9', 'I9'),
                ('R_ZONA_HOMO_GEOECONOMICA_CTM12', 'R_ZONA_HOMO_GEOECONOMICA_CTM12', 'Must Not Have Gaps', 'H10', 'I10'),

                # Must Not Overlap
                ('R_TERRENO_CTM12', 'R_TERRENO_CTM12', 'Must Not Overlap', 'H12', 'I12'),
                ('R_TERRENO_INFORMAL', 'R_TERRENO_INFORMAL', 'Must Not Overlap', 'H13', 'I13'),
                ('R_VEREDA_CTM12', 'R_VEREDA_CTM12', 'Must Not Overlap', 'H14', 'I14'),
                ('R_SECTOR_CTM12', 'R_SECTOR_CTM12', 'Must Not Overlap', 'H15', 'I15'),
                ('R_CONSTRUCCION_CTM12', 'R_CONSTRUCCION_CTM12', 'Must Not Overlap', 'H16', 'I16'),
                ('R_CONSTRUCCION_INFORMAL', 'R_CONSTRUCCION_INFORMAL', 'Must Not Overlap', 'H17', 'I17'),
                ('R_UNIDAD_CTM12', 'R_UNIDAD_CTM12', 'Must Not Overlap', 'H18', 'I18'),
                ('R_UNIDAD_INFORMAL', 'R_UNIDAD_INFORMAL', 'Must Not Overlap', 'H19', 'I19'),
                ('R_NOMEN_DOMICILIARIA_CTM12', 'R_NOMEN_DOMICILIARIA_CTM12', 'Must Not Overlap', 'H20', 'I20'),
                ('R_ZONA_HOMOGENEA_FISICA_CTM12', 'R_ZONA_HOMOGENEA_FISICA_CTM12', 'Must Not Overlap', 'H21', 'I21'),
                ('R_ZONA_HOMO_GEOECONOMICA_CTM12', 'R_ZONA_HOMO_GEOECONOMICA_CTM12', 'Must Not Overlap', 'H22', 'I22'),

                # Must Be Covered By Feature Class Of
                ('R_VEREDA_CTM12', 'R_SECTOR_CTM12', 'Must Be Covered By Feature Class Of', 'H24', 'I24'),
                ('R_TERRENO_CTM12', 'R_VEREDA_CTM12', 'Must Be Covered By Feature Class Of', 'H25', 'I25'),
                ('R_CONSTRUCCION_CTM12', 'R_TERRENO_CTM12', 'Must Be Covered By Feature Class Of', 'H26', 'I26'),
                ('R_CONSTRUCCION_INFORMAL', 'R_TERRENO_INFORMAL', 'Must Be Covered By Feature Class Of', 'H27', 'I27'),
                ('R_TERRENO_INFORMAL', 'R_TERRENO_CTM12', 'Must Be Covered By Feature Class Of', 'H28', 'I28'),
                ('R_TERRENO_CTM12', 'R_ZONA_HOMO_GEOECONOMICA_CTM12', 'Must Be Covered By Feature Class Of', 'H29', 'I29'),
                ('R_TERRENO_CTM12', 'R_ZONA_HOMOGENEA_FISICA_CTM12', 'Must Be Covered By Feature Class Of', 'H30', 'I30'),

                # Must Cover Each Other
                ('R_TERRENO_CTM12', 'R_VEREDA_CTM12', 'Must Cover Each Other', 'H32', 'I32'),
                ('R_UNIDAD_CTM12', 'R_CONSTRUCCION_CTM12', 'Must Cover Each Other', 'H33', 'I33'),
                ('R_UNIDAD_INFORMAL', 'R_CONSTRUCCION_INFORMAL', 'Must Cover Each Other', 'H34', 'I34'),
                ('R_ZONA_HOMO_GEOECONOMICA_CTM12', 'R_ZONA_HOMOGENEA_FISICA_CTM12', 'Must Cover Each Other', 'H35', 'I35')
            ],
            'URBANO': [
                # Must Not Have Gaps
                ('U_TERRENO', 'U_TERRENO', 'Must Not Have Gaps', 'H5', 'I5'),
                
                ('U_MANZANA', 'U_MANZANA', 'Must Not Have Gaps', 'H6', 'I6'),
                ('U_SECTOR', 'U_SECTOR', 'Must Not Have Gaps', 'H7', 'I7'),
                ('U_BARRIO', 'U_BARRIO', 'Must Not Have Gaps', 'H8', 'I8'),
                ('U_PERIMETRO', 'U_PERIMETRO', 'Must Not Have Gaps', 'H9', 'I9'),
                ('U_ZONA_HOMOGENEA_FISICA', 'U_ZONA_HOMOGENEA_FISICA', 'Must Not Have Gaps', 'H10', 'I10'),
                ('U_ZONA_HOMOGENEA_GEOECONOMICA', 'U_ZONA_HOMOGENEA_GEOECONOMICA', 'Must Not Have Gaps', 'H11', 'I11'),
                
                # Must Not Overlap (ya están completos, se mantienen igual)
                ('U_TERRENO', 'U_TERRENO', 'Must Not Overlap', 'H13', 'I13'),
                
                ('U_MANZANA', 'U_MANZANA', 'Must Not Overlap', 'H14', 'I14'),
                ('U_SECTOR', 'U_SECTOR', 'Must Not Overlap', 'H15', 'I15'),
                ('U_BARRIO', 'U_BARRIO', 'Must Not Overlap', 'H16', 'I16'),
                ('U_PERIMETRO', 'U_PERIMETRO', 'Must Not Overlap', 'H17', 'I17'),
                ('U_CONSTRUCCION', 'U_CONSTRUCCION', 'Must Not Overlap', 'H18', 'I18'),
                
                ('U_UNIDAD', 'U_UNIDAD', 'Must Not Overlap', 'H19', 'I19'),
                
                ('U_NOMENCLATURA_DOMICILIARIA', 'U_NOMENCLATURA_DOMICILIARIA', 'Must Not Overlap', 'H20', 'I20'),
                ('U_ZONA_HOMOGENEA_FISICA', 'U_ZONA_HOMOGENEA_FISICA', 'Must Not Overlap', 'H21', 'I21'),
                ('U_ZONA_HOMOGENEA_GEOECONOMICA', 'U_ZONA_HOMOGENEA_GEOECONOMICA', 'Must Not Overlap', 'H22', 'I22'),
                
                # Must Be Covered By Feature Class Of
                ('U_SECTOR', 'U_PERIMETRO', 'Must Be Covered By Feature Class Of', 'H24', 'I24'),
                ('U_BARRIO', 'U_SECTOR', 'Must Be Covered By Feature Class Of', 'H25', 'I25'),
                ('U_MANZANA', 'U_BARRIO', 'Must Be Covered By Feature Class Of', 'H26', 'I26'),
                ('U_TERRENO', 'U_MANZANA', 'Must Be Covered By Feature Class Of', 'H27', 'I27'),
                ('U_CONSTRUCCION', 'U_TERRENO', 'Must Be Covered By Feature Class Of', 'H28', 'I28'),
                
                ('U_TERRENO', 'U_ZONA_HOMOGENEA_GEOECONOMICA', 'Must Be Covered By Feature Class Of', 'H29', 'I29'),
                ('U_TERRENO', 'U_ZONA_HOMOGENEA_FISICA', 'Must Be Covered By Feature Class Of', 'H30', 'I30'),
                
                # Must Cover Each Other
                ('U_TERRENO', 'U_MANZANA', 'Must Cover Each Other', 'H32', 'I32'),
                ('U_UNIDAD', 'U_CONSTRUCCION', 'Must Cover Each Other', 'H33', 'I33'),
                ('U_ZONA_HOMOGENEA_GEOECONOMICA', 'U_ZONA_HOMOGENEA_FISICA', 'Must Cover Each Other', 'H34', 'I34')
                ],
        'RURAL': [
                # Must Not Have Gaps
                ('R_TERRENO', 'R_TERRENO', 'Must Not Have Gaps', 'H5', 'I5'),

                ('R_VEREDA', 'R_VEREDA', 'Must Not Have Gaps', 'H6', 'I6'),
                ('R_SECTOR', 'R_SECTOR', 'Must Not Have Gaps', 'H7', 'I7'),
                ('R_ZONA_HOMOGENEA_FISICA', 'R_ZONA_HOMOGENEA_FISICA', 'Must Not Have Gaps', 'H8', 'I8'),
                ('R_ZONA_HOMOGENEA_GEOECONOMICA', 'R_ZONA_HOMOGENEA_GEOECONOMICA', 'Must Not Have Gaps', 'H9', 'I9'),

                # Must Not Overlap
                ('R_TERRENO', 'R_TERRENO', 'Must Not Overlap', 'H11', 'I11'),

                ('R_VEREDA', 'R_VEREDA', 'Must Not Overlap', 'H12', 'I12'),
                ('R_SECTOR', 'R_SECTOR', 'Must Not Overlap', 'H13', 'I13'),
                ('R_CONSTRUCCION', 'R_CONSTRUCCION', 'Must Not Overlap', 'H14', 'I14'),

                ('R_UNIDAD', 'R_UNIDAD', 'Must Not Overlap', 'H15', 'I15'),

                ('R_NOMENCLATURA_DOMICILIARIA', 'R_NOMENCLATURA_DOMICILIARIA', 'Must Not Overlap', 'H16', 'I16'),
                ('R_ZONA_HOMOGENEA_FISICA', 'R_ZONA_HOMOGENEA_FISICA', 'Must Not Overlap', 'H17', 'I17'),
                ('R_ZONA_HOMOGENEA_GEOECONOMICA', 'R_ZONA_HOMOGENEA_GEOECONOMICA', 'Must Not Overlap', 'H18', 'I18'),

                # Must Be Covered By Feature Class Of
                ('R_VEREDA', 'R_SECTOR', 'Must Be Covered By Feature Class Of', 'H20', 'I20'),
                ('R_TERRENO', 'R_VEREDA', 'Must Be Covered By Feature Class Of', 'H21', 'I21'),
                ('R_CONSTRUCCION', 'R_TERRENO', 'Must Be Covered By Feature Class Of', 'H22', 'I22'),
                ('R_TERRENO', 'R_ZONA_HOMOGENEA_GEOECONOMICA', 'Must Be Covered By Feature Class Of', 'H23', 'I23'),
                ('R_TERRENO', 'R_ZONA_HOMOGENEA_FISICA', 'Must Be Covered By Feature Class Of', 'H24', 'I24'),

                # Must Cover Each Other
                ('R_TERRENO', 'R_VEREDA', 'Must Cover Each Other', 'H26', 'I26'),
                ('R_UNIDAD', 'R_CONSTRUCCION', 'Must Cover Each Other', 'H27', 'I27'),
                ('R_ZONA_HOMOGENEA_GEOECONOMICA', 'R_ZONA_HOMOGENEA_FISICA', 'Must Cover Each Other', 'H28', 'I28')
            ]
        }
        
        # Mapeo de tipos a nombres de hojas
        self.sheet_names = {
            'URBANO_CTM12': 'Consis.Topologica URBANO',
            'RURAL_CTM12': 'Consis.Topologica RURAL',
            'URBANO': 'Consis.Topologica URBANO',
            'RURAL': 'Consis.Topologica RURAL'
        }
        
        # Cargar configuración de datasets
        self.datasets = self.load_datasets_config()
        
    def get_rule_language(self, conn, folder):
        """Determine which language version of rules is used in the database."""
        sample_query = """
        SELECT RuleDescription 
        FROM {}
        WHERE RuleDescription IN (?, ?)
        LIMIT 1
        """.format(folder)
        
        cursor = conn.cursor()
        for eng_rule, esp_rule in self.rule_descriptions.items():
            cursor.execute(sample_query, (eng_rule, esp_rule))
            result = cursor.fetchone()
            if result:
                return 'en' if result[0] == eng_rule else 'es'
        return 'en'  # default to English if no matches found
    
    def load_datasets_config(self):
        config_path = os.path.join(self.base_path, "Files", "Temporary_Files", "array_config.txt")
        datasets = []
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    dataset_name = line.strip('",[]').strip()
                    if dataset_name:
                        datasets.append(dataset_name)
        return datasets

    def get_project_root(self):
        current_dir = os.path.abspath(os.getcwd())
        current_path = Path(current_dir)
        
        while current_path.parent != current_path:
            required_paths = [
                current_path / "Files" / "Templates" / "MODELO_IGAC"/ "02_TOPOLOGIA",
                current_path / "Files" / "Temporary_Files" / "MODELO_IGAC",
                current_path / "Files" / "Temporary_Files" / "array_config.txt",
                current_path / "Files" / "Temporary_Files" / "MODELO_IGAC" / "db",
                current_path / "Files" / "Temporary_Files" / "MODELO_IGAC" / "02_TOPOLOGIA"
            ]
            
            if all(required_path.parent.exists() for required_path in required_paths):
                print(f"Raíz del proyecto encontrada en: {current_path}")
                return str(current_path)
            
            current_path = current_path.parent
        
        raise Exception(
            "No se encontró la raíz del proyecto.\n"
            "Verifique que está ejecutando el script desde dentro del proyecto "
            "y que existe la siguiente estructura:\n"
            "- Files/Templates/MODELO_IGAC/02_TOPOLOGIA\n"
            "- Files/Temporary_Files/MODELO_IGAC\n"
            "- Files/Temporary_Files/array_config.txt\n"
            "- Files/Temporary_Files/MODELO_IGAC/db\n"
            "- Files/Temporary_Files/MODELO_IGAC/02_TOPOLOGIA"
        )

    def setup_folders(self):
        topology_template_path = os.path.join(self.templates_path,'MODELO_IGAC', '02_TOPOLOGIA')
        os.makedirs(topology_template_path, exist_ok=True)
        
        source_path = os.path.join(self.temp_files_path, '03_INCONSISTENCIAS', 'CONSISTENCIA_TOPOLOGICA')
        existing_folders = [f for f in os.listdir(source_path) if os.path.isdir(os.path.join(source_path, f))]
        
        for folder in existing_folders:
            os.makedirs(os.path.join(topology_template_path, folder), exist_ok=True)
        
        for folder in existing_folders:
            source = os.path.join(topology_template_path, folder)
            dest = os.path.join(self.temp_files_path, '02_TOPOLOGIA', folder)
            if os.path.exists(source):
                shutil.copytree(source, dest, dirs_exist_ok=True)

    def get_excel_file(self, folder_path):
        excel_files = [f for f in os.listdir(folder_path) if f.endswith(('.xlsx', '.xls'))]
        if not excel_files:
            raise Exception(f"No se encontró archivo Excel en {folder_path}")
        return os.path.join(folder_path, excel_files[0])

    def get_rule_description(self, rule_desc, language):
        """Get the appropriate rule description based on language."""
        if language == 'es':
            return self.rule_descriptions.get(rule_desc, rule_desc)
        return rule_desc

    def analyze_topology(self):
        topology_path = os.path.join(self.temp_files_path, '02_TOPOLOGIA')
        folders_to_process = [f for f in os.listdir(topology_path) 
                            if os.path.isdir(os.path.join(topology_path, f)) 
                            and f in self.datasets]
        
        conn = sqlite3.connect(self.db_path)
        
        for folder in folders_to_process:
            if folder in self.rules_config:
                rule_language = self.get_rule_language(conn, folder)
                folder_path = os.path.join(topology_path, folder)
                excel_path = self.get_excel_file(folder_path)
                
                workbook = openpyxl.load_workbook(excel_path)
                sheet_name = self.sheet_names[folder]
                
                if sheet_name not in workbook.sheetnames:
                    raise Exception(f"No se encontró la hoja '{sheet_name}' en el archivo Excel")
                sheet = workbook[sheet_name]
                
                for rule in self.rules_config[folder]:
                    origin_class, dest_class, rule_desc, cell_no_exception, cell_exception = rule
                    db_rule_desc = self.get_rule_description(rule_desc, rule_language)
                    
                    # Consulta base modificada para manejar reglas bidireccionales
                    if rule_desc == "Must Cover Each Other":
                        # Para Must Cover Each Other, buscar en ambas direcciones
                        total_query = """
                        SELECT COUNT(*) as total
                        FROM {}
                        WHERE (
                            (OriginObjectClassName = ? AND DestinationObjectClassName = ? AND RuleDescription = ?)
                            OR 
                            (OriginObjectClassName = ? AND DestinationObjectClassName = ? AND RuleDescription = ?)
                        )
                        """.format(folder)
                        
                        exception_query = """
                        SELECT COUNT(*) as count
                        FROM {}
                        WHERE (
                            (OriginObjectClassName = ? AND DestinationObjectClassName = ? AND RuleDescription = ?)
                            OR 
                            (OriginObjectClassName = ? AND DestinationObjectClassName = ? AND RuleDescription = ?)
                        )
                        AND isException != 0
                        """.format(folder)
                        
                        # Ejecutar consultas con parámetros duplicados para ambas direcciones
                        cursor = conn.execute(total_query, 
                                        (origin_class, dest_class, db_rule_desc,
                                            dest_class, origin_class, db_rule_desc))
                        total_count = cursor.fetchone()[0]
                        
                        cursor = conn.execute(exception_query,
                                        (origin_class, dest_class, db_rule_desc,
                                            dest_class, origin_class, db_rule_desc))
                        exception_count = cursor.fetchone()[0]
                        
                    else:
                        # Para otras reglas, mantener la consulta original
                        total_query = """
                        SELECT COUNT(*) as total
                        FROM {}
                        WHERE OriginObjectClassName = ? 
                        AND CASE 
                            WHEN ? = '' THEN DestinationObjectClassName = ''
                            ELSE DestinationObjectClassName = ?
                        END
                        AND RuleDescription = ?
                        """.format(folder)
                        
                        exception_query = """
                        SELECT COUNT(*) as count
                        FROM {}
                        WHERE OriginObjectClassName = ? 
                        AND CASE 
                            WHEN ? = '' THEN DestinationObjectClassName = ''
                            ELSE DestinationObjectClassName = ?
                        END
                        AND RuleDescription = ?
                        AND isException != 0
                        """.format(folder)
                        
                        cursor = conn.execute(total_query, (origin_class, dest_class, dest_class, db_rule_desc))
                        total_count = cursor.fetchone()[0]
                        
                        cursor = conn.execute(exception_query, (origin_class, dest_class, dest_class, db_rule_desc))
                        exception_count = cursor.fetchone()[0]
                    
                    # Actualizar celdas en Excel
                    sheet[cell_no_exception] = total_count
                    sheet[cell_exception] = exception_count
                
                workbook.save(excel_path)
        
        conn.close()

        
    def run(self):
        try:
            print("\nConfiguracion de datasets cargada:")
            print("--------------------------------")
            print("Datasets que serán procesados:")
            for ds in self.datasets:
                print(f"  - {ds}")
            print("--------------------------------\n")

            print("Configurando estructura de carpetas...")
            self.setup_folders()
            
            print("Analizando topología y actualizando archivos Excel...")
            self.analyze_topology()
            
            print("Proceso completado exitosamente")
        except Exception as e:
            print(f"Error durante la ejecución: {str(e)}")

if __name__ == "__main__":
    analyzer = TopologyAnalyzer()
    analyzer.run()