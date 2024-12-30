import arcpy
import os
import re
import logging

class TopologyRuleProcessor:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        # Diccionario de traducción de reglas
        self.rule_descriptions = {
            'Must Not Have Gaps': 'No debe tener espacios',
            'Must Not Overlap': 'No debe superponerse',
            'Must Be Covered By Feature Class Of': 'Debe ser cubierto por la clase de entidad de',
            'Must Cover Each Other': 'Deben cubrirse entre ellos'
        }
        
        # Mapeo inverso para búsqueda
        self.rule_descriptions_inverse = {v: k for k, v in self.rule_descriptions.items()}
        
        # Reglas de topología
        self.TOPOLOGY_RULES = {
            "URBANO_CTM12_Topology": {
                "dataset": "URBANO_CTM12",  
                "must_not_overlap": [
                    "U_TERRENO_CTM12", "U_TERRENO_INFORMAL", "U_MANZANA_CTM12",
                    "U_SECTOR_CTM12", "U_BARRIO_CTM12", "U_PERIMETRO_CTM12",
                    "U_MANZANA_CTM12",
                    "U_CONSTRUCCION_CTM12", "U_CONSTRUCCION_INFORMAL",
                    "U_ZONA_HOMOGENEA_FISICA_CTM12",
                    "U_ZONA_HOMO_GEOECONOMICA_CTM12"
                ],
                "must_not_have_gaps": [
                    "U_TERRENO_CTM12", "U_TERRENO_INFORMAL", "U_MANZANA_CTM12",
                    "U_SECTOR_CTM12", "U_BARRIO_CTM12", "U_PERIMETRO_CTM12",
                    "U_ZONA_HOMOGENEA_FISICA_CTM12", "U_ZONA_HOMO_GEOECONOMICA_CTM12"
                ],
                "must_be_covered_by": [
                    ("U_SECTOR_CTM12", "U_PERIMETRO_CTM12"),
                    ("U_BARRIO_CTM12", "U_SECTOR_CTM12"),
                    ("U_TERRENO_CTM12", "U_MANZANA_CTM12"),
                    ("U_CONSTRUCCION_CTM12", "U_TERRENO_CTM12"),
                    ("U_CONSTRUCCION_INFORMAL", "U_TERRENO_INFORMAL"),
                    ("U_TERRENO_INFORMAL","U_TERRENO_CTM12")
                ],
                "must_cover_each_other":[
                    ("U_TERRENO_CTM12", "U_MANZANA_CTM12"),
                    ("U_UNIDAD_CTM12", "U_CONSTRUCCION_CTM12"),
                    ("U_UNIDAD_INFORMAL", "U_CONSTRUCCION_INFORMAL"),
                    ("U_ZONA_HOMO_GEOECONOMICA_CTM12", "U_ZONA_HOMOGENEA_FISICA_CTM12")
                ]
            },
            "RURAL_CTM12_Topology": {
                "dataset": "RURAL_CTM12",  
                "must_not_overlap": [
                    "R_TERRENO_CTM12", "R_TERRENO_INFORMAL",
                    "R_CONSTRUCCION_CTM12", "R_CONSTRUCCION_INFORMAL",
                    "R_VEREDA_CTM12","R_SECTOR_CTM12",
                    "R_UNIDAD_CTM12","R_UNIDAD_INFORMAL",
                    "R_ZONA_HOMOGENEA_FISICA_CTM12",
                    "R_ZONA_HOMO_GEOECONOMICA_CTM12"
                ],
                "must_not_have_gaps": [
                    "R_TERRENO_CTM12", "R_TERRENO_INFORMAL",
                    "R_VEREDA_CTM12", "R_SECTOR_CTM12",
                    "R_ZONA_HOMOGENEA_FISICA_CTM12","R_ZONA_HOMO_GEOECONOMICA_CTM12"
                ],
                "must_be_covered_by": [
                    ("R_VEREDA_CTM12", "R_SECTOR_CTM12"),
                    ("R_TERRENO_CTM12", "R_VEREDA_CTM12"),
                    ("R_CONSTRUCCION_CTM12", "R_TERRENO_CTM12"),
                    ("R_CONSTRUCCION_INFORMAL", "R_TERRENO_INFORMAL"),
                    ("R_TERRENO_INFORMAL", "R_TERRENO_CTM12")
                ],
                "must_cover_each_other":[
                    ("R_TERRENO_CTM12", "R_VEREDA_CTM12"),
                    ("R_UNIDAD_CTM12", "R_CONSTRUCCION_CTM12"),
                    ("R_UNIDAD_INFORMAL", "R_CONSTRUCCION_INFORMAL"),
                    ("R_ZONA_HOMO_GEOECONOMICA_CTM12", "R_ZONA_HOMOGENEA_FISICA_CTM12")
                ]
            },
            "URBANO_Topology": {
                "dataset": "URBANO",  
                "must_not_overlap": [
                    "U_TERRENO","U_MANZANA",
                    "U_SECTOR", "U_BARRIO", "U_PERIMETRO",
                    "U_CONSTRUCCION",
                    "U_ZONA_HOMOGENEA_FISICA",
                    "U_ZONA_HOMOGENEA_GEOECONOMICA",
            
                ],
                "must_not_have_gaps": [
                    "U_TERRENO","U_MANZANA",
                    "U_SECTOR", "U_BARRIO", "U_PERIMETRO",
                    "U_ZONA_HOMOGENEA_FISICA", "U_ZONA_HOMOGENEA_GEOECONOMICA"

                ],
                "must_be_covered_by": [
                    ("U_SECTOR", "U_PERIMETRO"),
                    ("U_BARRIO", "U_SECTOR"),
                    ("U_TERRENO", "U_MANZANA"),
                    ("U_CONSTRUCCION", "U_TERRENO"),


                ],
                "must_cover_each_other":[
                    ("U_TERRENO", "U_MANZANA"),
                    ("U_UNIDAD", "U_CONSTRUCCION"),
                    ("U_ZONA_HOMOGENEA_GEOECONOMICA", "U_ZONA_HOMOGENEA_FISICA")

                ]
            },
            "RURAL_Topology": {
                "dataset": "RURAL",
                "must_not_overlap": [
                    "R_TERRENO","R_CONSTRUCCION","R_VEREDA","R_SECTOR",
                    "R_ZONA_HOMOGENEA_FISICA","R_ZONA_HOMOGENEA_GEOECONOMICA"
                ],
                "must_not_have_gaps": [
                    "R_TERRENO","R_VEREDA","R_SECTOR",
                    "R_ZONA_HOMOGENEA_FISICA","R_ZONA_HOMOGENEA_GEOECONOMICA"
                ],
                "must_be_covered_by": [
                    ("R_VEREDA", "R_SECTOR"),
                    ("R_TERRENO", "R_VEREDA"),
                    ("R_CONSTRUCCION", "R_TERRENO"),

                ],
                "must_cover_each_other":[
                    ("R_TERRENO", "R_VEREDA"),
                    ("R_UNIDAD", "R_CONSTRUCCION"),
                    ("R_ZONA_HOMOGENEA_GEOECONOMICA", "R_ZONA_HOMOGENEA_FISICA")

                ]
            }
        }

    def get_project_paths(self):
        """
        Obtiene las rutas del proyecto basadas en la ubicación actual del script
        """
        # Obtener la ruta del script actual
        script_path = os.path.abspath(__file__)
        
        # Obtener la ruta raíz del proyecto (2 niveles arriba de Scripts/Modelo_IGAC)
        root_path = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
        
        # Construir la ruta base para los archivos temporales
        temp_path = os.path.join(root_path, 'Files', 'Temporary_Files', 'MODELO_IGAC', 'Topology_Errors')
        
        return root_path, temp_path

    def detect_rule_language(self, gdb_path):
        """
        Detecta el idioma de las reglas en la geodatabase
        """
        arcpy.env.workspace = gdb_path
        fc_list = arcpy.ListFeatureClasses("*_errors_*")
        
        if not fc_list:
            return 'english'  # default to English if no feature classes found
            
        # Tomar el primer feature class no vacío
        for fc in fc_list:
            if int(arcpy.GetCount_management(fc)[0]) > 0:
                with arcpy.da.SearchCursor(fc, ['RuleDescription']) as cursor:
                    for row in cursor:
                        if row[0]:
                            # Verificar si la regla está en español
                            if any(spanish_rule in row[0] for spanish_rule in self.rule_descriptions.values()):
                                print("Reglas detectadas en español")
                                return 'spanish'
                            else:
                                print("Reglas detectadas en inglés")
                                return 'english'
        
        return 'english'  # default to English if no rules found

    def normalize_rule_description(self, rule, language):
        """
        Normaliza la descripción de la regla al formato interno en inglés
        """
        if language == 'spanish':
            return self.rule_descriptions_inverse.get(rule, rule)
        return rule

    def find_matching_feature(self, feature_name, rule_desc, empty_position, dataset, language):
        """Encuentra el feature correspondiente"""
        dataset_topology = f"{dataset}_Topology"
        
        if dataset_topology not in self.TOPOLOGY_RULES:
            self.logger.warning(f"Dataset {dataset_topology} no encontrado")
            return None
        
        rule_desc = self.normalize_rule_description(rule_desc, language)
        
        rule_map = {
            'Must Not Have Gaps': 'must_not_have_gaps',
            'Must Not Overlap': 'must_not_overlap',
            'Must Be Covered By Feature Class Of': 'must_be_covered_by',
            'Must Cover Each Other': 'must_cover_each_other'
        }
        
        rule_key = rule_map.get(rule_desc)
        if not rule_key:
            self.logger.warning(f"Regla no encontrada: {rule_desc}")
            return None
            
        rules = self.TOPOLOGY_RULES[dataset_topology].get(rule_key, [])
        ##self.logger.info(f"Reglas disponibles: {rules}")
        
        # Para reglas simples (Must Not Have Gaps y Must Not Overlap)
        if rule_key in ['must_not_have_gaps', 'must_not_overlap']:
            # Si el feature_name está en la lista de reglas, es válido
            if feature_name in rules:
                # Para estas reglas, el feature que falta es el mismo
                return feature_name
        
        # Para reglas que involucran pares
        else:
            for pair in rules:
                if not isinstance(pair, tuple):
                    continue
                    
                first, second = pair
                ##self.logger.info(f"Comparando con par: {first}, {second}")
                
                if rule_key == "must_be_covered_by":
                    if empty_position == "destination" and first == feature_name:
                        return second
                    elif empty_position == "origin" and second == feature_name:
                        return first
                
                elif rule_key == "must_cover_each_other":
                    if first == feature_name:
                        return second
                    elif second == feature_name:
                        return first
        
        self.logger.warning("No se encontró coincidencia")
        return None

    def process_topology_errors(self):
        root_path, temp_path = self.get_project_paths()
        gdb_list = [f for f in os.listdir(temp_path) if f.endswith('.gdb')]
        
        if not gdb_list:
            print("No se encontró geodatabase")
            return
            
        gdb_path = os.path.join(temp_path, gdb_list[0])
        print(f"Procesando GDB: {gdb_path}")
        
        language = self.detect_rule_language(gdb_path)
        
        arcpy.env.workspace = gdb_path
        fc_list = [fc for fc in arcpy.ListFeatureClasses("*_errors_*") 
                  if fc.endswith('_line') or fc.endswith('_poly')]
        
        for fc in fc_list:
            dataset_match = re.match(r'(.+?)_errors_', fc)
            if not dataset_match:
                continue
                
            dataset = dataset_match.group(1)
            print(f"\nProcesando: {fc}")
            
            if int(arcpy.GetCount_management(fc)[0]) == 0:
                continue
                
            fields = ['OriginObjectClassName', 'DestinationObjectClassName', 'RuleDescription']
            updates_count = 0
            
            with arcpy.da.UpdateCursor(fc, fields) as cursor:
                for row in cursor:
                    origin, destination, rule = row
                    if not origin and destination:
                        known_feature = destination
                        empty_position = 'origin'
                    elif not destination and origin:
                        known_feature = origin
                        empty_position = 'destination'
                    else:
                        continue
                    
                    matching_feature = self.find_matching_feature(
                        known_feature, 
                        rule, 
                        empty_position,
                        dataset,
                        language
                    )
                    
                    if matching_feature:
                        new_row = (matching_feature, destination, rule) if empty_position == 'origin' else (origin, matching_feature, rule)
                        cursor.updateRow(new_row)
                        updates_count += 1
                        
            print(f"Total actualizaciones en {fc}: {updates_count}")

if __name__ == "__main__":
    processor = TopologyRuleProcessor()
    processor.process_topology_errors()