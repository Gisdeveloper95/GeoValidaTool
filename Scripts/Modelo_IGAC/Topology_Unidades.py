import arcpy
import os
from pathlib import Path
import logging
from datetime import datetime
from collections import defaultdict, Counter

def setup_logging():
    """Configura el sistema de logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger(__name__)

def find_project_root():
    """Encuentra la raíz del proyecto y las rutas necesarias"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
    
    required_paths = [
        os.path.join(project_root, "Files", "Temporary_Files", "MODELO_IGAC"),
        os.path.join(project_root, "Files", "Temporary_Files", "array_config.txt")
    ]
    
    if all(os.path.exists(path) for path in required_paths):
        return project_root
    raise Exception("No se encontró la estructura correcta del proyecto")

def find_geodatabases(root_path):
    """Encuentra las geodatabases de entrada y de errores topológicos"""
    temp_path = os.path.join(root_path, "Files", "Temporary_Files", "MODELO_IGAC")
    topology_path = os.path.join(temp_path, "Topology_Errors")
    
    # Buscar GDB original
    input_gdb = None
    for item in os.listdir(temp_path):
        if item.endswith('.gdb'):
            input_gdb = os.path.join(temp_path, item)
            break
    
    # Buscar GDB de errores topológicos
    topology_gdb = None
    if os.path.exists(topology_path):
        for item in os.listdir(topology_path):
            if item.endswith('.gdb'):
                topology_gdb = os.path.join(topology_path, item)
                break
    
    if not input_gdb or not topology_gdb:
        raise Exception("No se encontraron las geodatabases necesarias")
        
    return input_gdb, topology_gdb

def load_active_datasets():
    """Carga la configuración de datasets activos"""
    root_path = find_project_root()
    config_path = os.path.join(root_path, "Files", "Temporary_Files", "array_config.txt")
    
    with open(config_path, 'r') as f:
        datasets = []
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                dataset_name = line.strip('[]",').strip()
                if dataset_name:
                    datasets.append(dataset_name)
    return datasets


import arcpy
import os
from pathlib import Path
import logging
from datetime import datetime
from collections import defaultdict
import uuid

class UnitDuplicateDetector:
    def __init__(self, input_gdb, topology_gdb):
        self.input_gdb = input_gdb
        self.topology_gdb = topology_gdb
        self.logger = logging.getLogger(__name__)
        arcpy.env.workspace = input_gdb
        
        self.unit_layers = {
            "URBANO_CTM12": ["U_UNIDAD_CTM12", "U_UNIDAD_INFORMAL"],
            "RURAL_CTM12": ["R_UNIDAD_CTM12", "R_UNIDAD_INFORMAL"],
            "URBANO": ["U_UNIDAD"],
            "RURAL": ["R_UNIDAD"]
        }
        
        # PLANTA está incluida intencionalmente para asegurar la diferenciación
        self.compare_fields = ["CODIGO", "TIPO_CONSTRUCCION", "IDENTIFICADOR", "PLANTA"]
        self.area_tolerance = 0.005
        self.batch_size = 50

    def _are_areas_similar(self, area1, area2):
        if area1 == 0 or area2 == 0:
            return False
        difference = abs(area1 - area2) / max(area1, area2)
        return difference <= self.area_tolerance

    def _find_layer_duplicates(self, fc_path, layer_name):
        spatial_index = None
        for idx in arcpy.ListIndexes(fc_path):
            if idx.name == "SHAPE_IDX":
                spatial_index = idx
                break
        if not spatial_index:
            arcpy.AddSpatialIndex_management(fc_path)

        potential_duplicates = defaultdict(list)
        oid_lookup = {}
        
        with arcpy.da.SearchCursor(fc_path, self.compare_fields + ["SHAPE@AREA", "OID@", "SHAPE@", "SHAPE@XY"]) as cursor:
            for row in cursor:
                # Obtenemos todos los valores de atributos excepto área, OID, geometría y centroide
                attr_values = row[:-4]
                if any(val is None for val in attr_values):
                    continue
                
                # Creamos la clave base con todos los atributos incluyendo PLANTA
                base_key = tuple(str(val).strip() for val in attr_values)
                area = row[-4]
                oid = row[-3]
                geom = row[-2]
                
                oid_lookup[oid] = {
                    'geometry': geom,
                    'area': area,
                    'attributes': base_key
                }

                # La clave completa incluye área al final
                full_key = base_key + (str(area),)
                
                found_group = False
                for existing_key in list(potential_duplicates.keys()):
                    # Comparamos todos los atributos excepto el área
                    existing_base = existing_key[:-1]
                    if existing_base == base_key:  # Asegura que PLANTA sea igual
                        existing_area = float(existing_key[-1])
                        if self._are_areas_similar(area, existing_area):
                            potential_duplicates[existing_key].append(oid)
                            found_group = True
                            break
                
                if not found_group:
                    potential_duplicates[full_key].append(oid)

        # Filtrar grupos que tienen más de un elemento
        duplicate_groups = {k: v for k, v in potential_duplicates.items() if len(v) > 1}
        
        if not duplicate_groups:
            return []

        final_duplicates = []
        groups_list = list(duplicate_groups.items())
        
        for batch_start in range(0, len(groups_list), self.batch_size):
            batch_end = min(batch_start + self.batch_size, len(groups_list))
            batch = groups_list[batch_start:batch_end]
            
            temp_name = f"TEMP_{str(uuid.uuid4()).replace('-', '')[:8]}"
            temp_fc = os.path.join("in_memory", temp_name)
            
            try:
                arcpy.CreateFeatureclass_management(
                    "in_memory", temp_name, "POLYGON",
                    spatial_reference=arcpy.Describe(fc_path).spatialReference
                )
                arcpy.AddField_management(temp_fc, "GROUP_ID", "TEXT")
                arcpy.AddField_management(temp_fc, "ORIG_OID", "LONG")

                with arcpy.da.InsertCursor(temp_fc, ["SHAPE@", "GROUP_ID", "ORIG_OID"]) as insert_cursor:
                    for group_key, oids in batch:
                        for oid in oids:
                            feature = oid_lookup[oid]
                            insert_cursor.insertRow([feature['geometry'], str(group_key), oid])

                overlap_name = f"OVERLAP_{str(uuid.uuid4()).replace('-', '')[:8]}"
                overlap_fc = os.path.join("in_memory", overlap_name)
                
                arcpy.Intersect_analysis([temp_fc], overlap_fc, "ALL")

                processed_groups = set()
                with arcpy.da.SearchCursor(overlap_fc, ["GROUP_ID", "ORIG_OID", "SHAPE@AREA"]) as cursor:
                    for row in cursor:
                        group_key = eval(row[0])
                        if group_key in processed_groups:
                            continue
                            
                        oid = row[1]
                        overlap_area = row[2]
                        if oid not in oid_lookup:
                            continue
                        
                        original_area = oid_lookup[oid]['area']
                        if overlap_area > (original_area * 0.05):
                            oids_in_group = duplicate_groups[group_key]
                            # Solo registrar los duplicados que comparten exactamente los mismos atributos
                            for feature_oid in oids_in_group[1:]:
                                if oid_lookup[oid]['attributes'] == oid_lookup[feature_oid]['attributes']:
                                    final_duplicates.append({
                                        'layer': layer_name,
                                        'attributes': group_key[:-1],
                                        'oid': feature_oid,
                                        'geometry': oid_lookup[feature_oid]['geometry'],
                                        'original_oid': oid
                                    })
                            processed_groups.add(group_key)

            finally:
                if arcpy.Exists(temp_fc):
                    arcpy.Delete_management(temp_fc)
                if arcpy.Exists(overlap_fc):
                    arcpy.Delete_management(overlap_fc)
            final_duplicates = self._verify_final_duplicates(final_duplicates, fc_path)
        
        return final_duplicates

    def clean_in_memory(self):
        try:
            arcpy.Delete_management("in_memory")
        except Exception as e:
            self.logger.warning(f"Error limpiando memoria: {str(e)}")

    def find_duplicates(self, dataset_name):
        if dataset_name not in self.unit_layers:
            return None
            
        duplicates = []
        for unit_layer in self.unit_layers[dataset_name]:
            fc_path = os.path.join(self.input_gdb, dataset_name, unit_layer)
            if not arcpy.Exists(fc_path):
                continue
                
            layer_duplicates = self._find_layer_duplicates(fc_path, unit_layer)
            duplicates.extend(layer_duplicates)
            
        return duplicates

    def _verify_final_duplicates(self, duplicates, fc_path):
        """Verificación final estricta usando solo los campos que determinan un duplicado"""
        verified_duplicates = []
        
        fields_to_verify = self.compare_fields + ["SHAPE@", "OID@"]
        
        features_info = {}
        with arcpy.da.SearchCursor(fc_path, fields_to_verify) as cursor:
            for row in cursor:
                oid = row[-1]
                features_info[oid] = {
                    'CODIGO': str(row[0]).strip(),
                    'TIPO_CONSTRUCCION': str(row[1]).strip(),
                    'IDENTIFICADOR': str(row[2]).strip(),
                    'PLANTA': str(row[3]).strip(),
                    'geometry': row[-2]
                }
        
        for duplicate in duplicates:
            oid = duplicate['oid']
            original_oid = duplicate.get('original_oid')
            
            if oid not in features_info or original_oid not in features_info:
                continue
                
            dup_feature = features_info[oid]
            orig_feature = features_info[original_oid]
            
            # Verificar campos clave
            is_duplicate = (
                dup_feature['CODIGO'] == orig_feature['CODIGO'] and
                dup_feature['TIPO_CONSTRUCCION'] == orig_feature['TIPO_CONSTRUCCION'] and
                dup_feature['IDENTIFICADOR'] == orig_feature['IDENTIFICADOR'] and
                dup_feature['PLANTA'] == orig_feature['PLANTA']
            )
            
            # Verificar superposición geométrica usando equals o intersect
            if is_duplicate:
                # Verificamos si son iguales o si tienen intersección significativa
                geom1 = dup_feature['geometry']
                geom2 = orig_feature['geometry']
                
                # Calcular área de intersección
                intersection = geom1.intersect(geom2, 4)
                union = geom1.union(geom2)
                
                if union.area > 0:
                    overlap_ratio = intersection.area / union.area
                    if overlap_ratio > 0.95:  # 95% de superposición
                        verified_duplicates.append(duplicate)
        
        return verified_duplicates
class TopologyErrorRecorder:
    def __init__(self, topology_gdb):
        self.topology_gdb = topology_gdb
        self.logger = logging.getLogger(__name__)
        self.recorded_pairs = set()  # Para evitar duplicados en el registro
        
    def record_duplicates(self, dataset_name, duplicates):
        """Registra los duplicados encontrados en la capa de errores correspondiente"""
        if not duplicates:
            return
            
        error_fc = os.path.join(self.topology_gdb, f"{dataset_name}_errors_poly")
        if not arcpy.Exists(error_fc):
            self.logger.error(f"No se encontró la capa de errores para {dataset_name}")
            return
            
        fields = ["SHAPE@", "OriginObjectClassName", "OriginObjectID",
                 "DestinationObjectClassName", "DestinationObjectID",
                 "RuleType", "RuleDescription", "isException"]
        
        count = 0
        with arcpy.da.InsertCursor(error_fc, fields) as cursor:
            for duplicate in duplicates:
                # Crear una clave única para el par de objetos
                if 'related_oid' in duplicate:
                    pair_key = tuple(sorted([duplicate['oid'], duplicate['related_oid']]))
                else:
                    pair_key = (duplicate['oid'], duplicate['oid'])
                
                # Verificar si ya hemos registrado este par
                if pair_key not in self.recorded_pairs:
                    row = (
                        duplicate['geometry'],
                        duplicate['layer'],
                        duplicate['oid'],
                        duplicate['layer'],
                        duplicate.get('related_oid', duplicate['oid']),  # Usar related_oid si existe
                        "esriTRTAreaNoOverlap",
                        "Must Not Overlap",
                        0
                    )
                    cursor.insertRow(row)
                    self.recorded_pairs.add(pair_key)
                    count += 1
        
        self.logger.info(f"Se registraron {count} duplicados únicos en {dataset_name}")  
def main():
    try:
        logger = setup_logging()
        logger.info("Iniciando detección de duplicados en unidades...")
        
        # Encontrar rutas necesarias
        project_root = find_project_root()
        input_gdb, topology_gdb = find_geodatabases(project_root)
        
        # Cargar datasets activos
        active_datasets = load_active_datasets()
        logger.info(f"Datasets activos: {active_datasets}")
        
        # Crear detectores
        duplicate_detector = UnitDuplicateDetector(input_gdb, topology_gdb)
        error_recorder = TopologyErrorRecorder(topology_gdb)
        
        # Limpiar memoria antes de comenzar
        duplicate_detector.clean_in_memory()
        
        # Procesar cada dataset
        for dataset in active_datasets:
            if dataset in ["URBANO_CTM12", "RURAL_CTM12", "URBANO", "RURAL"]:
                logger.info(f"\nProcesando dataset: {dataset}")
                
                # Encontrar duplicados
                duplicates = duplicate_detector.find_duplicates(dataset)
                if duplicates:
                    # Registrar en capa de errores
                    error_recorder.record_duplicates(dataset, duplicates)
                else:
                    logger.info(f"No se encontraron duplicados en {dataset}")
                
                # Limpiar memoria después de cada dataset
                duplicate_detector.clean_in_memory()
                    
        logger.info("\nProceso completado exitosamente")
        
    except Exception as e:
        logger.error(f"Error en el proceso: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
    finally:
        # Asegurar limpieza final de memoria
        try:
            arcpy.Delete_management("in_memory")
        except:
            pass

if __name__ == "__main__":
    main()