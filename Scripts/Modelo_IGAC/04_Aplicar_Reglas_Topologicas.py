import arcpy
import os
from datetime import datetime
import concurrent.futures
import time
import json
import re
import sys
sys.stdout.reconfigure(encoding='utf-8')
def log_message(message):
    """
    Imprime un mensaje con marca de tiempo
    """
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


# Carga inicial de DATASETS_TO_PROCESS desde archivo

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
    config_path = os.path.join(proyecto_dir, "Files", "Temporary_Files","array_config.txt")
    
    # Leer el archivo y filtrar los datasets activos
    active_datasets = []
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()  # Eliminar espacios en blanco alrededor
            # Agregar solo líneas no comentadas ni vacías
            if line and not line.startswith('#'):
                dataset_name = line.strip('",')  # Limpiar comillas y comas
                if dataset_name:  # Solo agregar si no está vacío
                    active_datasets.append(dataset_name)

    # Crear el diccionario con las topologías
    DATASETS_TO_PROCESS = {
    "topology": active_datasets,
    "line_topology": [ds for ds in active_datasets if ds in ["URBANO_CTM12", "RURAL_CTM12", "URBANO", "RURAL"]]
    }

    # Mostrar la configuración cargada
    print("\nConfiguración de datasets cargada:")
    print("--------------------------------")
    print("Datasets para topology:")
    for ds in DATASETS_TO_PROCESS["topology"]:
        print(f"  - {ds}")
    print("\nDatasets para line_topology:")
    for ds in DATASETS_TO_PROCESS["line_topology"]:
        if ds:  # Asegurarse de que no esté vacío
            print(f"  - {ds}")
    print("--------------------------------\n")

except Exception as e:
    print(f"Error al cargar configuración: {str(e)}")
    # Configuración por defecto en caso de error
    DATASETS_TO_PROCESS = {
        "topology": ["URBANO_CTM12", "RURAL_CTM12"],
        "line_topology": ["URBANO_CTM12", "RURAL_CTM12"]
    }
    print("\nUsando configuración por defecto:")
    print("--------------------------------")
    print("Datasets para topology:")
    for ds in DATASETS_TO_PROCESS["topology"]:
        print(f"  - {ds}")
    print("\nDatasets para line_topology:")
    for ds in DATASETS_TO_PROCESS["line_topology"]:
        print(f"  - {ds}")
    print("--------------------------------\n")




# Definición de reglas por topología con los datasets correctos
TOPOLOGY_RULES = {
    "URBANO_CTM12_Topology": {
        "dataset": "URBANO_CTM12",  
        "must_not_overlap": [
            "U_TERRENO_CTM12", "U_TERRENO_INFORMAL", "U_MANZANA_CTM12",
            "U_SECTOR_CTM12", "U_BARRIO_CTM12", "U_PERIMETRO_CTM12",
            "U_CONSTRUCCION_CTM12", "U_CONSTRUCCION_INFORMAL",
            "U_ZONA_HOMOGENEA_FISICA_CTM12",
            "U_ZONA_HOMO_GEOECONOMICA_CTM12",
    
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
            ("U_TERRENO_INFORMAL","U_TERRENO_CTM12"),


        ],
        "must_cover_each_other":[
            ("U_TERRENO_CTM12", "U_MANZANA_CTM12"),
            ("U_UNIDAD_CTM12", "U_CONSTRUCCION_CTM12"),
            ("U_UNIDAD_INFORMAL", "U_CONSTRUCCION_INFORMAL"),
            ("U_ZONA_HOMO_GEOECONOMICA_CTM12", "U_ZONA_HOMOGENEA_FISICA_CTM12")

        ]
        
    },
    "RURAL_TOPOLOGY": {
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
    },
    "RURAL_CTM12_Topology": {
        "dataset": "RURAL_CTM12",  
        "must_not_overlap": [
            "R_TERRENO_CTM12", "R_TERRENO_INFORMAL",
            "R_CONSTRUCCION_CTM12", "R_CONSTRUCCION_INFORMAL",
            "R_VEREDA_CTM12","R_SECTOR_CTM12",
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
            ("R_TERRENO_INFORMAL", "R_TERRENO_CTM12"),
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
    "ZONA_HOMOGENEA_URBANO_Topology": {
        "dataset": "ZONA_HOMOGENEA_URBANO",  
        "must_not_overlap": [
            "U_ZONA_HOMOGENEA_FISICA", "U_ZONA_HOMOGENEA_GEOECONOMICA"
           
        ],
        "must_not_have_gaps":[
            "U_ZONA_HOMOGENEA_FISICA", "U_ZONA_HOMOGENEA_GEOECONOMICA"
        ],
        "must_cover_each_other": [
            ("U_ZONA_HOMOGENEA_FISICA", "U_ZONA_HOMOGENEA_GEOECONOMICA")
        ]
    },
    "ZONA_HOMOGENEA_RURAL_CTM12_Topology": {
        "dataset": "ZONA_HOMOGENEA_RURAL_CTM12",  
        "must_not_overlap": [
            "R_ZONA_HOMO_GEOECONOMICA_CTM12",
            "R_ZONA_HOMOGENEA_FISICA_CTM12"
        ],
        "must_not_have_gaps": [
            "R_ZONA_HOMOGENEA_FISICA_CTM12",
            "R_ZONA_HOMO_GEOECONOMICA_CTM12"
        ],
        "must_cover_each_other": [
            ("R_ZONA_HOMOGENEA_FISICA_CTM12", "R_ZONA_HOMO_GEOECONOMICA_CTM12")
        ]
    },
    "ZONA_HOMOGENEA_URBANO_CTM12_Topology": {
        "dataset": "ZONA_HOMOGENEA_URBANO_CTM12",  
        "must_not_overlap": [
            "U_ZONA_HOMO_GEOECONOMICA_CTM12",
            "U_ZONA_HOMOGENEA_FISICA_CTM12"
        ],
        "must_not_have_gaps": [
            "U_ZONA_HOMOGENEA_FISICA_CTM12",
            "U_ZONA_HOMO_GEOECONOMICA_CTM12"
        ],
        "must_cover_each_other": [
            ("U_ZONA_HOMOGENEA_FISICA_CTM12", "U_ZONA_HOMO_GEOECONOMICA_CTM12")
        ]
    },
    "ZONA_HOMOGENEA_RURAL_Topology": {
        "dataset": "ZONA_HOMOGENEA_RURAL",  
        "must_not_overlap": [
            "R_ZONA_HOMOGENEA_FISICA", "R_ZONA_HOMOGENEA_GEOECONOMICA"
           
        ],
        "must_not_have_gaps":[
            "R_ZONA_HOMOGENEA_FISICA", "R_ZONA_HOMOGENEA_GEOECONOMICA"
        ],
        "must_cover_each_other": [
            ("R_ZONA_HOMOGENEA_FISICA", "R_ZONA_HOMOGENEA_GEOECONOMICA")
        ]
    }
}

# Nuevo diccionario para reglas de topología de líneas
LINE_TOPOLOGY_RULES = {
    "URBANO_CTM12_Topology": {
        "dataset": "URBANO_CTM12",
        "features": [
            {
                "name": "U_NOMEN_DOMICILIARIA_CTM12",
                "rules": ["must_not_overlap"]
            }

        ]
    },
    "RURAL_CTM12_Topology": {
        "dataset": "RURAL_CTM12",
        "features": [
            {
                "name": "R_NOMEN_DOMICILIARIA_CTM12",
                "rules": ["must_not_overlap"]
            }

        ]
    },
    "URBANO_Topology": {
        "dataset": "URBANO",
        "features": [
            {
                "name": "U_NOMENCLATURA_DOMICILIARIA",
                "rules": ["must_not_overlap"]
            }

        ]
    },
    "RURAL_Topology": {
        "dataset": "RURAL",
        "features": [
            {
                "name": "R_NOMENCLATURA_DOMICILIARIA",
                "rules": ["must_not_overlap"]
            }

        ]
    }
}


def filter_topology_rules(rules_dict, active_datasets):
    """
    Filtra el diccionario de reglas para incluir solo los datasets activos
    """
    return {
        topology_name: rules
        for topology_name, rules in rules_dict.items()
        if rules.get("dataset") in active_datasets
    }


def remove_all_topology_rules(topology_path):
    """
    Elimina todas las reglas de una topología
    """
    try:
        # Obtener todas las reglas existentes
        rules = arcpy.ListTopologyRules_management(topology_path)
        
        # Eliminar cada regla
        for rule in rules:
            try:
                arcpy.RemoveRuleFromTopology_management(topology_path, rule[0], rule[1], rule[2])
            except Exception as e:
                log_message(f"Error eliminando regla {rule}: {str(e)}")
                continue
    except Exception as e:
        log_message(f"Error eliminando reglas de topología: {str(e)}")

def apply_line_topology_rules(topology_info):
    try:
        topology_path = topology_info["topology_path"]
        topology_name = topology_info["topology_name"]
        
        if topology_name not in LINE_TOPOLOGY_RULES:
            return f"No se encontraron reglas de línea para {topology_name}"
            
        log_message(f"Aplicando reglas de línea a {topology_name}")
        rules = LINE_TOPOLOGY_RULES[topology_name]
        
        for feature in rules["features"]:
            feature_name = feature["name"]
            feature_path = os.path.join(topology_info["dataset_path"], feature_name)
            
            if not arcpy.Exists(feature_path):
                log_message(f"  Feature de línea no encontrado: {feature_name}")
                continue
            
            # Verificar tipo de geometría
            desc = arcpy.Describe(feature_path)
            if desc.shapeType != "Polyline":
                log_message(f"  {feature_name} no es una línea. Es: {desc.shapeType}")
                continue
            
            try:
                # Intentar añadir la feature class a la topología
                arcpy.AddFeatureClassToTopology_management(topology_path, feature_path)
                log_message(f"  Feature class {feature_name} añadida a la topología")
            except Exception as e:
                # Si hay un error porque ya existe, continuamos con las reglas
                if "ERROR 160032" in str(e):  # El error cuando la clase ya es miembro
                    log_message(f"  Feature class {feature_name} ya existe en la topología")
                else:
                    log_message(f"  Error añadiendo feature class {feature_name}: {str(e)}")
                    continue
                
            for rule in feature["rules"]:
                try:
                    if rule == "must_not_overlap":
                        # Intentar eliminar la regla si ya existe
                        try:
                            arcpy.RemoveRuleFromTopology_management(
                                topology_path,
                                "Must Not Self-Overlap (Line)",
                                feature_path
                            )
                        except:
                            pass  # Si la regla no existe, ignoramos el error
                            
                        log_message(f"  Añadiendo regla Must Not Self-Overlap para {feature_name}")
                        arcpy.AddRuleToTopology_management(
                            topology_path,
                            "Must Not Self-Overlap (Line)",
                            feature_path
                        )
                        
                        # Añadir también la regla Must Not Self-Intersect
                        try:
                            arcpy.RemoveRuleFromTopology_management(
                                topology_path,
                                "Must Not Self-Intersect (Line)",
                                feature_path
                            )
                        except:
                            pass
                            
                        log_message(f"  Añadiendo regla Must Not Self-Intersect para {feature_name}")
                        arcpy.AddRuleToTopology_management(
                            topology_path,
                            "Must Not Self-Intersect (Line)",
                            feature_path
                        )
                except Exception as e:
                    log_message(f"    Error aplicando regla {rule} para {feature_name}: {str(e)}")
                    
        # Validar la topología al final
        try:
            arcpy.ValidateTopology_management(topology_path, "Full_Extent")
            log_message(f"  Topología validada para {topology_name}")
        except Exception as e:
            log_message(f"  Error validando topología para {topology_name}: {str(e)}")
            
        return f"Reglas de línea aplicadas exitosamente a {topology_name}"
        
    except Exception as e:
        error_msg = f"Error aplicando reglas de línea a {topology_name}: {str(e)}"
        log_message(error_msg)
        return error_msg
def validate_and_add_manzana_rule(gdb_path, dataset_name):
    """
    Valida la existencia y contenido de feature classes específicos y 
    determina qué regla topológica debe agregarse para U_MANZANA_CTM12 o U_MANZANA
    
    Args:
        gdb_path (str): Ruta a la geodatabase
        dataset_name (str): Nombre del dataset (URBANO_CTM12 o URBANO)
        
    Returns:
        tuple: (regla_a_agregar, mensaje_log)
    """
    try:
        # Determinar los nombres de features según el dataset
        if dataset_name == "URBANO_CTM12":
            manzana_feature = "U_MANZANA_CTM12"
            barrio_feature = "U_BARRIO_CTM12"
            sector_feature = "U_SECTOR_CTM12"
        elif dataset_name == "URBANO":
            manzana_feature = "U_MANZANA"
            barrio_feature = "U_BARRIO"
            sector_feature = "U_SECTOR"
        else:
            return (None, f"Dataset no soportado: {dataset_name}")

        # Construir rutas completas
        barrio_path = os.path.join(gdb_path, dataset_name, barrio_feature)
        sector_path = os.path.join(gdb_path, dataset_name, sector_feature)
        
        # Verificar y contar registros en U_BARRIO
        if arcpy.Exists(barrio_path):
            try:
                count_barrio = int(arcpy.GetCount_management(barrio_path)[0])
            except:
                count_barrio = 0
                log_message(f"No se pudo obtener el conteo de {barrio_feature}")
            
            if count_barrio > 0:
                return ((manzana_feature, barrio_feature), 
                        f"Se agregó regla: {manzana_feature} must be covered by {barrio_feature} (tiene {count_barrio} registros)")
            else:
                # Si U_BARRIO no tiene registros, verificar U_SECTOR
                if arcpy.Exists(sector_path):
                    try:
                        count_sector = int(arcpy.GetCount_management(sector_path)[0])
                    except:
                        count_sector = 0
                        log_message(f"No se pudo obtener el conteo de {sector_feature}")
                    
                    if count_sector > 0:
                        return ((manzana_feature, sector_feature), 
                                f"Se agregó regla: {manzana_feature} must be covered by {sector_feature} ({barrio_feature} sin registros)")
                    else:
                        return ((manzana_feature, barrio_feature), 
                                f"Se agregó regla: {manzana_feature} must be covered by {barrio_feature} (sin registros, {sector_feature} también sin registros)")
        else:
            # Si U_BARRIO no existe, verificar U_SECTOR
            if arcpy.Exists(sector_path):
                return ((manzana_feature, sector_feature), 
                        f"Se agregó regla: {manzana_feature} must be covered by {sector_feature} ({barrio_feature} no existe)")
        
        # Si ninguna de las condiciones anteriores se cumple
        return (None, f"No se agregó ninguna regla para {manzana_feature} - No se cumplieron las condiciones")
    
    except Exception as e:
        return (None, f"Error al validar reglas para {dataset_name}: {str(e)}")

def repair_geometries(gdb_path, dataset_name):
    """
    Repara las geometrías de todos los feature classes en un dataset
    """
    try:
        workspace = os.path.join(gdb_path, dataset_name)
        arcpy.env.workspace = workspace
        
        # Obtener lista de feature classes en el dataset
        feature_classes = arcpy.ListFeatureClasses()
        
        log_message(f"Iniciando reparación de geometrías para dataset {dataset_name}")
        
        for fc in feature_classes:
            try:
                fc_path = os.path.join(workspace, fc)
                log_message(f"  Reparando geometría de {fc}")
                
                # Crear una copia temporal
                temp_fc = f"TEMP_{fc}"
                arcpy.CopyFeatures_management(fc, temp_fc)
                
                # Reparar geometrías
                arcpy.RepairGeometry_management(temp_fc, "DELETE_NULL")
                
                # Sobrescribir el original con la versión reparada
                arcpy.Delete_management(fc)
                arcpy.CopyFeatures_management(temp_fc, fc)
                arcpy.Delete_management(temp_fc)
                
                log_message(f"  Geometría reparada para {fc}")
                
            except Exception as e:
                log_message(f"  Error reparando {fc}: {str(e)}")
                continue
                
        return True
        
    except Exception as e:
        log_message(f"Error en reparación de geometrías: {str(e)}")
        return False

def validate_topology_in_parts(topology_path, rules):
    """
    Valida la topología por partes para reducir la carga de procesamiento
    """
    try:
        # Establecer entorno de procesamiento
        arcpy.env.XYResolution = "0.0001 Meters"
        arcpy.env.XYTolerance = "0.001 Meters"
        
        # Validar por grupos de reglas
        rule_groups = []
        current_group = []
        
        for rule in rules:
            current_group.append(rule)
            if len(current_group) >= 3:  # Procesar en grupos de 3 reglas
                rule_groups.append(current_group)
                current_group = []
        
        if current_group:  # Añadir el último grupo si existe
            rule_groups.append(current_group)
        
        # Validar cada grupo
        for group in rule_groups:
            try:
                log_message(f"  Validando grupo de reglas...")
                arcpy.ValidateTopology_management(topology_path, "Full_Extent")
                time.sleep(2)  # Pequeña pausa entre grupos
            except Exception as e:
                log_message(f"  Error validando grupo de reglas: {str(e)}")
                continue
        
        return True
        
    except Exception as e:
        log_message(f"Error en validación por partes: {str(e)}")
        return False


def apply_topology_rules(topology_info):
    """
    Aplica las reglas topológicas a una topología específica
    """
    try:
        topology_path = topology_info["topology_path"]
        topology_name = topology_info["topology_name"]
        
        if topology_name not in TOPOLOGY_RULES:
            return f"No se encontraron reglas para {topology_name}"
            
        log_message(f"Aplicando reglas a {topology_name}")
        rules = TOPOLOGY_RULES[topology_name]
        
        # Establecer entorno de procesamiento
        arcpy.env.XYResolution = "0.0001 Meters"
        arcpy.env.XYTolerance = "0.001 Meters"
        
        # Aplicar Must Not Overlap
        if "must_not_overlap" in rules:
            for feature_class in rules["must_not_overlap"]:
                try:
                    feature_path = os.path.join(topology_info["dataset_path"], feature_class)
                    if not arcpy.Exists(feature_path):
                        log_message(f"  Feature class no encontrado: {feature_class}")
                        continue
                        
                    log_message(f"  Añadiendo regla Must Not Overlap para {feature_class}")
                    arcpy.AddRuleToTopology_management(
                        topology_path,
                        "Must Not Overlap (Area)",
                        feature_path
                    )
                except Exception as e:
                    log_message(f"    Error en Must Not Overlap para {feature_class}: {str(e)}")
                    continue
        
        # Aplicar Must Not Have Gaps
        if "must_not_have_gaps" in rules:
            for feature_class in rules["must_not_have_gaps"]:
                try:
                    feature_path = os.path.join(topology_info["dataset_path"], feature_class)
                    if not arcpy.Exists(feature_path):
                        log_message(f"  Feature class no encontrado: {feature_class}")
                        continue
                        
                    log_message(f"  Añadiendo regla Must Not Have Gaps para {feature_class}")
                    arcpy.AddRuleToTopology_management(
                        topology_path,
                        "Must Not Have Gaps (Area)",
                        feature_path
                    )
                except Exception as e:
                    log_message(f"    Error en Must Not Have Gaps para {feature_class}: {str(e)}")
                    continue
        
        # Aplicar Must Be Covered By
        if "must_be_covered_by" in rules:
            for feature_class, destination_class in rules["must_be_covered_by"]:
                try:
                    feature_path = os.path.join(topology_info["dataset_path"], feature_class)
                    destination_path = os.path.join(topology_info["dataset_path"], destination_class)
                    
                    if not arcpy.Exists(feature_path) or not arcpy.Exists(destination_path):
                        log_message(f"  Feature class no encontrado para Must Be Covered By: {feature_class} o {destination_class}")
                        continue
                        
                    log_message(f"  Añadiendo regla Must Be Covered By entre {feature_class} y {destination_class}")
                    arcpy.AddRuleToTopology_management(
                        topology_path,
                        "Must Be Covered By Feature Class Of (Area-Area)",
                        feature_path,
                        "",
                        destination_path
                    )
                except Exception as e:
                    log_message(f"    Error en Must Be Covered By para {feature_class}: {str(e)}")
                    continue
        
        # Aplicar Must Cover Each Other
        if "must_cover_each_other" in rules:
            for feature_class, destination_class in rules["must_cover_each_other"]:
                try:
                    feature_path = os.path.join(topology_info["dataset_path"], feature_class)
                    destination_path = os.path.join(topology_info["dataset_path"], destination_class)
                    
                    if not arcpy.Exists(feature_path) or not arcpy.Exists(destination_path):
                        log_message(f"  Feature class no encontrado para Must Cover Each Other: {feature_class} o {destination_class}")
                        continue
                        
                    log_message(f"  Añadiendo regla Must Cover Each Other entre {feature_class} y {destination_class}")
                    arcpy.AddRuleToTopology_management(
                        topology_path,
                        "Must Cover Each Other (Area-Area)",
                        feature_path,
                        "",
                        destination_path
                    )
                except Exception as e:
                    log_message(f"    Error en Must Cover Each Other para {feature_class}: {str(e)}")
                    continue
        
        # Validar la topología con reintentos
        log_message(f"  Validando topología {topology_name}")
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                arcpy.ValidateTopology_management(topology_path, "Full_Extent")
                break
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    log_message(f"    Error en la validación después de {max_retries} intentos: {str(e)}")
                else:
                    log_message(f"    Reintento {retry_count} de validación...")
                    time.sleep(2)  # Esperar 2 segundos antes de reintentar
        
        return f"Reglas aplicadas exitosamente a {topology_name}"
        
    except Exception as e:
        error_msg = f"Error aplicando reglas a {topology_name}: {str(e)}"
        log_message(error_msg)
        return error_msg
def find_geodatabase(temp_dir):
    """
    Busca y retorna la ruta de la geodatabase en el directorio especificado
    """
    for file in os.listdir(temp_dir):
        if file.endswith(".gdb"):
            return os.path.join(temp_dir, file)
    return None

def get_topology_path(gdb_path, dataset_name, topology_name):
    """
    Construye y verifica la ruta completa a la topología
    """
    topology_path = os.path.join(gdb_path, dataset_name, topology_name)
    if not arcpy.Exists(topology_path):
        raise Exception(f"No se encontró la topología: {topology_path}")
    return topology_path


def process_topologies():
    try:
        start_time = time.time()
        log_message("Iniciando proceso de gestión de topologías...")
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
        temp_dir = os.path.join(proyecto_dir, "Files", "Temporary_Files","MODELO_IGAC")
        
        gdb_path = find_geodatabase(temp_dir)
        if not gdb_path:
            raise Exception("No se encontró ninguna geodatabase en el directorio temporal.")
        
        active_topology_rules = filter_topology_rules(TOPOLOGY_RULES, DATASETS_TO_PROCESS["topology"])
        active_line_topology_rules = filter_topology_rules(LINE_TOPOLOGY_RULES, DATASETS_TO_PROCESS["line_topology"])

        arcpy.env.workspace = gdb_path
        arcpy.env.overwriteOutput = True
        
        # Procesar topologías de línea primero
        log_message("Procesando reglas de topología de línea...")
        for topology_name, rules in active_line_topology_rules.items():
            dataset_name = rules.get("dataset")
            if not dataset_name:
                continue
                
            try:
                dataset_path = os.path.join(gdb_path, dataset_name)
                topology_path = get_topology_path(gdb_path, dataset_name, topology_name)
                
                topology_info = {
                    "topology_name": topology_name,
                    "topology_path": topology_path,
                    "dataset_path": dataset_path
                }
                
                # Limpiar topología existente antes de agregar nuevas reglas
                arcpy.RemoveAllRulesFromTopology_management(topology_path)
                
                result = apply_line_topology_rules(topology_info)
                log_message(result)
                
            except Exception as e:
                log_message(f"Error procesando reglas de línea para {topology_name}: {str(e)}")

        # Luego procesar las topologías regulares
        log_message("Procesando reglas de topología principales...")
        for topology_name, rules in active_topology_rules.items():
            dataset_name = rules.get("dataset")
            if not dataset_name:
                log_message(f"Dataset no especificado para {topology_name}")
                continue
                
            try:
                dataset_path = os.path.join(gdb_path, dataset_name)
                topology_path = get_topology_path(gdb_path, dataset_name, topology_name)
                
                topology_info = {
                    "topology_name": topology_name,
                    "topology_path": topology_path,
                    "dataset_path": dataset_path
                }
                
                result = apply_topology_rules(topology_info)
                log_message(result)
                
            except Exception as e:
                log_message(f"Error procesando {topology_name}: {str(e)}")

        # Procesar las reglas de línea
        log_message("Procesando reglas de topología de línea...")
        for topology_name, rules in active_line_topology_rules.items():
            dataset_name = rules.get("dataset")
            if not dataset_name:
                continue
                
            try:
                dataset_path = os.path.join(gdb_path, dataset_name)
                topology_path = get_topology_path(gdb_path, dataset_name, topology_name)
                
                topology_info = {
                    "topology_name": topology_name,
                    "topology_path": topology_path,
                    "dataset_path": dataset_path
                }
                
                result = apply_line_topology_rules(topology_info)
                log_message(result)
                
            except Exception as e:
                log_message(f"Error procesando reglas de línea para {topology_name}: {str(e)}")
        
        end_time = time.time()
        total_time = end_time - start_time
        log_message(f"\nProceso completado en {total_time:.2f} segundos")
        
    except Exception as e:
        log_message(f"ERROR GENERAL: {str(e)}")


if __name__ == "__main__":
    process_topologies()