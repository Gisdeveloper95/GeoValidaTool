import arcpy
import os
from pathlib import Path
import uuid
import json

import sys
sys.stdout.reconfigure(encoding='utf-8')
def get_relative_path():
    """
    Encuentra la raíz del proyecto verificando la estructura de directorios esperada.
    """
    # Empezar desde el directorio del script actual
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    
    # Subir dos niveles para llegar a la raíz del proyecto (desde Modelo_IGAC/script.py hasta GeoValidaTool)
    ruta_proyecto = os.path.abspath(os.path.join(ruta_actual, '..', '..'))
    
    # Verifica la estructura característica del proyecto
    rutas_requeridas = [
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC"),
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "array_config.txt"),
        os.path.join(ruta_proyecto, "Scripts")
    ]
    
    # Para debug, imprimir las rutas que está verificando
    arcpy.AddMessage("\nVerificando rutas del proyecto:")
    for ruta in rutas_requeridas:
        existe = os.path.exists(ruta)
        arcpy.AddMessage(f"Ruta: {ruta}")
        arcpy.AddMessage(f"¿Existe?: {existe}")
    
    if all(os.path.exists(ruta) for ruta in rutas_requeridas):
        arcpy.AddMessage(f"\nRaíz del proyecto encontrada en: {ruta_proyecto}")
        
        # Crear directorios necesarios si no existen
        dirs_to_create = [
            os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "Topology_Errors"),
            os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "03_INCONSISTENCIAS", "CONSISTENCIA_TOPOLOGICA")
        ]
        
        for directory in dirs_to_create:
            if not os.path.exists(directory):
                os.makedirs(directory)
                arcpy.AddMessage(f"Directorio creado: {directory}")
        
        return ruta_proyecto
    
    raise ValueError(
        "No se encontró la raíz del proyecto.\n"
        "Verifique que está ejecutando el script desde dentro del proyecto "
        "y que existe la siguiente estructura:\n"
        "- Files/Temporary_Files/MODELO_IGAC\n"
        "- Files/Temporary_Files/array_config.txt\n"
        "- Files/Temporary_Files/MODELO_IGAC/Topology_Errors\n"
        "- Files/Temporary_Files/MODELO_IGAC/03_INCONSISTENCIAS/CONSISTENCIA_TOPOLOGICA"
    )

def create_output_directory(base_path, group_name):
    """
    Crea el directorio de salida para un grupo específico
    """
    output_dir = os.path.join(
        base_path,
        "Files",
        "Temporary_Files",
        "MODELO_IGAC",
        "03_INCONSISTENCIAS",
        "CONSISTENCIA_TOPOLOGICA",
        group_name
    )
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        arcpy.AddMessage(f"Directorio creado para grupo {group_name}: {output_dir}")
    return output_dir

def get_group_name(fc_name):
    parts = fc_name.split('_errors_')
    return parts[0] if len(parts) > 1 else None

def get_rule_mapping():
    """
    Retorna un diccionario con el mapeo de reglas inglés/español
    """
    return {
        'Must Not Have Gaps': 'No debe tener espacios',
        'Must Not Overlap': 'No debe superponerse',
        'Must Be Covered By Feature Class Of': 'Debe ser cubierto por la clase de entidad de',
        'Must Cover Each Other': 'Deben cubrirse entre ellos'
    }

def get_rule_conditions(group_name):
    # AQUÍ DEBES AGREGAR TUS CONDICIONES
    # Ejemplo del formato:
    conditions = conditions = {
        "URBANO_CTM12" : {
            ("U_TERRENO_CTM12", "U_TERRENO_CTM12", "Must Not Have Gaps"):"01_U_TERRENO_CTM12_must_not_have_gaps",
            ("U_TERRENO_INFORMAL", "U_TERRENO_INFORMAL", "Must Not Have Gaps"):"02_U_TERRENO_INFORMAL_must_not_have_gaps",
            ("U_MANZANA_CTM12", "U_MANZANA_CTM12", "Must Not Have Gaps"):"03_U_MANZANA_CTM12_must_not_have_gaps",
            ("U_SECTOR_CTM12", "U_SECTOR_CTM12", "Must Not Have Gaps"):"04_U_SECTOR_CTM12_must_not_have_gaps",
            ("U_BARRIO_CTM12", "U_BARRIO_CTM12", "Must Not Have Gaps"):"05_U_BARRIO_CTM12_must_not_have_gaps",
            ("U_PERIMETRO_CTM12", "U_PERIMETRO_CTM12", "Must Not Have Gaps"):"06_U_PERIMETRO_CTM12_must_not_have_gaps",
            ("U_ZONA_HOMOGENEA_FISICA_CTM12", "U_ZONA_HOMOGENEA_FISICA_CTM12", "Must Not Have Gaps"):"07_U_ZONA_HOMOGENEA_FISICA_CTM12_must_not_have_gaps",
            ("U_ZONA_HOMO_GEOECONOMICA_CTM12", "U_ZONA_HOMO_GEOECONOMICA_CTM12", "Must Not Have Gaps"):"08_U_ZONA_HOMO_GEOECONOMICA_CTM12_must_not_have_gaps",
            ("U_TERRENO_CTM12", "U_TERRENO_CTM12", "Must Not Overlap"):"09_U_TERRENO_CTM12_must_not_overlap",
            ("U_TERRENO_INFORMAL", "U_TERRENO_INFORMAL", "Must Not Overlap"):"10_U_TERRENO_INFORMAL_must_not_overlap",
            ("U_MANZANA_CTM12", "U_MANZANA_CTM12", "Must Not Overlap"):"11_U_MANZANA_CTM12_must_not_overlap",
            ("U_SECTOR_CTM12", "U_SECTOR_CTM12", "Must Not Overlap"):"12_U_SECTOR_CTM12_must_not_overlap",
            ("U_BARRIO_CTM12", "U_BARRIO_CTM12", "Must Not Overlap"):"13_U_BARRIO_CTM12_must_not_overlap",
            ("U_PERIMETRO_CTM12", "U_PERIMETRO_CTM12", "Must Not Overlap"):"14_U_PERIMETRO_CTM12_must_not_overlap",
            ("U_CONSTRUCCION_CTM12", "U_CONSTRUCCION_CTM12", "Must Not Overlap"):"15_U_CONSTRUCCION_CTM12_must_not_overlap",
            ("U_CONSTRUCCION_INFORMAL", "U_CONSTRUCCION_INFORMAL", "Must Not Overlap"):"16_U_CONSTRUCCION_INFORMAL_must_not_overlap",
            ("U_UNIDAD_CTM12", "U_UNIDAD_CTM12", "Must Not Overlap"):"17_U_UNIDAD_CTM12_must_not_overlap",
            ("U_UNIDAD_INFORMAL", "U_UNIDAD_INFORMAL", "Must Not Overlap"):"18_U_UNIDAD_INFORMAL_must_not_overlap",
            ("U_NOMEN_DOMICILIARIA_CTM12", "U_NOMEN_DOMICILIARIA_CTM12", "Must Not Overlap"):"19_U_NOMEN_DOMICILIARIA_CTM12_must_not_overlap",
            ("U_ZONA_HOMOGENEA_FISICA_CTM12", "U_ZONA_HOMOGENEA_FISICA_CTM12", "Must Not Overlap"):"20_U_ZONA_HOMOGENEA_FISICA_CTM12_must_not_overlap",
            ("U_ZONA_HOMO_GEOECONOMICA_CTM12", "U_ZONA_HOMO_GEOECONOMICA_CTM12", "Must Not Overlap"):"21_U_ZONA_HOMO_GEOECONOMICA_CTM12_must_not_overlap",
            ("U_SECTOR_CTM12", "U_PERIMETRO_CTM12", "Must Be Covered By Feature Class Of"):"22_U_SECTOR_CTM12_must_be_covered_by_U_PERIMETRO_CTM12",
            ("U_BARRIO_CTM12", "U_SECTOR_CTM12", "Must Be Covered By Feature Class Of"):"23_U_BARRIO_CTM12_must_be_covered_by_U_SECTOR_CTM12",
            ("U_MANZANA_CTM12", "U_BARRIO_CTM12", "Must Be Covered By Feature Class Of"):"24_U_MANZANA_CTM12_must_be_covered_by_U_BARRIO_CTM12",
            ("U_MANZANA_CTM12", "U_SECTOR_CTM12", "Must Be Covered By Feature Class Of"):"24_U_MANZANA_CTM12_must_be_covered_by_U_SECTOR_CTM12",
            ("U_TERRENO_CTM12", "U_MANZANA_CTM12", "Must Be Covered By Feature Class Of"):"25_U_TERRENO_CTM12_must_be_covered_by_U_MANZANA_CTM12",
            ("U_CONSTRUCCION_CTM12", "U_TERRENO_CTM12", "Must Be Covered By Feature Class Of"):"26_U_CONSTRUCCION_CTM12_must_be_covered_by_U_TERRENO_CTM12",
            ("U_CONSTRUCCION_INFORMAL", "U_TERRENO_INFORMAL", "Must Be Covered By Feature Class Of"):"27_U_CONSTRUCCION_INFORMAL_must_be_covered_by_U_TERRENO_INFORMAL",
            ("U_TERRENO_INFORMAL", "U_TERRENO_CTM12", "Must Be Covered By Feature Class Of"):"28_U_TERRENO_INFORMAL_must_be_covered_by_U_TERRENO_CTM12",
            ("U_TERRENO_CTM12", "U_ZONA_HOMO_GEOECONOMICA_CTM12", "Must Be Covered By Feature Class Of"):"29_U_TERRENO_CTM12_must_be_covered_by_U_ZONA_HOMO_GEOECONOMICA_CTM12",
            ("U_TERRENO_CTM12", "U_ZONA_HOMOGENEA_FISICA_CTM12", "Must Be Covered By Feature Class Of"):"30_U_TERRENO_CTM12_must_be_covered_by_U_ZONA_HOMOGENEA_FISICA_CTM12",
            ("U_TERRENO_CTM12", "U_MANZANA_CTM12", "Must Cover Each Other"):"31_U_TERRENO_CTM12_must_cover_each_other_U_MANZANA_CTM12",
            ("U_MANZANA_CTM12", "U_TERRENO_CTM12", "Must Cover Each Other"): "31_U_TERRENO_CTM12_must_cover_each_other_U_MANZANA_CTM12",
            ("U_UNIDAD_CTM12", "U_CONSTRUCCION_CTM12", "Must Cover Each Other"):"32_U_UNIDAD_CTM12_must_cover_each_other_U_CONSTRUCCION_CTM12",
            ("U_UNIDAD_INFORMAL", "U_CONSTRUCCION_INFORMAL", "Must Cover Each Other"):"33_U_UNIDAD_INFORMAL_must_cover_each_other_U_CONSTRUCCION_INFORMAL",
            ("U_ZONA_HOMO_GEOECONOMICA_CTM12", "U_ZONA_HOMOGENEA_FISICA_CTM12", "Must Cover Each Other"):"34_U_ZONA_HOMO_GEOECONOMICA_CTM12_must_cover_each_other_U_ZONA_HOMOGENEA_FISICA_CTM12"
        },
       "RURAL_CTM12": {
            ("R_TERRENO_CTM12", "R_TERRENO_CTM12", "Must Not Have Gaps"):"01_R_TERRENO_CTM12_must_not_have_gaps",
            ("R_TERRENO_INFORMAL", "R_TERRENO_INFORMAL", "Must Not Have Gaps"):"02_R_TERRENO_INFORMAL_must_not_have_gaps",
            ("R_VEREDA_CTM12", "R_VEREDA_CTM12", "Must Not Have Gaps"):"03_R_VEREDA_CTM12_must_not_have_gaps",
            ("R_SECTOR_CTM12", "R_SECTOR_CTM12", "Must Not Have Gaps"):"04_R_SECTOR_CTM12_must_not_have_gaps",
            ("R_ZONA_HOMOGENEA_FISICA_CTM12", "R_ZONA_HOMOGENEA_FISICA_CTM12", "Must Not Have Gaps"):"05_R_ZONA_HOMOGENEA_FISICA_CTM12_must_not_have_gaps",
            ("R_ZONA_HOMO_GEOECONOMICA_CTM12", "R_ZONA_HOMO_GEOECONOMICA_CTM12", "Must Not Have Gaps"):"06_R_ZONA_HOMO_GEOECONOMICA_CTM12_must_not_have_gaps",
            ("R_TERRENO_CTM12", "R_TERRENO_CTM12", "Must Not Overlap"):"07_R_TERRENO_CTM12_must_not_overlap",
            ("R_TERRENO_INFORMAL", "R_TERRENO_INFORMAL", "Must Not Overlap"):"08_R_TERRENO_INFORMAL_must_not_overlap",
            ("R_VEREDA_CTM12", "R_VEREDA_CTM12", "Must Not Overlap"):"09_R_VEREDA_CTM12_must_not_overlap",
            ("R_SECTOR_CTM12", "R_SECTOR_CTM12", "Must Not Overlap"):"10_R_SECTOR_CTM12_must_not_overlap",
            ("R_CONSTRUCCION_CTM12", "R_CONSTRUCCION_CTM12", "Must Not Overlap"):"11_R_CONSTRUCCION_CTM12_must_not_overlap",
            ("R_CONSTRUCCION_INFORMAL", "R_CONSTRUCCION_INFORMAL", "Must Not Overlap"):"12_R_CONSTRUCCION_INFORMAL_must_not_overlap",
            ("R_UNIDAD_CTM12", "R_UNIDAD_CTM12", "Must Not Overlap"):"13_R_UNIDAD_CTM12_must_not_overlap",
            ("R_UNIDAD_INFORMAL", "R_UNIDAD_INFORMAL", "Must Not Overlap"):"14_R_UNIDAD_INFORMAL_must_not_overlap",
            ("R_NOMEN_DOMICILIARIA_CTM12", "R_NOMEN_DOMICILIARIA_CTM12", "Must Not Overlap"):"15_R_NOMEN_DOMICILIARIA_CTM12_must_not_overlap",
            ("R_ZONA_HOMOGENEA_FISICA_CTM12", "R_ZONA_HOMOGENEA_FISICA_CTM12", "Must Not Overlap"):"16_R_ZONA_HOMOGENEA_FISICA_CTM12_must_not_overlap",
            ("R_ZONA_HOMO_GEOECONOMICA_CTM12", "R_ZONA_HOMO_GEOECONOMICA_CTM12", "Must Not Overlap"):"17_R_ZONA_HOMO_GEOECONOMICA_CTM12_must_not_overlap",
            ("R_VEREDA_CTM12", "R_SECTOR_CTM12", "Must Be Covered By Feature Class Of"):"18_R_VEREDA_CTM12_must_be_covered_by_R_SECTOR_CTM12",
            ("R_TERRENO_CTM12", "R_VEREDA_CTM12", "Must Be Covered By Feature Class Of"):"19_R_TERRENO_CTM12_must_be_covered_by_R_VEREDA_CTM12",
            ("R_CONSTRUCCION_CTM12", "R_TERRENO_CTM12", "Must Be Covered By Feature Class Of"):"20_R_CONSTRUCCION_CTM12_must_be_covered_by_R_TERRENO_CTM12",
            ("R_CONSTRUCCION_INFORMAL", "R_TERRENO_INFORMAL", "Must Be Covered By Feature Class Of"):"21_R_CONSTRUCCION_INFORMAL_must_be_covered_by_R_TERRENO_INFORMAL",
            ("R_TERRENO_INFORMAL", "R_TERRENO_CTM12", "Must Be Covered By Feature Class Of"):"22_R_TERRENO_INFORMAL_must_be_covered_by_R_TERRENO_CTM12",
            ("R_TERRENO_CTM12", "R_ZONA_HOMO_GEOECONOMICA_CTM12", "Must Be Covered By Feature Class Of"):"23_R_TERRENO_CTM12_must_be_covered_by_R_ZONA_HOMO_GEOECONOMICA_CTM12",
            ("R_TERRENO_CTM12", "R_ZONA_HOMOGENEA_FISICA_CTM12", "Must Be Covered By Feature Class Of"):"24_R_TERRENO_CTM12_must_be_covered_by_R_ZONA_HOMOGENEA_FISICA_CTM12",
            ("R_TERRENO_CTM12", "R_VEREDA_CTM12", "Must Cover Each Other"):"25_R_TERRENO_CTM12_must_cover_each_other_R_VEREDA_CTM12",
            ("R_VEREDA_CTM12","R_TERRENO_CTM12", "Must Cover Each Other"):"25_R_TERRENO_CTM12_must_cover_each_other_R_VEREDA_CTM12",
            ("R_UNIDAD_CTM12", "R_CONSTRUCCION_CTM12", "Must Cover Each Other"):"26_R_UNIDAD_CTM12_must_cover_each_other_R_CONSTRUCCION_CTM12",
            ("R_UNIDAD_INFORMAL", "R_CONSTRUCCION_INFORMAL", "Must Cover Each Other"):"27_R_UNIDAD_INFORMAL_must_cover_each_other_R_CONSTRUCCION_INFORMAL",
            ("R_ZONA_HOMO_GEOECONOMICA_CTM12", "R_ZONA_HOMOGENEA_FISICA_CTM12", "Must Cover Each Other"):"28_R_ZONA_HOMO_GEOECONOMICA_CTM12_must_cover_each_other_R_ZONA_HOMOGENEA_FISICA_CTM12"
        },
        "URBANO" : {
            ("U_TERRENO", "U_TERRENO", "Must Not Have Gaps"):"01_U_TERRENO_must_not_have_gaps",
            ("U_MANZANA", "U_MANZANA", "Must Not Have Gaps"):"02_U_MANZANA_must_not_have_gaps",
            ("U_SECTOR", "U_SECTOR", "Must Not Have Gaps"):"03_U_SECTOR_must_not_have_gaps",
            ("U_BARRIO", "U_BARRIO", "Must Not Have Gaps"):"04_U_BARRIO_must_not_have_gaps",
            ("U_PERIMETRO", "U_PERIMETRO", "Must Not Have Gaps"):"05_U_PERIMETRO_must_not_have_gaps",
            ("U_ZONA_HOMOGENEA_FISICA", "U_ZONA_HOMOGENEA_FISICA", "Must Not Have Gaps"):"06_U_ZONA_HOMOGENEA_FISICA_must_not_have_gaps",
            ("U_ZONA_HOMOGENEA_GEOECONOMICA", "U_ZONA_HOMOGENEA_GEOECONOMICA", "Must Not Have Gaps"):"07_U_ZONA_HOMOGENEA_GEOECONOMICA_must_not_have_gaps",
            
            ("U_TERRENO", "U_TERRENO", "Must Not Overlap"):"08_U_TERRENO_must_not_overlap",
            ("U_MANZANA", "U_MANZANA", "Must Not Overlap"):"09_U_MANZANA_must_not_overlap",
            ("U_SECTOR", "U_SECTOR", "Must Not Overlap"):"10_U_SECTOR_must_not_overlap",
            ("U_BARRIO", "U_BARRIO", "Must Not Overlap"):"11_U_BARRIO_must_not_overlap",
            ("U_PERIMETRO", "U_PERIMETRO", "Must Not Overlap"):"12_U_PERIMETRO_must_not_overlap",
            ("U_CONSTRUCCION", "U_CONSTRUCCION", "Must Not Overlap"):"13_U_CONSTRUCCION_must_not_overlap",
            ("U_UNIDAD", "U_UNIDAD", "Must Not Overlap"):"14_U_UNIDAD_must_not_overlap",
            ("U_NOMEN_DOMICILIARIA", "U_NOMEN_DOMICILIARIA", "Must Not Overlap"):"15_U_NOMEN_DOMICILIARIA_must_not_overlap",
            ("U_ZONA_HOMOGENEA_FISICA", "U_ZONA_HOMOGENEA_FISICA", "Must Not Overlap"):"16_U_ZONA_HOMOGENEA_FISICA_must_not_overlap",
            ("U_ZONA_HOMOGENEA_GEOECONOMICA", "U_ZONA_HOMOGENEA_GEOECONOMICA", "Must Not Overlap"):"17_U_ZONA_HOMOGENEA_GEOECONOMICA_must_not_overlap",
            
            ("U_SECTOR", "U_PERIMETRO", "Must Be Covered By Feature Class Of"):"18_U_SECTOR_must_be_covered_by_U_PERIMETRO",
            ("U_BARRIO", "U_SECTOR", "Must Be Covered By Feature Class Of"):"19_U_BARRIO_must_be_covered_by_U_SECTOR",
            ("U_MANZANA", "U_BARRIO", "Must Be Covered By Feature Class Of"):"20_U_MANZANA_must_be_covered_by_U_BARRIO",
            ("U_MANZANA", "U_SECTOR", "Must Be Covered By Feature Class Of"):"20_U_MANZANA_must_be_covered_by_U_SECTOR",
            ("U_TERRENO", "U_MANZANA", "Must Be Covered By Feature Class Of"):"21_U_TERRENO_must_be_covered_by_U_MANZANA",
            ("U_CONSTRUCCION", "U_TERRENO", "Must Be Covered By Feature Class Of"):"22_U_CONSTRUCCION_must_be_covered_by_U_TERRENO",
            ("U_TERRENO", "U_ZONA_HOMOGENEA_GEOECONOMICA", "Must Be Covered By Feature Class Of"):"23_U_TERRENO_must_be_covered_by_U_ZONA_HOMOGENEA_GEOECONOMICA",
            ("U_TERRENO", "U_ZONA_HOMOGENEA_FISICA", "Must Be Covered By Feature Class Of"):"24_U_TERRENO_must_be_covered_by_U_ZONA_HOMOGENEA_FISICA",
            
            ("U_TERRENO", "U_MANZANA", "Must Cover Each Other"):"25_U_TERRENO_must_cover_each_other_U_MANZANA",
            ("U_MANZANA","U_TERRENO" , "Must Cover Each Other"):"25_U_TERRENO_must_cover_each_other_U_MANZANA",
            ("U_UNIDAD", "U_CONSTRUCCION", "Must Cover Each Other"):"26_U_UNIDAD_must_cover_each_other_U_CONSTRUCCION",
            ("U_ZONA_HOMOGENEA_GEOECONOMICA", "U_ZONA_HOMOGENEA_FISICA", "Must Cover Each Other"):"27_U_ZONA_HOMOGENEA_GEOECONOMICA_must_cover_each_other_U_ZONA_HOMOGENEA_FISICA"
        },
        "RURAL": {
            ("R_TERRENO", "R_TERRENO", "Must Not Have Gaps"):"01_R_TERRENO_must_not_have_gaps",
            ("R_VEREDA", "R_VEREDA", "Must Not Have Gaps"):"02_R_VEREDA_must_not_have_gaps",
            ("R_SECTOR", "R_SECTOR", "Must Not Have Gaps"):"03_R_SECTOR_must_not_have_gaps",
            ("R_ZONA_HOMOGENEA_FISICA", "R_ZONA_HOMOGENEA_FISICA", "Must Not Have Gaps"):"04_R_ZONA_HOMOGENEA_FISICA_must_not_have_gaps",
            ("R_ZONA_HOMOGENEA_GEOECONOMICA", "R_ZONA_HOMOGENEA_GEOECONOMICA", "Must Not Have Gaps"):"05_R_ZONA_HOMOGENEA_GEOECONOMICA_must_not_have_gaps",
            ("R_TERRENO", "R_TERRENO", "Must Not Overlap"):"06_R_TERRENO_must_not_overlap",
            ("R_VEREDA", "R_VEREDA", "Must Not Overlap"):"07_R_VEREDA_must_not_overlap",
            ("R_SECTOR", "R_SECTOR", "Must Not Overlap"):"08_R_SECTOR_must_not_overlap",
            ("R_CONSTRUCCION", "R_CONSTRUCCION", "Must Not Overlap"):"09_R_CONSTRUCCION_must_not_overlap",
            ("R_UNIDAD", "R_UNIDAD", "Must Not Overlap"):"10_R_UNIDAD_must_not_overlap",
            ("R_NOMEN_DOMICILIARIA", "R_NOMEN_DOMICILIARIA", "Must Not Overlap"):"11_R_NOMEN_DOMICILIARIA_must_not_overlap",
            ("R_ZONA_HOMOGENEA_FISICA", "R_ZONA_HOMOGENEA_FISICA", "Must Not Overlap"):"12_R_ZONA_HOMOGENEA_FISICA_must_not_overlap",
            ("R_ZONA_HOMOGENEA_GEOECONOMICA", "R_ZONA_HOMOGENEA_GEOECONOMICA", "Must Not Overlap"):"13_R_ZONA_HOMOGENEA_GEOECONOMICA_must_not_overlap",
            ("R_VEREDA", "R_SECTOR", "Must Be Covered By Feature Class Of"):"14_R_VEREDA_must_be_covered_by_R_SECTOR",
            ("R_TERRENO", "R_VEREDA", "Must Be Covered By Feature Class Of"):"15_R_TERRENO_must_be_covered_by_R_VEREDA",
            ("R_CONSTRUCCION", "R_TERRENO", "Must Be Covered By Feature Class Of"):"16_R_CONSTRUCCION_must_be_covered_by_R_TERRENO",
            ("R_TERRENO", "R_ZONA_HOMOGENEA_GEOECONOMICA", "Must Be Covered By Feature Class Of"):"17_R_TERRENO_must_be_covered_by_R_ZONA_HOMOGENEA_GEOECONOMICA",
            ("R_TERRENO", "R_ZONA_HOMOGENEA_FISICA", "Must Be Covered By Feature Class Of"):"18_R_TERRENO_must_be_covered_by_R_ZONA_HOMOGENEA_FISICA",
            ("R_TERRENO", "R_VEREDA", "Must Cover Each Other"):"19_R_TERRENO_must_cover_each_other_R_VEREDA",
            ("R_VEREDA","R_TERRENO", "Must Cover Each Other"):"19_R_TERRENO_must_cover_each_other_R_VEREDA",
            ("R_UNIDAD", "R_CONSTRUCCION", "Must Cover Each Other"):"20_R_UNIDAD_must_cover_each_other_R_CONSTRUCCION",
            ("R_ZONA_HOMOGENEA_GEOECONOMICA", "R_ZONA_HOMOGENEA_FISICA", "Must Cover Each Other"):"21_R_ZONA_HOMOGENEA_GEOECONOMICA_must_cover_each_other_R_ZONA_HOMOGENEA_FISICA"
        }


    }
    
    # Si no hay reglas específicas para el grupo, crear reglas dinámicas
    if group_name not in conditions:
        conditions[group_name] = {}
    
    return conditions.get(group_name, {})

def process_feature_classes(gdb_path, valid_groups):
    arcpy.env.workspace = gdb_path
    feature_classes = arcpy.ListFeatureClasses()
    
    #arcpy.AddMessage(f"\nFeature classes encontrados: {feature_classes}")
    
    grouped_fcs = {}
    for fc in feature_classes:
        group_name = get_group_name(fc)
        if group_name in valid_groups:
            if group_name not in grouped_fcs:
                grouped_fcs[group_name] = []
            grouped_fcs[group_name].append(fc)
    
    #arcpy.AddMessage(f"\nGrupos detectados: {list(grouped_fcs.keys())}")
    
    for group_name, fcs in grouped_fcs.items():
        process_group(group_name, fcs, gdb_path)

def process_group(group_name, feature_classes, gdb_path):
    try:
        base_path = get_relative_path()
        output_dir = create_output_directory(base_path, group_name)
        rules = get_rule_conditions(group_name)
        
        arcpy.AddMessage(f"\nProcesando grupo: {group_name}")
        arcpy.AddMessage(f"Directorio de salida: {output_dir}")
        
        for fc in feature_classes:
            count = int(arcpy.GetCount_management(os.path.join(gdb_path, fc))[0])
            if count == 0:
                arcpy.AddMessage(f"Feature class {fc} está vacío - saltando")
                continue
                
            arcpy.AddMessage(f"Feature class {fc} tiene {count} registros")
            process_single_fc(fc, gdb_path, output_dir, rules)
            
    except Exception as e:
        arcpy.AddError(f"Error procesando grupo {group_name}: {str(e)}")

    try:
        arcpy.AddMessage(f"\nProcesando feature class: {fc}")
        
        # Si no hay reglas predefinidas, crear reglas basadas en los datos
        if not rules:
            unique_rules = set()
            with arcpy.da.SearchCursor(os.path.join(gdb_path, fc), 
                                     ["OriginObjectClassName", "DestinationObjectClassName", "RuleDescription"]) as cursor:
                for row in cursor:
                    unique_rules.add((row[0] if row[0] else "", 
                                    row[1] if row[1] else "", 
                                    row[2] if row[2] else ""))
                
                # Crear reglas dinámicas
                rules_dict = {}
                for i, rule in enumerate(unique_rules, 1):
                    origin_class, dest_class, rule_desc = rule
                    if origin_class:  # Solo si hay una clase de origen
                        rule_name = f"{i:02d}.{origin_class}_{rule_desc.replace(' ', '_').lower()}"
                        rules_dict[rule] = rule_name
                rules = rules_dict

        #arcpy.AddMessage(f"Reglas a aplicar: {rules}")
        
        fields = [f.name for f in arcpy.ListFields(os.path.join(gdb_path, fc))]
        
        if "SHAPE@" not in fields:
            fields.insert(0, "SHAPE@")
        else:
            fields.remove("SHAPE@")
            fields.insert(0, "SHAPE@")

        rule_groups = {}
        with arcpy.da.SearchCursor(os.path.join(gdb_path, fc), fields) as cursor:
            rows_processed = 0
            for row in cursor:
                origin_class = row[fields.index("OriginObjectClassName")] if "OriginObjectClassName" in fields else ""
                dest_class = row[fields.index("DestinationObjectClassName")] if "DestinationObjectClassName" in fields else ""
                rule_desc = row[fields.index("RuleDescription")] if "RuleDescription" in fields else ""
                
                rule_key = (origin_class, dest_class, rule_desc)
                
                if rule_key in rules:
                    output_name = rules[rule_key]
                    if output_name not in rule_groups:
                        rule_groups[output_name] = []
                    rule_groups[output_name].append(row)
                    rows_processed += 1
        
        arcpy.AddMessage(f"Registros procesados: {rows_processed}")
        arcpy.AddMessage(f"Grupos de reglas encontrados: {list(rule_groups.keys())}")
        
        if not rule_groups:
            arcpy.AddMessage(f"No se encontraron registros que coincidan con las reglas para {fc}")
            return
        
        for output_name, features in rule_groups.items():
            export_to_shapefile(fc, features, output_dir, output_name, fields)
            
    except Exception as e:
        arcpy.AddError(f"Error procesando feature class {fc}: {str(e)}")

def get_rule_mapping():
    """
    Retorna un diccionario con el mapeo de reglas inglés/español
    """
    return {
        'Must Not Have Gaps': 'No debe tener espacios',
        'Must Not Overlap': 'No debe superponerse',
        'Must Be Covered By Feature Class Of': 'Debe ser cubierto por la clase de entidad de',
        'Must Cover Each Other': 'Deben cubrirse entre ellos'
    }

def verify_rule_language(gdb_path):
    """
    Verifica el idioma de las reglas en la geodatabase
    """
    arcpy.env.workspace = gdb_path
    rule_examples = set()
    
    for fc in arcpy.ListFeatureClasses("*_errors_*"):
        with arcpy.da.SearchCursor(fc, ["RuleDescription"]) as cursor:
            for row in cursor:
                if row[0]:
                    rule_examples.add(row[0].strip())
    
    arcpy.AddMessage("\nReglas encontradas en la geodatabase:")
    for rule in sorted(rule_examples):
        arcpy.AddMessage(f"  - {rule}")
    
    return rule_examples
def normalize_rule_description(rule_desc):
    """
    Normaliza la descripción de la regla, manejando tanto inglés como español
    """
    rule_mapping = {
        'Must Not Have Gaps': 'No debe tener espacios',
        'Must Not Overlap': 'No debe superponerse',
        'Must Be Covered By Feature Class Of': 'Debe ser cubierto por la clase de entidad de',
        'Must Cover Each Other': 'Deben cubrirse entre ellos'
    }
    
    # Si la regla está en español, convertirla a inglés
    reverse_mapping = {esp: eng for eng, esp in rule_mapping.items()}
    
    # Intentar traducir de español a inglés si es necesario
    if rule_desc in reverse_mapping:
        return reverse_mapping[rule_desc]
    
    # Si ya está en inglés o no se encuentra traducción, devolver la original
    return rule_desc

def process_single_fc(fc, gdb_path, output_dir, rules):
    try:
        arcpy.AddMessage(f"\nProcesando feature class: {fc}")
        
        fields = [f.name for f in arcpy.ListFields(os.path.join(gdb_path, fc))]
        if "SHAPE@" not in fields:
            fields.insert(0, "SHAPE@")
        else:
            fields.remove("SHAPE@")
            fields.insert(0, "SHAPE@")
        
        rule_groups = {}
        with arcpy.da.SearchCursor(os.path.join(gdb_path, fc), fields) as cursor:
            for row in cursor:
                origin_index = fields.index("OriginObjectClassName")
                dest_index = fields.index("DestinationObjectClassName")
                rule_index = fields.index("RuleDescription")
                
                origin_class = row[origin_index].strip() if row[origin_index] else ""
                dest_class = row[dest_index].strip() if row[dest_index] else ""
                rule_desc = row[rule_index].strip() if row[rule_index] else ""
                
                # Normalizar la regla a inglés
                normalized_rule_desc = normalize_rule_description(rule_desc)
                
                # Crear las dos posibles combinaciones para Must Cover Each Other
                rule_key = (origin_class, dest_class, normalized_rule_desc)
                rule_key_reversed = (dest_class, origin_class, normalized_rule_desc)
                
                output_name = None
                
                # Verificar si es una regla Must Cover Each Other
                if normalized_rule_desc == "Must Cover Each Other" or normalized_rule_desc == "Deben cubrirse entre ellos":
                    # Buscar la regla en ambas direcciones
                    if rule_key in rules:
                        output_name = rules[rule_key]
                    elif rule_key_reversed in rules:
                        output_name = rules[rule_key_reversed]
                else:
                    # Para otras reglas, mantener el comportamiento original
                    if rule_key in rules:
                        output_name = rules[rule_key]
                
                if output_name:
                    if output_name not in rule_groups:
                        rule_groups[output_name] = []
                    rule_groups[output_name].append(row)
                else:
                    arcpy.AddMessage(f"  No se encontró coincidencia para la regla: {rule_key}")
        
        for output_name, features in rule_groups.items():
            arcpy.AddMessage(f"\nExportando {output_name} con {len(features)} registros")
            export_to_shapefile(fc, features, output_dir, output_name, fields)
            
    except Exception as e:
        arcpy.AddError(f"Error procesando feature class {fc}: {str(e)}")
        import traceback
        arcpy.AddError(traceback.format_exc())
                     
def export_to_shapefile(fc, features, output_dir, output_name, fields):
    try:
        arcpy.AddMessage(f"\nExportando: {output_name}")
        #arcpy.AddMessage(f"Número de features a exportar: {len(features)}")
        
        # Crear un nombre temporal único
        temp_name = f"temp_{uuid.uuid4().hex[:8]}"
        temp_fc = os.path.join("in_memory", temp_name)
        
        # Asegurar que el directorio existe
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Generar el path de salida
        output_path = os.path.join(output_dir, f"{output_name}.shp")
        
        try:
            # Limpiar cualquier feature class temporal existente
            if arcpy.Exists(temp_fc):
                arcpy.Delete_management(temp_fc)
                
            # Limpiar archivo de salida si existe
            if arcpy.Exists(output_path):
                arcpy.Delete_management(output_path)
                # Esperar un momento para asegurar que el archivo se liberó
                import time
                time.sleep(1)
                
            # Crear feature class temporal
            desc = arcpy.Describe(os.path.join(arcpy.env.workspace, fc))
            arcpy.CreateFeatureclass_management(
                "in_memory",
                temp_name,
                desc.shapeType,
                os.path.join(arcpy.env.workspace, fc),
                spatial_reference=desc.spatialReference
            )
            
            # Copiar features
            features_copied = 0
            with arcpy.da.InsertCursor(temp_fc, fields) as insert_cursor:
                for feature in features:
                    insert_cursor.insertRow(feature)
                    features_copied += 1
                    
            #arcpy.AddMessage(f"Features copiados: {features_copied}")
            
            # Establecer el entorno para permitir sobrescritura
            arcpy.env.overwriteOutput = True
            
            # Copiar a shapefile final
            result = arcpy.CopyFeatures_management(temp_fc, output_path)
            
            if arcpy.Exists(output_path):
                arcpy.AddMessage(f"Shapefile exportado exitosamente")
            else:
                raise Exception("El archivo de salida no se creó correctamente")
                
        finally:
            # Limpiar recursos temporales
            if arcpy.Exists(temp_fc):
                arcpy.Delete_management(temp_fc)
                
    except Exception as e:
        arcpy.AddError(f"Error exportando shapefile : {str(e)}")
        import traceback
        arcpy.AddError(traceback.format_exc())
        
      
def main():
    try:
        valid_groups = [
            "ZONA_HOMOGENEA_RURAL",
            "ZONA_HOMOGENEA_URBANO",
            "RURAL_CTM12",
            "URBANO",
            "ZONA_HOMOGENEA_URBANO_CTM12",
            "ZONA_HOMOGENEA_RURAL_CTM12",
            "URBANO_CTM12",
            "RURAL"
        ]
        
        base_path = get_relative_path()
        gdb_path = os.path.join(
            base_path,
            "Files",
            "Temporary_Files",
            "MODELO_IGAC",
            "Topology_Errors"
        )
        
        gdb_name = next((f for f in os.listdir(gdb_path) if f.endswith('.gdb')), None)
        if not gdb_name:
            raise ValueError("No se encontró una geodatabase en el directorio especificado")
        
        gdb_full_path = os.path.join(gdb_path, gdb_name)
        
        arcpy.AddMessage(f"\nIniciando procesamiento de: {gdb_full_path}")
        # Verificar el idioma de las reglas
        rule_examples = verify_rule_language(gdb_full_path)
        
        process_feature_classes(gdb_full_path, valid_groups)
        
        arcpy.AddMessage("\nProcesamiento completado exitosamente")
        
    except Exception as e:
        arcpy.AddError(f"Error en la ejecución del script: {str(e)}")

if __name__ == '__main__':
    main()