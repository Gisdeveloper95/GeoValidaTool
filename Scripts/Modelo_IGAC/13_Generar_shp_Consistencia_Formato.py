import os
import arcpy
import shutil
from pathlib import Path
import sys
sys.stdout.reconfigure(encoding='utf-8')
def find_project_root(current_dir):
    """
    Encuentra la raíz del proyecto basándose en la estructura interna de carpetas,
    independientemente del nombre del directorio raíz.
    """
    current = Path(current_dir).resolve()
    
    while current.parent != current:
        # Verifica la estructura característica del proyecto
        expected_structure = [
            current / "Files" / "Temporary_Files" / "MODELO_IGAC",
            current / "Files" / "Temporary_Files" / "array_config.txt"
        ]
        
        # Si encuentra la estructura esperada, este es el directorio raíz
        if all(path.exists() for path in expected_structure):
            arcpy.AddMessage(f"Raíz del proyecto encontrada en: {current}")
            return current
        
        # Si no es la raíz, sube un nivel
        current = current.parent
    
    raise ValueError(
        "No se encontró la raíz del proyecto. "
        "Verifique que está ejecutando el script desde dentro del proyecto y que existe "
        "la estructura: Files/Temporary_Files/MODELO_IGAC y array_config.txt"
    )

def create_directory_structure(root_path, dataset_name):
    """Crea la estructura de directorios necesaria."""
    base_path = root_path / "Files" / "Temporary_Files" / "MODELO_IGAC" / "03_INCONSISTENCIAS" / "CONSISTENCIA_FORMATO" / dataset_name
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path

def get_shape_mapping():
    """Define el mapeo de nombres de shapefiles según el dataset."""
    return {
        "URBANO_CTM12": {
            "U_BARRIO_CTM12":"1.U_BARRIO_CTM12_atributos_mal_calculados",
            "U_SECTOR_CTM12":"2.U_SECTOR_CTM12_atributos_mal_calculados",
            "U_MANZANA_CTM12":"3.U_MANZANA_CTM12_atributos_mal_calculados",
            "U_TERRENO_CTM12":"4.U_TERRENO_CTM12_atributos_mal_calculados",
            "U_CONSTRUCCION_CTM12":"5.U_CONSTRUCCION_CTM12_atributos_mal_calculados",
            "U_UNIDAD_CTM12":"6.U_UNIDAD_CTM12_atributos_mal_calculados",
            "U_NOMEN_DOMICILIARIA_CTM12":"7.U_NOMEN_DOMICILIARIA_CTM12_atributos_mal_calculados",
            "U_NOMENCLATURA_VIAL_CTM12":"8.U_NOMENCLATURA_VIAL_CTM12_atributos_mal_calculados",
            "INTERSECT_U_MANZANA_CTM12_U_SECTOR_CTM12":"9.U_SECTOR_CTM12_atributos_NO_coinciden_con_U_MANZANA_CTM12",
            "INTERSECT_U_TERRENO_CTM12_U_MANZANA_CTM12":"10.U_MANZANA_CTM12_atributos_NO_coinciden_con_U_TERRENO_CTM12",
            "INTERSECT_U_CONSTRUCCION_CTM12_U_TERRENO_CTM12":"11.U_TERRENO_CTM12_atributos_NO_coinciden_con_U_CONSTRUCCION_CTM12",
            "INTERSECT_U_UNIDAD_CTM12_U_CONSTRUCCION_CTM12":"12.U_CONSTRUCCION_CTM12_atributos_NO_coinciden_con_U_UNIDAD_CTM12",
            "INTERSECT_U_UNIDAD_CTM12_U_TERRENO_CTM12":"13.U_TERRENO_CTM12_atributos_NO_coinciden_con_U_UNIDAD_CTM12",
            "INTERSECT_U_NOMEN_DOMICILIARIA_CTM12_U_TERRENO_CTM12":"14.U_TERRENO_CTM12_atributos_NO_coinciden_con_U_NOMEN_DOMICILIARIA_CTM12"

        },
        "RURAL_CTM12": {
            "R_SECTOR_CTM12":"1.R_SECTOR_CTM12_atributos_mal_calculados",
            "R_VEREDA_CTM12":"2.R_VEREDA_CTM12_atributos_mal_calculados",
            "R_TERRENO_CTM12":"3.R_TERRENO_CTM12_atributos_mal_calculados",
            "R_CONSTRUCCION_CTM12":"4.R_CONSTRUCCION_CTM12_atributos_mal_calculados",
            "R_UNIDAD_CTM12":"5.R_UNIDAD_CTM12_atributos_mal_calculados",
            "R_NOMEN_DOMICILIARIA_CTM12":"6.R_NOMEN_DOMICILIARIA_CTM12_atributos_mal_calculados",
            "R_NOMENCLATURA_VIAL_CTM12":"7.R_NOMENCLATURA_VIAL_CTM12_atributos_mal_calculados",
            "INTERSECT_R_VEREDA_CTM12_R_SECTOR_CTM12":"8.R_SECTOR_CTM12_atributos_NO_coinciden_con_R_VEREDA_CTM12,",
            "INTERSECT_R_TERRENO_CTM12_R_VEREDA_CTM12":"9.R_VEREDA_CTM12_atributos_NO_coinciden_con_R_TERRENO_CTM12,",
            "INTERSECT_R_CONSTRUCCION_CTM12_R_TERRENO_CTM12":"10.R_TERRENO_CTM12_atributos_NO_coinciden_con_R_CONSTRUCCION_CTM12,",
            "INTERSECT_R_UNIDAD_CTM12_R_CONSTRUCCION_CTM12":"11.R_CONSTRUCCION_CTM12_atributos_NO_coinciden_con_R_UNIDAD_CTM12,",
            "INTERSECT_R_UNIDAD_CTM12_R_TERRENO_CTM12":"12.R_TERRENO_CTM12_atributos_NO_coinciden_con_R_UNIDAD_CTM12,",
            "INTERSECT_R_NOMEN_DOMICILIARIA_CTM12_R_TERRENO_CTM12":"13.R_TERRENO_CTM12_atributos_NO_coinciden_con_R_NOMEN_DOMICILIARIA_CTM12"

        },
        "URBANO": {
            "U_BARRIO":"1.U_BARRIO_atributos_mal_calculados",
            "U_SECTOR":"2.U_BARRIO_atributos_mal_calculados",
            "U_MANZANA":"3.U_SECTOR_atributos_mal_calculados",
            "U_TERRENO":"4.U_TERRENO_atributos_mal_calculados",
            "U_CONSTRUCCION":"5.U_CONSTRUCCION_atributos_mal_calculados",
            "U_UNIDAD":"6.U_UNIDAD_atributos_mal_calculados",
            "U_NOMENCLATURA_DOMICILIARIA":"7.U_NOMENCLATURA_DOMICILIARIA_atributos_mal_calculados",
            "U_NOMENCLATURACLATURA_VIAL":"8.U_NOMENCLATURACLATURA_VIAL_atributos_mal_calculados",
            "INTERSECT_U_MANZANA_U_SECTOR":"9.U_SECTOR_atributos_NO_coinciden_con_U_MANZANA",
            "INTERSECT_U_TERRENO_U_MANZANA":"10.U_MANZANA_atributos_NO_coinciden_con_U_TERRENO",
            "INTERSECT_U_CONSTRUCCION_U_TERRENO":"11.U_TERRENO_atributos_NO_coinciden_con_U_CONSTRUCCION",
            "INTERSECT_U_UNIDAD_U_CONSTRUCCION":"12.U_CONSTRUCCION_atributos_NO_coinciden_con_U_UNIDAD",
            "INTERSECT_U_UNIDAD_U_TERRENO":"13.U_TERRENO_atributos_NO_coinciden_con_U_UNIDAD",
            "INTERSECT_U_NOMENCLATURA_DOMICILIARIA_U_TERRENO":"14.U_TERRENO_atributos_NO_coinciden_con_U_NOMENCLATURA_DOMICILIARIA"

        },
        "RURAL": {
            "R_SECTOR":"1.R_SECTOR_atributos_mal_calculados",
            "R_VEREDA":"2.R_VEREDA_atributos_mal_calculados",
            "R_TERRENO":"3.R_TERRENO_atributos_mal_calculados",
            "R_CONSTRUCCION":"4.R_CONSTRUCCION_atributos_mal_calculados",
            "R_UNIDAD":"5.R_UNIDAD_atributos_mal_calculados",
            "R_NOMENCLATURA_DOMICILIARIA":"6.R_NOMENCLATURA_DOMICILIARIA_atributos_mal_calculados",
            "R_NOMENCLATURACLATURA_VIAL":"7.R_NOMENCLATURACLATURA_VIAL_atributos_mal_calculados",
            "INTERSECT_R_VEREDA_R_SECTOR":"8.R_SECTOR_atributos_NO_coinciden_con_R_VEREDA,",
            "INTERSECT_R_TERRENO_R_VEREDA":"9.R_VEREDA_atributos_NO_coinciden_con_R_TERRENO,",
            "INTERSECT_R_CONSTRUCCION_R_TERRENO":"10.R_TERRENO_atributos_NO_coinciden_con_R_CONSTRUCCION,",
            "INTERSECT_R_UNIDAD_R_CONSTRUCCION":"11.R_CONSTRUCCION_atributos_NO_coinciden_con_R_UNIDAD,",
            "INTERSECT_R_UNIDAD_R_TERRENO":"12.R_TERRENO_atributos_NO_coinciden_con_R_UNIDAD,",
            "INTERSECT_R_NOMENCLATURA_DOMICILIARIA_R_TERRENO":"13.R_TERRENO_atributos_NO_coinciden_con_R_NOMENCLATURA_DOMICILIARIA"

        }
    }

def find_and_convert_feature_classes(gdb_path, dataset_name, output_dir, shape_mapping):
    """Busca y convierte feature classes de una geodatabase a shapefiles."""
    try:
        mapping = shape_mapping.get(dataset_name, {})
        arcpy.env.workspace = str(gdb_path)
        
        # Buscar en datasets si la GDB los tiene
        datasets = arcpy.ListDatasets()
        if datasets:
            for ds in datasets:
                if ds == dataset_name:  # Solo procesar el dataset solicitado
                    arcpy.env.workspace = str(gdb_path / ds)
                    fcs = arcpy.ListFeatureClasses()
                    for fc in fcs:
                        if fc in mapping:
                            output_name = mapping[fc] + ".shp"
                            output_path = output_dir / output_name
                            if not output_path.exists():
                                arcpy.FeatureClassToFeatureClass_conversion(
                                    str(gdb_path / ds / fc),
                                    str(output_dir),
                                    output_name
                                )
                                arcpy.AddMessage(f"Convertido: {fc} -> {output_name}")
        
        # Buscar feature classes sueltos (sin dataset)
        arcpy.env.workspace = str(gdb_path)
        fcs = arcpy.ListFeatureClasses()
        for fc in fcs:
            if fc in mapping:
                output_name = mapping[fc] + ".shp"
                output_path = output_dir / output_name
                if not output_path.exists():
                    arcpy.FeatureClassToFeatureClass_conversion(
                        str(gdb_path / fc),
                        str(output_dir),
                        output_name
                    )
                    arcpy.AddMessage(f"Convertido: {fc} -> {output_name}")
                    
    except Exception as e:
        arcpy.AddWarning(f"Error procesando {gdb_path}: {str(e)}")

def main():
    try:
        # Encontrar la raíz del proyecto
        project_root = find_project_root(os.getcwd())
        arcpy.AddMessage(f"Raíz del proyecto encontrada: {project_root}")

        # Configurar rutas a las geodatabases
        base_path = project_root / "Files" / "Temporary_Files" / "MODELO_IGAC"
        formato_gdb = next((base_path / "consistencia_formato_temp").glob("*.gdb"))
        geoespacial_gdb = next((base_path / "consistencia_geoespacial_temp").glob("*.gdb"))

        # Leer datasets a procesar desde archivo
        config_path = project_root / "Files" / "Temporary_Files" / "array_config.txt"
        datasets_to_process = []
        try:
            with open(config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        dataset_name = line.strip('",[]').strip()
                        if dataset_name:
                            datasets_to_process.append(dataset_name)
        except Exception as e:
            arcpy.AddWarning(f"Error leyendo configuración: {str(e)}")
            datasets_to_process = ["URBANO_CTM12"]  # Dataset por defecto

        # Obtener mapeo de nombres
        shape_mapping = get_shape_mapping()

        # Procesar cada dataset
        for dataset in datasets_to_process:
            arcpy.AddMessage(f"\nProcesando dataset: {dataset}")
            output_dir = create_directory_structure(project_root, dataset)

            # Procesar ambas geodatabases
            find_and_convert_feature_classes(formato_gdb, dataset, output_dir, shape_mapping)
            find_and_convert_feature_classes(geoespacial_gdb, dataset, output_dir, shape_mapping)

    except Exception as e:
        arcpy.AddError(f"Error general: {str(e)}")

if __name__ == "__main__":
    main()