import os
import arcpy
import pandas as pd
import sqlite3
import openpyxl
from pathlib import Path
import json
import sys
sys.stdout.reconfigure(encoding='utf-8')
def encontrar_raiz_proyecto():
    """
    Encuentra la raíz del proyecto basándose en su estructura interna,
    independientemente del nombre del directorio.
    """
    ruta_actual = Path(os.getcwd()).resolve()
    
    while ruta_actual.parent != ruta_actual:
        # Verifica la estructura característica del proyecto
        rutas_esperadas = [
            ruta_actual / "Files" / "Temporary_Files" / "MODELO_IGAC",
            ruta_actual / "Files" / "Temporary_Files" / "array_config.txt",
            ruta_actual / "Files" / "Temporary_Files" / "MODELO_IGAC" / "02_TOPOLOGIA",
            ruta_actual / "Files" / "Temporary_Files" / "MODELO_IGAC" / "db"
        ]
        
        if all(path.exists() for path in rutas_esperadas):
            print(f"Raíz del proyecto encontrada en: {ruta_actual}")
            return ruta_actual
        
        ruta_actual = ruta_actual.parent
    
    raise Exception(
        "No se encontró la raíz del proyecto.\n"
        "Verifique que está ejecutando el script desde dentro del proyecto "
        "y que existe la siguiente estructura:\n"
        "- Files/Temporary_Files/MODELO_IGAC\n"
        "- Files/Temporary_Files/array_config.txt\n"
        "- Files/Temporary_Files/MODELO_IGAC/02_TOPOLOGIA\n"
        "- Files/Temporary_Files/MODELO_IGAC/db"
    )

def crear_directorio_inconsistencias(raiz_proyecto):
    """
    Crea el directorio para inconsistencias si no existe.
    Incluye verificaciones de permisos y manejo de errores.
    """
    try:
        ruta = raiz_proyecto / 'Files' / 'Temporary_Files' / 'MODELO_IGAC' / '03_INCONSISTENCIAS' / 'CONSISTENCIA_FORMATO'
        
        if not ruta.parent.exists():
            raise Exception(f"El directorio padre no existe: {ruta.parent}")
            
        # Verificar permisos antes de crear
        if ruta.parent.exists() and not os.access(ruta.parent, os.W_OK):
            raise PermissionError(f"No tiene permisos de escritura en: {ruta.parent}")
            
        os.makedirs(ruta, exist_ok=True)
        
        if not os.access(ruta, os.W_OK):
            raise PermissionError(f"No tiene permisos de escritura en el directorio creado: {ruta}")
            
        print(f"Directorio de inconsistencias creado/verificado en: {ruta}")
        return ruta
        
    except Exception as e:
        print(f"Error al crear directorio de inconsistencias: {str(e)}")
        raise

def obtener_excel_topologia(raiz_proyecto, dataset):
    """
    Encuentra el archivo Excel en el directorio de topología para el dataset específico.
    Incluye verificaciones adicionales y manejo de errores mejorado.
    """
    try:
        ruta_topologia = raiz_proyecto / 'Files' / 'Temporary_Files' / 'MODELO_IGAC' / '02_TOPOLOGIA' / dataset
        print(f"Buscando Excel en: {ruta_topologia}")
        
        if not ruta_topologia.exists():
            raise Exception(f"El directorio no existe: {ruta_topologia}")
            
        if not os.access(ruta_topologia, os.R_OK):
            raise PermissionError(f"No tiene permisos de lectura en: {ruta_topologia}")
        
        archivos_excel = list(ruta_topologia.glob('*.xlsx'))
        if not archivos_excel:
            raise Exception(f"No se encontró archivo Excel en {ruta_topologia}")
        
        # Verificar permisos de lectura del archivo Excel
        if not os.access(archivos_excel[0], os.R_OK):
            raise PermissionError(f"No tiene permisos de lectura en: {archivos_excel[0]}")
            
        print(f"Excel encontrado: {archivos_excel[0]}")
        return archivos_excel[0]
        
    except Exception as e:
        print(f"Error al buscar archivo Excel: {str(e)}")
        raise

def obtener_datos_sqlite(raiz_proyecto, dataset, columna, db_name):
    """
    Obtiene el valor más reciente de la columna específica en la tabla del dataset.
    Incluye mejor manejo de errores y verificaciones.
    """
    try:
        db_path = raiz_proyecto / 'Files' / 'Temporary_Files' / 'MODELO_IGAC' / 'db' / db_name
        print(f"Conectando a base de datos: {db_path}")
        
        if not db_path.exists():
            raise FileNotFoundError(f"No se encontró la base de datos: {db_path}")
            
        if not os.access(db_path, os.R_OK):
            raise PermissionError(f"No tiene permisos de lectura en la base de datos: {db_path}")
        
        conn = sqlite3.connect(str(db_path))
        
        query = f"""
        SELECT {columna} 
        FROM {dataset} 
        ORDER BY fecha_proceso DESC 
        LIMIT 1
        """
        
        try:
            resultado = pd.read_sql_query(query, conn)[columna].iloc[0]
            print(f"Valor para columna {columna} en {db_name}: {resultado}")
            return resultado
        except IndexError:
            print(f"No se encontraron datos para {columna} en {db_name}")
            return 0
        except Exception as e:
            print(f"Error al consultar columna {columna} en {db_name}: {str(e)}")
            return 0
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error en obtener_datos_sqlite: {str(e)}")
        return 0

def get_active_datasets(root_path):
    """
    Lee y retorna los datasets activos del archivo de configuración.
    """
    try:
        config_path = root_path / "Files" / "Temporary_Files" / "array_config.txt"
        
        if not config_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo de configuración en: {config_path}")
            
        active_datasets = []
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    dataset_name = line.strip('",[]').strip()
                    if dataset_name:
                        active_datasets.append(dataset_name)
        
        if not active_datasets:
            print("No se encontraron datasets activos. Usando configuración por defecto.")
            return ["URBANO_CTM12", "RURAL_CTM12"]
            
        print("\nDatasets activos encontrados:")
        for ds in active_datasets:
            print(f"  - {ds}")
            
        return active_datasets
        
    except Exception as e:
        print(f"Error al leer configuración: {str(e)}")
        print("Usando configuración por defecto")
        return ["URBANO_CTM12", "RURAL_CTM12"]

def procesar_dataset(raiz_proyecto, dataset, mapeo_celdas):
    """Procesa un dataset específico"""
    try:
        print(f"\nIniciando procesamiento de {dataset}")
        
        # Encontrar el archivo Excel
        ruta_excel = obtener_excel_topologia(raiz_proyecto, dataset)
        print(f"Trabajando con Excel: {ruta_excel}")
        
        # Cargar el archivo Excel usando openpyxl para modificar celdas específicas
        wb = openpyxl.load_workbook(ruta_excel)
        hoja = wb['Consistencia Formato']
        
        # Procesar cada mapeo de celdas
        for celda, columnas in mapeo_celdas[dataset].items():
            print(f"Procesando celda {celda} para columnas {columnas}")
            
            # Obtener valores de ambas bases de datos
            valor_errores = obtener_datos_sqlite(raiz_proyecto, dataset, columnas[0], 'errores_consistencia_formato.db')
            valor_excepciones = obtener_datos_sqlite(raiz_proyecto, dataset, columnas[1], 'excepciones_consistencia_formato.db')
            
            # Calcular la diferencia
            diferencia = valor_errores - valor_excepciones
            
            # Actualizar la celda específica en el Excel
            hoja[celda] = diferencia
            print(f"Diferencia calculada para {celda}: {diferencia} ({valor_errores} - {valor_excepciones})")
        
        # Guardar los cambios
        print(f"Guardando cambios en {ruta_excel}")
        wb.save(ruta_excel)
        print(f"Procesamiento completado para {dataset}")
        
    except Exception as e:
        print(f"Error procesando {dataset}: {str(e)}")

def main():
    # Definir los datasets a procesar
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
        config_path = os.path.join(proyecto_dir,  "Files", "Temporary_Files", "array_config.txt")

        # Leer el archivo y filtrar solo los datasets activos
        DATASETS_TO_PROCESS = []
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()  # Limpiar espacios alrededor
                if line and not line.startswith('#'):
                    # Eliminar posibles comillas, comas o corchetes en el nombre del dataset
                    dataset_name = line.strip('",[]').strip()
                    if dataset_name:  # Solo agregar si no está vacío
                        DATASETS_TO_PROCESS.append(dataset_name)

        # Imprimir el contenido de DATASETS_TO_PROCESS para depuración
        print("\nConfiguración de datasets cargada:")
        print("--------------------------------")
        print("Datasets que serán procesados:")
        for ds in DATASETS_TO_PROCESS:
            print(f"  - {ds}")
        print("--------------------------------\n")

    except Exception as e:
        print(f"Error al cargar configuración: {str(e)}")
        # Configuración por defecto en caso de error
        DATASETS_TO_PROCESS = ["URBANO_CTM12", "RURAL_CTM12"]
        print("\nUsando configuración por defecto:")
        print("--------------------------------")
        print("Datasets que serán procesados:")
        for ds in DATASETS_TO_PROCESS:
            print(f"  - {ds}")
        print("--------------------------------\n")

    # Mapeo de celdas y columnas en la tabla (ahora con tuplas para ambas columnas)
    MAPEO_CELDAS = {
        "URBANO_CTM12": {
            "H5":("U_BARRIO_CTM12", "U_BARRIO_CTM12"),
            "H6":( "U_SECTOR_CTM12", "U_SECTOR_CTM12"),
            "H7":( "U_MANZANA_CTM12", "U_MANZANA_CTM12"),
            "H8":( "U_TERRENO_CTM12", "U_TERRENO_CTM12"),
            "H9":( "U_CONSTRUCCION_CTM12", "U_CONSTRUCCION_CTM12"),
            "H10":( "U_UNIDAD_CTM12", "U_UNIDAD_CTM12"),
            "H11":( "U_NOMEN_DOMICILIARIA_CTM12", "U_NOMEN_DOMICILIARIA_CTM12"),
            "H12":( "U_NOMENCLATURA_VIAL_CTM12", "U_NOMENCLATURA_VIAL_CTM12"),
            "H13":( "U_MANZANA_CTM12_U_SECTOR_CTM12", "U_MANZANA_CTM12_U_SECTOR_CTM12"),
            "H14":( "U_TERRENO_CTM12_U_MANZANA_CTM12", "U_TERRENO_CTM12_U_MANZANA_CTM12"),
            "H15":( "U_CONSTRUCCION_CTM12_U_TERRENO_CTM12", "U_CONSTRUCCION_CTM12_U_TERRENO_CTM12"),
            "H16":( "U_UNIDAD_CTM12_U_CONSTRUCCION_CTM12", "U_UNIDAD_CTM12_U_CONSTRUCCION_CTM12"),
            "H17":( "U_UNIDAD_CTM12_U_TERRENO_CTM12", "U_UNIDAD_CTM12_U_TERRENO_CTM12"),
            "H18":( "U_NOMEN_DOMICILIARIA_CTM12_U_TERRENO_CTM12", "U_NOMEN_DOMICILIARIA_CTM12_U_TERRENO_CTM12")

        },
        "RURAL_CTM12": {
            "H20":( "R_SECTOR_CTM12", "R_SECTOR_CTM12"),
            "H21":( "R_VEREDA_CTM12", "R_VEREDA_CTM12"),
            "H22":( "R_TERRENO_CTM12", "R_TERRENO_CTM12"),
            "H23":( "R_CONSTRUCCION_CTM12", "R_CONSTRUCCION_CTM12"),
            "H24":( "R_UNIDAD_CTM12", "R_UNIDAD_CTM12"),
            "H25":( "R_NOMEN_DOMICILIARIA_CTM12", "R_NOMEN_DOMICILIARIA_CTM12"),
            "H26":( "R_NOMENCLATURA_VIAL_CTM12", "R_NOMENCLATURA_VIAL_CTM12"),
            "H27":( "R_VEREDA_CTM12_R_SECTOR_CTM12", "R_VEREDA_CTM12_R_SECTOR_CTM12"),
            "H28":( "R_TERRENO_CTM12_R_VEREDA_CTM12", "R_TERRENO_CTM12_R_VEREDA_CTM12"),
            "H29":( "R_CONSTRUCCION_CTM12_R_TERRENO_CTM12", "R_CONSTRUCCION_CTM12_R_TERRENO_CTM12"),
            "H30":( "R_UNIDAD_CTM12_R_CONSTRUCCION_CTM12", "R_UNIDAD_CTM12_R_CONSTRUCCION_CTM12"),
            "H31":( "R_UNIDAD_CTM12_R_TERRENO_CTM12", "R_UNIDAD_CTM12_R_TERRENO_CTM12"),
            "H32":( "R_NOMEN_DOMICILIARIA_CTM12_R_TERRENO_CTM12","R_NOMEN_DOMICILIARIA_CTM12_R_TERRENO_CTM12")        

        },
        "URBANO": {
            "H5":( "U_BARRIO", "U_BARRIO"),
            "H6":( "U_SECTOR", "U_SECTOR"),
            "H7":( "U_MANZANA", "U_MANZANA"),
            "H8":( "U_TERRENO", "U_TERRENO"),
            "H9":( "U_CONSTRUCCION", "U_CONSTRUCCION"),
            "H10":( "U_UNIDAD", "U_UNIDAD"),
            "H11":( "U_NOMENCLATURA_DOMICILIARIA", "U_NOMENCLATURA_DOMICILIARIA"),
            "H12":( "U_NOMENCLATURACLATURA_VIAL", "U_NOMENCLATURACLATURA_VIAL"),
            "H13":( "U_MANZANA_U_SECTOR", "U_MANZANA_U_SECTOR"),
            "H14":( "U_TERRENO_U_MANZANA", "U_TERRENO_U_MANZANA"),
            "H15":( "U_CONSTRUCCION_U_TERRENO", "U_CONSTRUCCION_U_TERRENO"),
            "H16":( "U_UNIDAD_U_CONSTRUCCION", "U_UNIDAD_U_CONSTRUCCION"),
            "H17":( "U_UNIDAD_U_TERRENO", "U_UNIDAD_U_TERRENO"),
            "H18":( "U_NOMENCLATURA_DOMICILIARIA_U_TERRENO","U_NOMENCLATURA_DOMICILIARIA_U_TERRENO")

        },
        "RURAL": {
            "H20":( "R_SECTOR", "R_SECTOR"),
            "H21":( "R_VEREDA", "R_VEREDA"),
            "H22":( "R_TERRENO", "R_TERRENO"),
            "H23":( "R_CONSTRUCCION", "R_CONSTRUCCION"),
            "H24":( "R_UNIDAD", "R_UNIDAD"),
            "H25":( "R_NOMENCLATURA_DOMICILIARIA", "R_NOMENCLATURA_DOMICILIARIA"),
            "H26":( "R_NOMENCLATURACLATURA_VIAL", "R_NOMENCLATURACLATURA_VIAL"),
            "H27":( "R_VEREDA_R_SECTOR", "R_VEREDA_R_SECTOR"),
            "H28":( "R_TERRENO_R_VEREDA", "R_TERRENO_R_VEREDA"),
            "H29":( "R_CONSTRUCCION_R_TERRENO", "R_CONSTRUCCION_R_TERRENO"),
            "H30":( "R_UNIDAD_R_CONSTRUCCION", "R_UNIDAD_R_CONSTRUCCION"),
            "H31":( "R_UNIDAD_R_TERRENO", "R_UNIDAD_R_TERRENO"),
            "H32":( "R_NOMENCLATURA_DOMICILIARIA_R_TERRENO","R_NOMENCLATURA_DOMICILIARIA_R_TERRENO")

        }
    }

    try:
        # Encontrar la raíz del proyecto
        raiz_proyecto = encontrar_raiz_proyecto()
        print(f"Raíz del proyecto encontrada: {raiz_proyecto}")
        
        # Obtener datasets activos
        datasets_to_process = get_active_datasets(raiz_proyecto)
        
        # Crear directorio de inconsistencias
        dir_inconsistencias = crear_directorio_inconsistencias(raiz_proyecto)
        print(f"Directorio de inconsistencias creado: {dir_inconsistencias}")
        
        # Procesar cada dataset
        for dataset in datasets_to_process:
            if dataset in MAPEO_CELDAS:
                print(f"\nProcesando dataset: {dataset}")
                procesar_dataset(raiz_proyecto, dataset, MAPEO_CELDAS)
            else:
                print(f"No hay mapeo definido para el dataset: {dataset}")
                
    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")

if __name__ == "__main__":
    main()