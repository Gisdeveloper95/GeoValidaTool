import os
import json
import shutil
import zipfile
from pathlib import Path
import warnings
import time
import tkinter as tk
from tkinter import messagebox
import arcpy
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtCore import QTimer

import sys
import unicodedata

sys.stdout.reconfigure(encoding='utf-8')
def show_error_message(message):
    """Muestra una ventana de alerta con mensaje de error"""
    root = tk.Tk()
    root.withdraw()  # Oculta la ventana principal
    messagebox.showwarning("Advertencia", message)
    root.destroy()

def show_validation_result(title, message, icon=QMessageBox.Information):
    """Muestra un mensaje de validación usando PySide6"""
    app = QApplication.instance()
    if not app:
        app = QApplication([])
    
    msg_box = QMessageBox()
    msg_box.setWindowTitle(title)
    msg_box.setText(message)
    msg_box.setIcon(icon)
    msg_box.exec()

def get_project_root():
    """
    Encuentra la raíz del proyecto basándose en la ubicación del script actual
    y verificando la estructura de directorios esperada.
    """
    # Obtener la ruta absoluta del script actual
    script_path = Path(__file__).resolve()
    
    # El script está en Scripts/Modelo_IGAC/, así que subimos dos niveles
    # para llegar a la raíz del proyecto
    current_path = script_path.parent.parent.parent
    
    # Verifica si la estructura requerida existe en esta ubicación
    required_paths = [
        current_path / "Files" / "Temporary_Files" / "MODELO_IGAC",
        current_path / "Files" / "Temporary_Files" / "array_config.txt",
        current_path / "Files" / "Temporary_Files" / "Ruta_Insumos",
        current_path / "Scripts"
    ]
    
    if all(path.exists() for path in required_paths):
        print(f"Raíz del proyecto encontrada en: {current_path}")
        return current_path
    
    # Si no se encuentra la estructura, mostrar un mensaje de error más detallado
    missing_paths = [path for path in required_paths if not path.exists()]
    error_message = (
        "No se encontró la estructura correcta del proyecto.\n"
        f"Script ubicado en: {script_path}\n"
        f"Buscando raíz en: {current_path}\n"
        "Rutas faltantes:\n"
        + "\n".join(f"- {path}" for path in missing_paths)
    )
    raise Exception(error_message)

def verify_file_exists(file_path):
    """Verifica si un archivo existe y muestra una advertencia si no"""
    if not os.path.exists(str(file_path)):
        warning_message = f"¡Advertencia! El archivo no existe: {file_path}. Continuando automáticamente..."
        print(warning_message)
        return False
    return True

def create_or_clean_directory(directory):
    """Crea un directorio o lo limpia si ya existe"""
    if os.path.exists(str(directory)):
        print(f"Limpiando directorio existente: {directory}")
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                shutil.rmtree(str(directory))
                break
            except PermissionError:
                if attempt < max_attempts - 1:
                    print(f"Intento {attempt + 1} fallido, esperando...")
                    time.sleep(2)
                else:
                    show_error_message(f"No se puede eliminar el directorio {directory} porque está siendo usado por otro proceso.")
                    raise
    os.makedirs(str(directory))
    print(f"Directorio creado: {directory}")

def load_active_datasets():
    """Carga los datasets activos desde el archivo de configuración"""
    try:
        root = get_project_root()
        config_path = root / "Files" / "Temporary_Files" / "array_config.txt"
        
        active_datasets = []
        with open(config_path, 'r') as f:
            for line in f:
                # Limpiamos la línea de cualquier carácter especial
                line = line.strip()
                line = line.strip('[]",\n ')  # Eliminar corchetes, comillas, comas y saltos de línea
                
                # Solo procesar líneas no vacías y no comentadas
                if line and not line.startswith('#'):
                    dataset_name = line.strip()  # Un strip adicional por si acaso
                    if dataset_name:
                        active_datasets.append(dataset_name)
        
        # Imprimir los datasets activos para verificación
        print("\nDatasets activos encontrados:")
        for ds in active_datasets:
            print(f"  - {ds}")
            
        return {
            "topology": active_datasets,
            "line_topology": [ds for ds in active_datasets if ds in ["URBANO_CTM12", "RURAL_CTM12"]]
        }
    except Exception as e:
        print(f"Error al cargar configuración: {str(e)}")
        return {
            "topology": ["URBANO_CTM12", "RURAL_CTM12"],
            "line_topology": ["URBANO_CTM12", "RURAL_CTM12"]
        }

def analyze_and_validate_gdb(gdb_path):
   """Analiza y valida la estructura de la GDB usando el archivo de referencia JSON"""
   try:
       from PySide6.QtWidgets import QDialog, QVBoxLayout, QTextEdit, QLabel, QPushButton
       from PySide6.QtCore import Qt, QTimer
       import json

       # Definir datasets auxiliares requeridos
       auxiliary_datasets = {
           "URBANO_CTM12": {
               "source_dataset": "ZONA_HOMOGENEA_URBANO_CTM12",
               "feature_classes": [
                   "U_ZONA_HOMO_GEOECONOMICA_CTM12",
                   "U_ZONA_HOMOGENEA_FISICA_CTM12"
               ]
           },
           "RURAL_CTM12": {
               "source_dataset": "ZONA_HOMOGENEA_RURAL_CTM12", 
               "feature_classes": [
                   "R_ZONA_HOMO_GEOECONOMICA_CTM12",
                   "R_ZONA_HOMOGENEA_FISICA_CTM12"
               ]
           },
           "URBANO": {
               "source_dataset": "ZONA_HOMOGENEA_URBANO",
               "feature_classes": [
                   "U_ZONA_HOMOGENEA_GEOECONOMICA",
                   "U_ZONA_HOMOGENEA_FISICA"
               ]
           },
           "RURAL": {
               "source_dataset": "ZONA_HOMOGENEA_RURAL",
               "feature_classes": [
                   "R_ZONA_HOMOGENEA_GEOECONOMICA",
                   "R_ZONA_HOMOGENEA_FISICA"
               ]
           }
       }
       
       IGNORED_FIELDS = ['GLOBALID_SNC', 'FECHA_LOG', 'GLOBALID', 'USUARIO_LOG']
       
       root = get_project_root()
       reference_json = root / "Scripts" / "Modelo_IGAC"/ "reference" / "gdb_structure_reference.json"
       
       with open(reference_json, 'r') as f:
           reference_structure = json.load(f)
       
       validation_results = {
           'missing_required_datasets': [],
           'missing_auxiliary_datasets': [],
           'missing_auxiliary_fcs': [],
           'field_issues': {},
           'type_mismatches': {}
       }
       
       arcpy.env.workspace = str(gdb_path)
       existing_datasets = arcpy.ListDatasets()
       active_datasets = load_active_datasets()["topology"]
       
       # Validar datasets activos y sus auxiliares
       for dataset_name in active_datasets:
           # Validar dataset principal
           if dataset_name not in existing_datasets:
               validation_results['missing_required_datasets'].append(dataset_name)
               continue
               
           # Validar si requiere dataset auxiliar
           if dataset_name in auxiliary_datasets:
               aux_info = auxiliary_datasets[dataset_name]
               aux_dataset = aux_info["source_dataset"]
               
               if aux_dataset not in existing_datasets:
                   validation_results['missing_auxiliary_datasets'].append({
                       'main': dataset_name,
                       'auxiliary': aux_dataset
                   })
               else:
                   # Validar feature classes auxiliares
                   aux_path = os.path.join(str(gdb_path), aux_dataset)
                   arcpy.env.workspace = aux_path
                   existing_fcs = arcpy.ListFeatureClasses()
                   
                   for required_fc in aux_info["feature_classes"]:
                       if required_fc not in existing_fcs:
                           validation_results['missing_auxiliary_fcs'].append({
                               'dataset': aux_dataset,
                               'fc': required_fc
                           })
           
           # Validar estructura del dataset principal
           if dataset_name in reference_structure:
               dataset_path = os.path.join(str(gdb_path), dataset_name)
               arcpy.env.workspace = dataset_path
               existing_fcs = arcpy.ListFeatureClasses()
               
               for fc_name, expected_structure in reference_structure[dataset_name].items():
                   fc_path = os.path.join(dataset_path, fc_name)
                   
                   if not arcpy.Exists(fc_path):
                       validation_results['missing_layers'].append(f"{dataset_name}/{fc_name}")
                       continue
                   
                   # Validar campos
                   actual_fields = {field.name: {
                       'type': field.type,
                       'length': field.length,
                       'precision': field.precision,
                       'scale': field.scale
                   } for field in arcpy.ListFields(fc_path)}
                   
                   field_issues = []
                   type_issues = []
                   
                   for field_name, expected_field in expected_structure['fields'].items():
                       if field_name in IGNORED_FIELDS:
                           continue
                           
                       if field_name not in actual_fields:
                           field_issues.append(f"Falta el campo: {field_name}")
                       else:
                           actual_field = actual_fields[field_name]
                           exp_type = expected_field['type']
                           act_type = actual_field['type']
                           
                           if exp_type != act_type:
                               type_issues.append(
                                   f"Campo {field_name}: tipo incorrecto (es {act_type}, debe ser {exp_type})"
                               )
                   
                   if field_issues:
                       validation_results['field_issues'][f"{dataset_name}/{fc_name}"] = field_issues
                   if type_issues:
                       validation_results['type_mismatches'][f"{dataset_name}/{fc_name}"] = type_issues
       
       # Determinar severidad de errores
       has_critical_errors = bool(validation_results['missing_required_datasets'])
       has_auxiliary_errors = bool(validation_results['missing_auxiliary_datasets']) or bool(validation_results['missing_auxiliary_fcs'])
       has_other_issues = any([
           validation_results['field_issues'],
           validation_results['type_mismatches']
       ])

       # Crear ventana de resultado
       dialog = QDialog()
       dialog.setWindowTitle("Validación de Estructura GDB")
       dialog.setMinimumSize(500, 300)
       dialog.setWindowFlags(dialog.windowFlags() | Qt.WindowStaysOnTopHint)
       
       layout = QVBoxLayout()
       
       verdict_label = QLabel()
       verdict_label.setStyleSheet("""
           QLabel {
               padding: 20px;
               border-radius: 8px;
               font-size: 14px;
               font-weight: bold;
               margin: 10px;
           }
       """)
       
       if has_critical_errors:
           verdict_text = """
               ❌ ERROR CRÍTICO DE ESTRUCTURA
               
               Faltan datasets REQUERIDOS en la geodatabase.
               Los procesos siguientes FALLARÁN.
               Corrija los errores antes de continuar.
           """
           verdict_label.setStyleSheet(verdict_label.styleSheet() + """
               background-color: #fee2e2;
               color: #991b1b;
               border: 2px solid #dc2626;
           """)
       elif has_auxiliary_errors:
           verdict_text = """
               ⚠️ ADVERTENCIA: FALTAN DATASETS/FEATURECLASS
               
               Faltan datasets o features class auxiliares necesarios para analisis Topologico.
               El proceso puede continuar pero GENERARÁ ERRORES en algunas funcionalidades.
               Se recomienda corregir antes de continuar.
           """
           verdict_label.setStyleSheet(verdict_label.styleSheet() + """
               background-color: #fef3c7;
               color: #92400e;
               border: 2px solid #d97706;
           """)
       elif has_other_issues:
           verdict_text = """
               ⚠️ ADVERTENCIA: PROBLEMAS DE ESTRUCTURA
               
               Existen problemas con campos o tipos de datos.
               El proceso puede continuar pero podrían surgir errores.
               Se recomienda revisar los detalles.
           """
           verdict_label.setStyleSheet(verdict_label.styleSheet() + """
               background-color: #fef3c7;
               color: #92400e;
               border: 2px solid #d97706;
           """)
       else:
           verdict_text = """
               ✅ ESTRUCTURA CORRECTA
               
               La geodatabase cumple con la estructura requerida.
               Puede continuar con los procesos siguientes.
               
               Esta ventana se cerrará automáticamente en 10 segundos...
           """
           verdict_label.setStyleSheet(verdict_label.styleSheet() + """
               background-color: #dcfce7;
               color: #166534;
               border: 2px solid #22c55e;
           """)
           timer = QTimer()
           timer.setSingleShot(True)
           timer.timeout.connect(dialog.accept)
           timer.start(10000)
           
       verdict_label.setText(verdict_text)
       verdict_label.setAlignment(Qt.AlignCenter)
       layout.addWidget(verdict_label)
       
       if has_critical_errors or has_auxiliary_errors or has_other_issues:
           details = QTextEdit()
           details.setReadOnly(True)
           details.setStyleSheet("""
               QTextEdit {
                   font-family: 'Consolas', 'Courier New', monospace;
                   font-size: 12px;
                   background-color: #f8f9fa;
                   border: 1px solid #dee2e6;
                   border-radius: 4px;
                   margin: 10px;
                   padding: 10px;
               }
           """)
           
           error_text = "ERRORES ENCONTRADOS:\n\n"
           
           if validation_results['missing_required_datasets']:
               error_text += "Datasets REQUERIDOS Faltantes:\n"
               for dataset in validation_results['missing_required_datasets']:
                   error_text += f"❌ {dataset}\n"
               error_text += "\n"
           
           if validation_results['missing_auxiliary_datasets']:
               error_text += "Datasets Auxiliares Faltantes:\n"
               for missing in validation_results['missing_auxiliary_datasets']:
                   error_text += f"⚠️ Falta {missing['auxiliary']} (requerido por {missing['main']})\n"
               error_text += "\n"
               
           if validation_results['missing_auxiliary_fcs']:
               error_text += "Feature Classes Auxiliares Faltantes:\n"
               for missing in validation_results['missing_auxiliary_fcs']:
                   error_text += f"⚠️ Falta {missing['fc']} en dataset {missing['dataset']}\n"
               error_text += "\n"
           
           if validation_results['field_issues']:
               error_text += "Campos Faltantes:\n"
               for fc, issues in validation_results['field_issues'].items():
                   error_text += f"⚠️ {fc}:\n"
                   for issue in issues:
                       error_text += f"   - {issue}\n"
               error_text += "\n"
           
           if validation_results['type_mismatches']:
               error_text += "Tipos de Campo Incorrectos:\n"
               for fc, issues in validation_results['type_mismatches'].items():
                   error_text += f"⚠️ {fc}:\n"
                   for issue in issues:
                       error_text += f"   - {issue}\n"
           
           details.setText(error_text)
           layout.addWidget(details)
       
       continue_button = QPushButton("Continuar")
       continue_button.setStyleSheet("""
           QPushButton {
               background-color: #0284c7;
               color: white;
               padding: 10px 20px;
               border: none;
               border-radius: 5px;
               font-size: 13px;
               font-weight: bold;
               min-width: 120px;
               margin: 10px;
           }
           QPushButton:hover {
               background-color: #0369a1;
           }
           QPushButton:pressed {
               background-color: #075985;
           }
       """)
       continue_button.clicked.connect(dialog.accept)
       layout.addWidget(continue_button, alignment=Qt.AlignCenter)
       
       dialog.setLayout(layout)
       result = dialog.exec()
       
       return not (has_critical_errors or has_auxiliary_errors or has_other_issues)
           
   except Exception as e:
       show_validation_result(
           "Error",
           f"Error durante la validación: {str(e)}",
           QMessageBox.Critical
       )
       return False


def delete_reports_folder(root_path):
    """Intenta borrar la carpeta Reportes y muestra una alerta si no es posible"""
    reports_folder = root_path / 'Reportes'
    if os.path.exists(str(reports_folder)):
        try:
            shutil.rmtree(str(reports_folder))
            print("Carpeta Reportes eliminada exitosamente")
        except PermissionError:
            show_error_message("La carpeta Reportes no se puede eliminar porque está siendo usada por otro proceso.")
            raise
        except Exception as e:
            show_error_message(f"Error al eliminar la carpeta Reportes: {str(e)}")
            raise


def normalize_path(path_str):
    """
    Normaliza una ruta para manejar caracteres especiales.
    """
    # Asegurarse de que la ruta está en formato unicode
    if isinstance(path_str, bytes):
        path_str = path_str.decode('utf-8')
    
    # Normalizar caracteres Unicode (NFC para compatibilidad con Windows)
    normalized = unicodedata.normalize('NFC', str(path_str))
    return normalized

def copy_file_or_directory(source, destination):
    """Copia un archivo o directorio de origen a destino con soporte para caracteres especiales"""
    if not verify_file_exists(source):
        return
    
    try:
        source_str = normalize_path(str(source))
        destination_str = normalize_path(str(destination))
        
        print(f"Copiando de: {source_str}")
        print(f"Hacia: {destination_str}")
        
        if source_str.endswith('.gdb'):
            try:
                # Intentar copiar la GDB
                if os.path.exists(destination_str):
                    try:
                        shutil.rmtree(destination_str)
                    except:
                        pass
                
                try:
                    shutil.copytree(source_str, destination_str)
                except OSError as e:
                    if "Permission denied" in str(e):
                        # Aquí es donde capturamos los errores de .lock
                        print("\n¡LA GDB ESTÁ EN USO!")
                        QMessageBox.warning(
                            None,
                            "⚠️ GDB EN USO",
                            "¡CIERRE ARCGIS/QGIS ANTES DE CONTINUAR!\n\n"
                            "La Geodatabase está siendo usada por otro programa!.\n\n"
                            "1. Cierre TODOS los programas SIG (ArcGIS, QGIS)\n"
                            "2. Vuelva a ejecutar este proceso nuevamente\n"
                            "3. Si el proceso continua con la segunda Etapa Cancela el proceso, \n\n🔶Cierra y Abre nuevamente GeoValidaTool "
                            "\n\n ❌ Nota:  Continuar el proceso Acarreara Errores.",
                            QMessageBox.Ok
                        )
                        # No lanzamos la excepción, solo retornamos False
                        return False
                    else:
                        raise
                
                print("Validando estructura de la geodatabase...")
                return analyze_and_validate_gdb(destination_str)
                    
            except Exception as e:
                print(f"\nError al copiar GDB: {str(e)}")
                QMessageBox.warning(
                    None,
                            "⚠️ GDB EN USO",
                            "¡CIERRE ARCGIS/QGIS ANTES DE CONTINUAR!\n\n"
                            "La Geodatabase está siendo usada por otro programa!.\n\n"
                            "1. Cierre TODOS los programas SIG (ArcGIS, QGIS)\n"
                            "2. Vuelva a ejecutar este proceso nuevamente\n"
                            "3. Si el proceso continua con la segunda Etapa Cancela el proceso, \n\n🔶Cierra y Abre nuevamente GeoValidaTool "
                            "\n\n ❌ Nota:  Continuar el proceso Acarreara Errores.",
                            QMessageBox.Ok
                        )
                return False
        else:
            try:
                os.makedirs(os.path.dirname(destination_str), exist_ok=True)
                shutil.copy2(source_str, destination_str)
                return True
            except Exception as e:
                print(f"Error al copiar archivo: {str(e)}")
                return False
                
    except Exception as e:
        print(f"Error general: {str(e)}")
        return False
def main():
    try:
        # Configurar codificación UTF-8 para la consola
        if sys.platform == 'win32':
            sys.stdout.reconfigure(encoding='utf-8')
        
        # Inicializar la aplicación QT
        app = QApplication.instance()
        if not app:
            app = QApplication(sys.argv)
            
        root = get_project_root()
        print(f"Raíz del proyecto encontrada: {root}")

        delete_reports_folder(root)

        temp_files = root / 'Files' / 'Temporary_Files' / 'MODELO_IGAC'
        insumos_dir = temp_files / 'INSUMOS'
        temp_files_config = root / 'Files' / 'Temporary_Files'
        json_path = temp_files_config / 'Ruta_Insumos' / 'rutas_archivos.json'
        
        try:
            with open(str(json_path), 'r', encoding='utf-8') as f:
                rutas = json.load(f)
                print(f"Contenido del JSON cargado: {rutas}")
        except PermissionError:
            show_error_message(f"No se puede abrir el archivo JSON {json_path} porque está siendo usado por otro proceso.")
            raise
        except Exception as e:
            show_error_message(f"Error al leer el archivo JSON {json_path}: {str(e)}")
            raise

        # Crear directorio INSUMOS
        create_or_clean_directory(insumos_dir)

        # Crear los subdirectorios dentro de INSUMOS
        apex_terreno_dir = insumos_dir / 'Apex_Terreno'
        apex_unidad_dir = insumos_dir / 'Apex_Unidad'
        
        # Crear los subdirectorios
        create_or_clean_directory(apex_terreno_dir)
        create_or_clean_directory(apex_unidad_dir)

        # Copiar GDB si existe en las rutas
        if 'gdb' in rutas:
            gdb_dest = temp_files / Path(normalize_path(rutas['gdb'])).name
            copy_file_or_directory(rutas['gdb'], gdb_dest)

        # Copiar archivo para Apex_Unidad
        if 'construccion' in rutas:
            try:
                source_path = Path(normalize_path(rutas['construccion']))
                if not source_path.exists():
                    show_error_message(f"El archivo no existe en la ruta: {source_path}")
                else:
                    csv_dest = apex_unidad_dir / source_path.name
                    print(f"Copiando archivo a Apex_Unidad: {csv_dest}")
                    copy_file_or_directory(source_path, csv_dest)
            except Exception as e:
                show_error_message(f"Error al procesar el archivo para Apex_Unidad: {str(e)}")
        else:
            print("No se encontró la clave 'construccion' en el JSON")

        # Copiar archivo para Apex_Terreno
        if 'predio' in rutas:
            try:
                source_path = Path(normalize_path(rutas['predio']))
                if not source_path.exists():
                    show_error_message(f"El archivo no existe en la ruta: {source_path}")
                else:
                    csv_dest = apex_terreno_dir / source_path.name
                    print(f"Copiando archivo a Apex_Terreno: {csv_dest}")
                    copy_file_or_directory(source_path, csv_dest)
            except Exception as e:
                show_error_message(f"Error al procesar el archivo para Apex_Terreno: {str(e)}")
        else:
            print("No se encontró la clave 'predio' en el JSON")

        print("Proceso completado exitosamente")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()