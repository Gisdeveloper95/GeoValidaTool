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
from PySide6.QtWidgets import QApplication, QMessageBox, QDialog, QVBoxLayout, QTextEdit, QLabel, QPushButton
from PySide6.QtCore import Qt, QTimer
import sys
import unicodedata

sys.stdout.reconfigure(encoding='utf-8')

def show_error_message(message):
    root = tk.Tk()
    root.withdraw()
    messagebox.showwarning("Advertencia", message)
    root.destroy()

def show_validation_result(title, message, icon=QMessageBox.Information):
    app = QApplication.instance()
    if not app:
        app = QApplication([])
    
    msg_box = QMessageBox()
    msg_box.setWindowTitle(title)
    msg_box.setText(message)
    msg_box.setIcon(icon)
    msg_box.exec()

def verify_file_exists(file_path):
    if not os.path.exists(str(file_path)):
        warning_message = f"¡Advertencia! El archivo no existe: {file_path}. Continuando automáticamente..."
        print(warning_message)
        return False
    return True

def normalize_path(path_str):
    if isinstance(path_str, bytes):
        path_str = path_str.decode('utf-8')
    normalized = unicodedata.normalize('NFC', str(path_str))
    return normalized


def get_project_root():
    script_path = Path(__file__).resolve()
    current_path = script_path.parent.parent.parent
    
    required_paths = [
        current_path / "Files" / "Temporary_Files" / "MODELO_LADM_1_0",
        current_path / "Files" / "Temporary_Files" / "array_config.txt",
        current_path / "Files" / "Temporary_Files" / "Ruta_Insumos",
        current_path / "Scripts"
    ]
    
    if all(path.exists() for path in required_paths):
        print(f"Raíz del proyecto encontrada en: {current_path}")
        return current_path
    
    missing_paths = [path for path in required_paths if not path.exists()]
    error_message = (
        "No se encontró la estructura correcta del proyecto.\n"
        f"Script ubicado en: {script_path}\n"
        f"Buscando raíz en: {current_path}\n"
        "Rutas faltantes:\n"
        + "\n".join(f"- {path}" for path in missing_paths)
    )
    raise Exception(error_message)

 
def copy_gpkg(source, destination, gpkg_type):
    if not verify_file_exists(source):
        return False
        
    try:
        source_str = normalize_path(str(source))
        destination_str = normalize_path(str(destination))
        
        if os.path.exists(destination_str):
            try:
                shutil.rmtree(destination_str)
            except:
                pass
        
        try:
            shutil.copy2(source_str, destination_str)
        except OSError as e:
            if "Permission denied" in str(e):
                show_validation_result(
                    "⚠️ GPKG EN USO",
                    "¡CIERRE ARCGIS/QGIS ANTES DE CONTINUAR!\n\n"
                    "El archivo está siendo usado por otro programa!.\n\n"
                    "1. Cierre TODOS los programas SIG (ArcGIS, QGIS)\n"
                    "2. Vuelva a ejecutar este proceso nuevamente\n"
                    "3. Si el proceso continua con la segunda Etapa Cancela el proceso, \n\n"
                    "🔶Cierra y Abre nuevamente GeoValidaTool\n\n"
                    "❌ Nota: Continuar el proceso Acarreara Errores.",
                    QMessageBox.Warning
                )
                return False
            else:
                raise
        
        return analyze_and_validate_gpkg(destination_str, gpkg_type)
            
    except Exception as e:
        show_validation_result(
            "⚠️ Error en GPKG",
            f"Error al procesar el GeoPackage {gpkg_type}: {str(e)}",
            QMessageBox.Warning
        )
        return False
    
def process_template_gdb(root_path, gpkg_name=None):
    template_zip = root_path / 'Files' / 'Templates' / 'MODELO_LADM_1_0' / 'GDB' / 'Template.gdb.zip'
    temp_dir = root_path / 'Files' / 'Temporary_Files' / 'MODELO_LADM_1_0'
    
    os.makedirs(temp_dir, exist_ok=True)
    
    with zipfile.ZipFile(template_zip, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
        
    old_gdb = temp_dir / 'Template.gdb'
    new_gdb = temp_dir / f"{gpkg_name.replace('.gpkg', '.gdb')}" if gpkg_name else old_gdb
    
    if old_gdb != new_gdb:
        if os.path.exists(new_gdb):
            shutil.rmtree(new_gdb)
        os.rename(old_gdb, new_gdb)

def analyze_and_validate_gpkg(gpkg_path, gpkg_type):
    try:
        root = get_project_root()
        json_filename = f"gpkg_reference_ladm_1_0_{gpkg_type}.json"
        reference_json = root / "Scripts" / "MODELO_LADM_1_0" / "reference" / json_filename
        
        validation_results = {
            'missing_layers': [],
            'field_issues': {}
        }
        
        arcpy.env.workspace = str(gpkg_path)
        existing_layers = arcpy.ListFeatureClasses()
        if not existing_layers:  # Si no hay capas, es un error
            validation_results['missing_layers'].append("No se encontraron capas en el archivo")
            has_errors = True
        else:
            existing_layers = [layer.split('.')[-1] if '.' in layer else layer for layer in existing_layers]
            
            with open(reference_json, 'r') as f:
                reference_structure = json.load(f)
            
            for layer_name, expected_structure in reference_structure.items():
                clean_layer_name = layer_name.split('.')[-1] if '.' in layer_name else layer_name
                
                if clean_layer_name not in existing_layers:
                    validation_results['missing_layers'].append(clean_layer_name)
                    continue
                
                actual_fields = {field.name for field in arcpy.ListFields(os.path.join(str(gpkg_path), clean_layer_name))}
                field_issues = []
                
                for field_name in expected_structure['fields'].keys():
                    if field_name.upper() in ['GLOBALID', 'GLOBALID_SNC', 'FECHA_LOG', 'USUARIO_LOG']:
                        continue
                    if field_name not in actual_fields:
                        field_issues.append(f"Falta el campo: {field_name}")
                
                if field_issues:
                    validation_results['field_issues'][clean_layer_name] = field_issues
        
        has_errors = bool(validation_results['missing_layers']) or bool(validation_results['field_issues'])

        dialog = QDialog()
        dialog.setWindowTitle(f"Validación de Estructura GeoPackage {gpkg_type.upper()}")
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
        
        if has_errors:
            verdict_text = f"""
                ❌ ERROR CRÍTICO DE ESTRUCTURA EN GEOPACKAGE {gpkg_type.upper()}
                
                ¿ESTAS USANDO EL ARCHIVO GPKG CORRECTO?
                
                -Verifica que guardaste el archivo en la ruta correcta
                
                Faltan capas o campos REQUERIDOS en el geopackage.
                Los procesos siguientes FALLARÁN.
                Corrija los errores antes de continuar.
                
                Archivo analizado: {os.path.basename(gpkg_path)}
            """
            verdict_label.setStyleSheet(verdict_label.styleSheet() + """
                background-color: #fee2e2;
                color: #991b1b;
                border: 2px solid #dc2626;
            """)
            
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
            
            if validation_results['missing_layers']:
                error_text += "Capas Faltantes:\n"
                for layer in validation_results['missing_layers']:
                    error_text += f"❌ {layer}\n"
                error_text += "\n"
            
            if validation_results['field_issues']:
                error_text += "Campos Faltantes:\n"
                for layer, issues in validation_results['field_issues'].items():
                    error_text += f"⚠️ {layer}:\n"
                    for issue in issues:
                        error_text += f"   - {issue}\n"
            
            details.setText(error_text)
            layout.addWidget(details)
        else:
            verdict_text = f"""
                ✅ ESTRUCTURA CORRECTA - GEOPACKAGE {gpkg_type.upper()}
                
                El geopackage cumple con la estructura requerida.
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
        
        return not has_errors

    except Exception as e:
        show_validation_result(
            "Error",
            f"Error durante la validación del GeoPackage {gpkg_type}: {str(e)}",
            QMessageBox.Critical
        )
        return False

def main():
    print("EJECUTANDO MODELO LADM COL 1.0")
    try:
        app = QApplication.instance()
        if not app:
            app = QApplication(sys.argv)
            
        root = get_project_root()
        temp_files = root / 'Files' / 'Temporary_Files' / 'MODELO_LADM_1_0'
        temp_files_config = root / 'Files' / 'Temporary_Files'
        json_path = temp_files_config / 'Ruta_Insumos' / 'rutas_archivos_ladm_1_0.json'
        
        with open(str(json_path), 'r', encoding='utf-8') as f:
            rutas = json.load(f)

        if 'gpkg_original' in rutas and 'gpkg_modified' in rutas:
            gpkg_original_name = Path(normalize_path(rutas['gpkg_original'])).name
            gpkg_modified_name = Path(normalize_path(rutas['gpkg_modified'])).name
            original_dir = temp_files / 'GPKG_ORIGINAL'
            os.makedirs(original_dir, exist_ok=True)
            
            original_valid = analyze_and_validate_gpkg(rutas['gpkg_original'], 'original')
            modified_valid = analyze_and_validate_gpkg(rutas['gpkg_modified'], 'modificado')
            
            if not original_valid or not modified_valid:
                sys.exit(1)

            gpkg_original_dest = original_dir / gpkg_original_name
            gpkg_modified_dest = temp_files / gpkg_modified_name

            shutil.copy2(rutas['gpkg_original'], gpkg_original_dest)
            process_template_gdb(root, gpkg_modified_name)
            shutil.copy2(rutas['gpkg_modified'], gpkg_modified_dest)
        else:
            process_template_gdb(root)

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()