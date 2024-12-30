from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QScrollArea, QWidget, 
                             QPushButton, QCheckBox, QMessageBox, QGroupBox, QLabel,QScrollBar)
from PySide6.QtCore import Qt
import os
import shutil
import sys
sys.stdout.reconfigure(encoding='utf-8')
class ProcessSelectorDialog(QDialog):
    def __init__(self, parent=None, process_manager=None):
        super().__init__(parent)
        self.process_manager = process_manager
        self.parent = parent
        self.selected_processes = []
        self.checkboxes = {}
        self.setWindowTitle("Selector de Procesos")
        self.setMinimumWidth(500)
        self.setMinimumHeight(600)
        self.setup_ui()
               
    def check_and_clean_directories(self):
        """Verifica y limpia el directorio correspondiente según el modelo"""
        # Solo verificamos si el primer script está entre los seleccionados
        needs_cleaning = "01_Copiar_Archivos_necesarios.py" in self.selected_processes
        
        if not needs_cleaning:
            # Si no está seleccionado el primer script, no necesitamos limpiar
            return True
                
        model_type = str(type(self.parent)).lower()
        
        # Determinar el directorio correcto según el modelo
        if "ladm_10_tab" in model_type:
            model_dir = "MODELO_LADM_1_0"
        elif "ladm_12_tab" in model_type:
            model_dir = "MODELO_LADM_1_2"
        elif "interno_tab" in model_type:
            model_dir = "MODELO_INTERNO_1_0"
        else:
            model_dir = "MODELO_IGAC"
                
        target_dir = os.path.join(self.parent.project_root, "Files", "Temporary_Files", model_dir)
        
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
            return True
        
        if any(os.scandir(target_dir)):
            response = QMessageBox.warning(
                self,
                "Archivos existentes",
                f"Se eliminarán todos los archivos dentro de {model_dir} para iniciar este proceso. ¿Desea Continuar?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if response == QMessageBox.No:
                return False
            
            try:
                for item in os.listdir(target_dir):
                    item_path = os.path.join(target_dir, item)
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                self.parent.add_log(f"Contenido de {model_dir} eliminado exitosamente")
                return True
            
            except Exception as e:
                QMessageBox.critical(
                    self,
                    "Error",
                    f"Error al limpiar el directorio: {str(e)}"
                )
                return False
        
        return True

    def accept(self):
        """Sobrescribe el método accept para asegurar que se guarden las selecciones"""
        # Asegurarnos de que tenemos las selecciones antes de cerrar
        if not hasattr(self, 'selected_processes') or not self.selected_processes:
            selected = []
            for filename, checkbox in self.checkboxes.items():
                try:
                    if checkbox and checkbox.isChecked():
                        selected.append(filename)
                except:
                    continue
            self.selected_processes = selected

        # Proceder con el accept normal
        super().accept()

    def process_selected(self):
        """Procesa los checkboxes seleccionados"""
        try:
            selected = []
            for filename, checkbox in self.checkboxes.items():
                if checkbox.isChecked():
                    selected.append(filename)
            
            if not selected:
                QMessageBox.warning(self, "Advertencia", "Por favor seleccione al menos un proceso.")
                return
                    
            # Guardar las selecciones y cerrar el diálogo
            self.selected_processes = selected
            QDialog.accept(self)
            
        except Exception as e:
            print(f"Error en process_selected: {str(e)}")
            QMessageBox.critical(self, "Error", "Error al procesar la selección.")   
        
    def setup_ui(self):
        """Configura la interfaz del selector de procesos"""
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        
        # Header
        header_layout = QHBoxLayout()
        title_label = QLabel("Selección de Procesos")
        title_label.setStyleSheet("font-weight: bold; font-size: 12px;")
        
        clear_btn = QPushButton("Limpiar Datos")
        clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #e74c3c;
                color: white;
                padding: 5px 10px;
                border-radius: 4px;
            }
        """)
        clear_btn.clicked.connect(self.clear_data)
        
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        header_layout.addWidget(clear_btn)
        layout.addLayout(header_layout)
        
        # Determinar el modelo y los procesos
        model_type = str(type(self.parent)).lower()  # Aquí está el cambio
        
        if "ladm_10_tab" in model_type:
            self.scripts_dir = os.path.join(self.parent.project_root, "Scripts", "MODELO_LADM_1_0")
            self.processes =  [
                ("01_Copiar_Archivos_necesarios.py", "1. Copiar Archivos necesarios", False),
                ("02_convertir_gpkg_a_gdb.py", "2. Convertir GPKG (Output) a GDB", False),
                ("03_Procesar_Conteo_de_Elementos.py", "3. Procesar Conteo de Elementos", False),
                ("04_Toolbox_validaciones.py", "4. Toolbox Validaciones", False),
                ("05_Habilitar_column_excepciones.py", "5. Habilitar Columnas de Excepciones", True),
                ("06_Generar_DB_registro_Errores.py", "6. Generar DB registro Errores", False),
                ("07_Diligenciar_Err._y_Excep._a_Excel.py", "7. Diligenciar Err. y Excep. a Excel", False),
                ("08_Encabecado_Formato_Consitencia_Logica.py", "8. Encabecado Formato Consitencia Logica", False),
                ("09_Reportes_Finales.py", "9. Reportes Finales", False),
                ("10_Compilación_Datos.py", "10. Compilación Datos", False)
            ]
        elif "ladm_12_tab" in str(type(self.parent)).lower():
            self.scripts_dir = os.path.join(self.parent.project_root, "Scripts", "MODELO_LADM_1_2")
            self.processes = [
                ("01_Copiar_Archivos_necesarios.py", "1. Copiar Archivos necesarios", False),
                ("02_convertir_gpkg_a_gdb.py", "2. Convertir GPKG (Output) a GDB", False),
                ("03_Procesar_Conteo_de_Elementos.py", "3. Procesar Conteo de Elementos", False),
                ("04_Toolbox_validaciones.py", "4. Toolbox Validaciones", False),
                ("05_Habilitar_column_excepciones.py", "5. Habilitar Columnas de Excepciones", True),
                ("06_Generar_DB_registro_Errores.py", "6. Generar DB registro Errores", False),
                ("07_Diligenciar_Err._y_Excep._a_Excel.py", "7. Diligenciar Err. y Excep. a Excel", False),
                ("08_Encabecado_Formato_Consitencia_Logica.py", "8. Encabecado Formato Consitencia Logica", False),
                ("09_Reportes_Finales.py", "9. Reportes Finales", False),
                ("10_Compilación_Datos.py", "10. Compilación Datos", False)
            ]
        elif "interno_tab" in str(type(self.parent)).lower():
            self.scripts_dir = os.path.join(self.parent.project_root, "Scripts", "MODELO_INTERNO_1_0")
            self.processes = [
                ("01_Copiar_Archivos_necesarios.py", "1. Copiar Archivos necesarios", False),
                ("02_convertir_gpkg_a_gdb.py", "2. Convertir GPKG (Output) a GDB", False),
                ("03_Procesar_Conteo_de_Elementos.py", "3. Procesar Conteo de Elementos", False),
                ("04_Toolbox_validaciones.py", "4. Toolbox Validaciones", False),
                ("05_Habilitar_column_excepciones.py", "5. Habilitar Columnas de Excepciones", True),
                ("06_Generar_DB_registro_Errores.py", "6. Generar DB registro Errores", False),
                ("07_Diligenciar_Err._y_Excep._a_Excel.py", "7. Diligenciar Err. y Excep. a Excel", False),
                ("08_Encabecado_Formato_Consitencia_Logica.py", "8. Encabecado Formato Consitencia Logica", False),
                ("09_Reportes_Finales.py", "9. Reportes Finales", False),
                ("10_Compilación_Datos.py", "10. Compilación Datos", False)
            ]
        else:  # CICA/CONSERVACION por defecto
            self.scripts_dir = os.path.join(self.parent.project_root, "Scripts", "Modelo_IGAC")
            self.processes = [
                ("01_Copiar_Archivos_necesarios.py", "1. Copiar Archivos necesarios", False),
                ("02_Procesar_Conteo_de_Elementos.py", "2. Procesar Conteo de Elementos", False),
                ("03_Crear_Topologías.py", "3. Crear Topologías", False),
                ("04_Aplicar_Reglas_Topologicas.py", "4. Aplicar Reglas Topologicas", False),
                ("05_Exportar_Erro.Topológicos_a_SHP_1_2.py", "5. Exportar Erro.Topológicos a SHP 1/2", False),
                ("06_Exportar_Erro.Topológicos_a_SHP_2_2.py", "6. Exportar Erro.Topológicos a SHP 2/2", True),
                ("07_Generar_DB_registro_Errores.py", "7. Generar DB registro Errores", False),  
                ("08_Exportar_Err.Topologicos_segun_reglas_a_SHP.py", "8. Exportar Err.Topologicos segun reglas a SHP", False),
                ("09_Diligenciar_Err._y_Excep._a_Excel.py", "9. Diligenciar Err. y Excep. a Excel", False),
                ("10_Encabecado_Formato_Consitencia_Logica.py", "10. Encabecado Formato Consitencia Logica", False),
                ("11_Toolbox_Consistencia_Formato.py", "11. Toolbox Consistencia Formato", False),
                ("12_Toolbox_Interseccion_Consistencia.py", "12. Toolbox Intersecciones Consistencia Formato", False),
                ("13_Generar_shp_Consistencia_Formato.py", "13. Generar SHP Consistencia Formato", False),
                ("14_Generar_DB_registro_Errores_Consistencia.py", "14. Generar DB registro Errores Consistencia", False),
                ("15_Diligenciar_Err._Consitencia_a_Excel.py", "15. Diligenciar Err. Consitencia a Excel", True),
                ("16_Generar_DB_registro_Excepciones_Consistencia.py", "16. Generar DB registro Excepciones Consistencia", False),
                ("17_Diligenciar_Excepciones_Consitencia_a_Excel.py", "17. Diligenciar Excepciones Consitencia a Excel", False),
                ("18_Toolbox_Omision_Comision.py", "18. Toolbox Omision-Comision", False),
                ("19_Conteo_Duplicados.py", "19. Conteo Poligonos Duplicados", False),
                ("20_Análisis_Omision_Comision.py", "20. Análisis Omisión Comisión", False),
                ("21_Reportes_Finales.py", "21. Reportes Finales", False),
                ("22_Compilación_Datos.py", "22. Compilación Datos", False)
            ]
        
        # Crear checkboxes y continuar con el resto de la configuración UI
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        self.checkboxes = {}
        for filename, display_name, should_color in self.processes:
            checkbox = QCheckBox(display_name)
            checkbox.setProperty('filename', filename)
            if should_color:
                checkbox.setStyleSheet("QCheckBox { color: #FF1493; font-weight: bold; }")
            self.checkboxes[filename] = checkbox
            scroll_layout.addWidget(checkbox)
        
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)
        
        # Botones de control
        select_all_btn = QPushButton("Seleccionar Todos")
        select_all_btn.clicked.connect(self.select_all)
        layout.addWidget(select_all_btn)
        
        deselect_all_btn = QPushButton("Deseleccionar Todos")
        deselect_all_btn.clicked.connect(self.deselect_all)
        layout.addWidget(deselect_all_btn)
        
        process_btn = QPushButton("Procesar Seleccionados")
        process_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
                padding: 10px;
                border-radius: 5px;
            }
        """)
        process_btn.clicked.connect(self.process_selected)
        layout.addWidget(process_btn)
    
    def clear_data(self):
        """Limpia todas las selecciones"""
        try:
            reply = QMessageBox.question(
                self,
                'Confirmar Limpieza',
                '¿Está seguro de que desea limpiar todas las selecciones?',
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                # Desmarcar todos los checkboxes
                for checkbox in self.checkboxes.values():
                    checkbox.setChecked(False)
                self.selected_processes = []
                
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"Error al limpiar las selecciones: {str(e)}"
            )
    def select_all(self):
        for checkbox in self.checkboxes.values():
            checkbox.setChecked(True)
            
    def deselect_all(self):
        for checkbox in self.checkboxes.values():
            checkbox.setChecked(False)
            
