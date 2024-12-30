from PySide6.QtWidgets import (QDialog, QVBoxLayout, QScrollArea, QWidget, 
                             QPushButton, QCheckBox, QMessageBox, QGroupBox)
from PySide6.QtCore import Qt
import os
import shutil


class ProcessSelectorDialog(QDialog):
    def __init__(self, parent=None, process_manager=None):
        super().__init__(parent)
        self.process_manager = process_manager
        self.parent = parent
        self.scripts_dir = os.path.join(parent.project_root, "Scripts", "Modelo_IGAC")
        self.selected_processes = []
        self.setWindowTitle("Selector de Procesos")
        self.setMinimumWidth(500)
        self.setMinimumHeight(600)
        self.setup_ui()
        
    def check_and_clean_directories(self):
        """Verifica y limpia SOLO el directorio MODELO_IGAC si se selecciona el primer script"""
        if "01_Copiar_Archivos_necesarios.py" not in self.selected_processes:
            return True
            
        modelo_igac_dir = os.path.join(self.parent.project_root, "Files", "Temporary_Files", "MODELO_IGAC")
        
        # Verificar si el directorio MODELO_IGAC existe
        if not os.path.exists(modelo_igac_dir):
            os.makedirs(modelo_igac_dir)
            return True
            
        # Verificar si el directorio tiene contenido
        if any(os.scandir(modelo_igac_dir)):
            response = QMessageBox.warning(
                self,
                "Archivos existentes",
                "Se eliminarán todos los archivos dentro de MODELO_IGAC para iniciar este proceso. ¿Desea Continuar?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if response == QMessageBox.No:
                return False
                
            try:
                # Eliminar SOLO el contenido dentro de MODELO_IGAC
                for item in os.listdir(modelo_igac_dir):
                    item_path = os.path.join(modelo_igac_dir, item)
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                print(f"Contenido de {modelo_igac_dir} eliminado exitosamente")
                return True
                
            except PermissionError:
                QMessageBox.critical(
                    self,
                    "Error",
                    "No se puede eliminar el contenido porque algunos archivos están en uso.\n"
                    "Cierre cualquier programa que pueda estar usando estos archivos e intente nuevamente."
                )
                return False
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al limpiar el directorio: {str(e)}")
                return False
        
        return True

    def process_selected(self):
        self.selected_processes = []
        for filename, checkbox in self.checkboxes.items():
            if checkbox.isChecked():
                self.selected_processes.append(filename)
                
        if not self.selected_processes:
            QMessageBox.warning(self, "Advertencia", "Por favor seleccione al menos un proceso.")
            return
            
        # Verificar y limpiar directorios solo si es necesario
        if self.check_and_clean_directories():
            self.accept()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Crear área de scroll para los checkboxes
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        # Lista de procesos con marcadores para colorear
        self.processes = [
            ("01_Copiar_Archivos_necesarios.py", "1. Copiar Archivos necesarios", False),
            ("02_Procesar_Conteo_de_Elementos.py", "2. Procesar Conteo de Elementos", False),
            ("03_Crear_Topologías.py", "3. Crear Topologías", False),
            ("04_Aplicar_Reglas_Topologicas.py", "4. Aplicar Reglas Topologicas", False),
            ("05_Exportar_Erro.Topológicos_a_SHP_1_2.py", "5. Exportar Erro.Topológicos a SHP 1/2", False),
            ("06_Exportar_Erro.Topológicos_a_SHP_2_2.py", "6. Exportar Erro.Topológicos a SHP 2/2", True),# Marcado para colorear
            ("07_Generar_DB_registro_Errores.py", "7. Generar DB registro Errores", False),  
            ("08_Exportar_Err.Topologicos_segun_reglas_a_SHP.py", "8. Exportar Err.Topologicos segun reglas a SHP", False),
            ("09_Diligenciar_Err._y_Excep._a_Excel.py", "9. Diligenciar Err. y Excep. a Excel", False),
            ("10_Encabecado_Formato_Consitencia_Logica.py", "10. Encabecado Formato Consitencia Logica", False),
            ("11_Toolbox_Consistencia_Formato.py", "11. Toolbox Consistencia Formato", False),
            ("12_Toolbox_Interseccion_Consistencia.py", "12. Toolbox Intersecciones Consistencia Formato", False),
            ("13_Generar_shp_Consistencia_Formato.py", "13. Generar SHP Consistencia Formato", False),
            ("14_Generar_DB_registro_Errores_Consistencia.py", "14. Generar DB registro Errores Consistencia", False),
            ("15_Diligenciar_Err._Consitencia_a_Excel.py", "15. Diligenciar Err. Consitencia a Excel", True),  # Marcado para colorear
            ("16_Generar_DB_registro_Excepciones_Consistencia.py", "16. Generar DB registro Excepciones Consistencia", False),
            ("17_Diligenciar_Excepciones_Consitencia_a_Excel.py", "17. Diligenciar Excepciones Consitencia a Excel", False),
            ("18_Toolbox_Omision_Comision.py", "18. Toolbox Omision-Comision", False),
            ("19_Conteo_Duplicados.py", "19. Conteo Poligonos Duplicados", False),
            ("20_Análisis_Omision_Comision.py", "20. Análisis Omisión Comisión", False),
            ("21_Reportes_Finales.py", "21. Reportes Finales", False),
            ("22_Compilación_Datos.py", "22. Compilación Datos", False)
        ]
        
        # Crear checkboxes
        self.checkboxes = {}
        for filename, display_name, should_color in self.processes:
            checkbox = QCheckBox(display_name)
            if should_color:
                # Aplicar estilo para texto rojo-rosado
                checkbox.setStyleSheet("""
                    QCheckBox {
                        color: #FF1493;  /* Deep Pink */
                        font-weight: bold;
                    }
                    QCheckBox:hover {
                        color: #FF69B4;  /* Hot Pink */
                    }
                """)
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
        
        # Botón de procesamiento
        process_btn = QPushButton("Procesar Seleccionados")
        process_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
                padding: 10px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        process_btn.clicked.connect(self.process_selected)
        layout.addWidget(process_btn)
        
    def select_all(self):
        for checkbox in self.checkboxes.values():
            checkbox.setChecked(True)
            
    def deselect_all(self):
        for checkbox in self.checkboxes.values():
            checkbox.setChecked(False)
            
