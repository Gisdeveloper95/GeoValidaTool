from PySide6.QtWidgets import (QPushButton, QLabel, QFrame, QVBoxLayout, 
                              QHBoxLayout, QGridLayout, QMessageBox, QWidget,
                              QScrollArea)
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QIcon, QPixmap
import os
import shutil
from base_tab import BaseModelTab
from utils.process_manager import ProcessManager
from utils.rich_text_edit import RichTextEdit
from utils.status_indicator import StatusIndicator
from utils.config_dialog import ConfigDialog
from utils.zones_dialog import ZonesDialog
from utils.batch_processor import BatchProcessor
from utils.process_selector import ProcessSelectorDialog
from utils.process_manager import ProcessManager

class LADM12ModelTab(BaseModelTab):
    def __init__(self, parent=None, model_name="", scripts_dir=""):
        super().__init__(parent, model_name, scripts_dir)
        
        # Inicializar atributos importantes
        self.process_manager = ProcessManager(self)
        self.has_inputs = False
        self.has_zones = False
        self.process_buttons = []
        self.status_indicators = {}
        self.console = None

        # Contenedor principal
        self.main_container = QFrame()
        self.main_container.setStyleSheet("""
            QFrame {
                background-color: #F5F5DC;
                margin: 0px;
                padding: 0px;
            }
            
            QPushButton {
                color: white;
                border: none;
                padding: 8px 15px;
                border-radius: 4px;
                font-weight: bold;
                min-height: 30px;
            }
            
            QPushButton:hover {
                background-color: #8b4513;
            }
            
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
            
            QLabel {
                color: #4A4A4A;
                padding: 2px;
            }
        """)
        
        self.main_layout.addWidget(self.main_container)
        self.container_layout = QVBoxLayout(self.main_container)
        self.container_layout.setContentsMargins(0, 0, 0, 0)
        self.container_layout.setSpacing(5)

        self.setup_ui()
        self.check_existing_configuration()

    def setup_ui(self):
        """Configura la interfaz principal"""
        # Marco superior que contiene todos los elementos de la parte superior
        top_container = QFrame()
        top_container.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 1px solid #D2B48C;
                border-radius: 4px;
                margin: 5px;
            }
        """)
        top_layout = QHBoxLayout(top_container)
        top_layout.setContentsMargins(15, 10, 15, 10)
        top_layout.setSpacing(15)
        
        # Configurar elementos del header dentro del contenedor superior
        self.setup_header(top_layout)
        
        # Marco inferior para los dos botones de proceso
        bottom_container = QFrame()
        bottom_container.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 1px solid #D2B48C;
                border-radius: 4px;
                margin: 5px;
            }
            
            QPushButton {
                background-color: #2D3E50;
                color: white;
                height: 35px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #34495E;
            }
            QPushButton:pressed {
                background-color: #2C3E50;
            }
            
            QPushButton:disabled {
                background-color: #D4D4D4;
                color: #909090;
            }
        """)
        
        process_layout = QHBoxLayout(bottom_container)
        process_layout.setContentsMargins(15, 10, 15, 10)
        process_layout.setSpacing(15)
        
        # Solo dos botones para el modelo LADM 1.0
        process_names = [
            "1. Estructuración de Datos y Topologia",
            "2. Reportes"
        ]
        
        for i, name in enumerate(process_names):
            btn = QPushButton(name)
            if i == 0:
                btn.clicked.connect(self.run_data_structuring)
            elif i == 1:
                btn.clicked.connect(self.run_reports)
                
            self.process_buttons.append(btn)
            process_layout.addWidget(btn)
        
        # Agregar los contenedores al layout principal
        self.main_layout.addWidget(top_container)
        self.main_layout.addWidget(bottom_container)
        
        # Configurar el resto de la interfaz
        self.setup_status_and_console(self.main_layout)
        self.setup_bottom_controls(self.main_layout)
        
    def setup_bottom_controls(self, parent_layout):
        """Configura los botones de control en la parte inferior"""
        controls_widget = QWidget()
        controls_layout = QHBoxLayout(controls_widget)
        controls_layout.setContentsMargins(10, 5, 10, 5)
        
        clear_btn = QPushButton("Limpiar Consola")
        clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #FF8C00;
                color: white;
                height: 22px;
                margin: 0px 5px;
            }
        """)
        clear_btn.clicked.connect(self.clear_console)
        
        stop_btn = QPushButton("Detener Procesos")
        stop_btn.setStyleSheet("""
            QPushButton {
                background-color: #c41604;
                color: white;
                height: 22px;
                margin: 0px 5px;
            }
        """)
        stop_btn.clicked.connect(self.stop_processes)
        
        controls_layout.addWidget(clear_btn)
        controls_layout.addWidget(stop_btn)
        
        parent_layout.addWidget(controls_widget)
        
    def setup_header(self, parent_layout):
        """Configura los elementos del header"""
        # Frame de configuración
        config_frame = QFrame()
        config_frame.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 1px solid #D2B48C;
                border-radius: 4px;
            }
            
            QPushButton {
                background-color: #804000; 
                color: white;
                padding: 10px;
                border-radius: 5px;
                font-size: 14px;
                border: none;
                height: 17px;
                
            }
            
            QPushButton:hover {
                background-color: #663300;
            }
            
            QPushButton:pressed {
                background-color: #4D2600;
            }
        """)
        
        config_layout = QVBoxLayout(config_frame)
        config_layout.setContentsMargins(10, 10, 10, 10)
        
        config_label = QLabel("Configuración LADM_COL 1.2")  # Cambio de título
        config_label.setFont(QFont("Segoe UI", 14, QFont.Bold))
        config_label.setAlignment(Qt.AlignCenter)
        config_layout.addWidget(config_label)
        
        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        
        self.btn_inputs = QPushButton("Cargar Insumos")
        self.btn_zones = QPushButton("Definir Zonas")
        
        self.btn_inputs.setFixedWidth(180)
        self.btn_zones.setFixedWidth(180)
        
        self.btn_inputs.clicked.connect(self.show_config_dialog)
        self.btn_zones.clicked.connect(self.show_zones_dialog)
        
        button_layout.addWidget(self.btn_inputs)
        button_layout.addWidget(self.btn_zones)
        config_layout.addLayout(button_layout)
        
        # Frame para botones de ejecución
        execution_frame = QFrame()
        execution_frame.setStyleSheet("""
            QPushButton {
                background-color: #2B4C7E;
                color: white;
                height: 34px;
                min-width: 200px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #3A669D;
            }
            QPushButton:pressed {
                background-color: #1D3557;
            }
        """)
        
        execution_layout = QVBoxLayout(execution_frame)
        execution_layout.setContentsMargins(10, 10, 10, 10)
        
        individual_btn = QPushButton("Ejecutar Procesos Individualmente")
        individual_btn.setStyleSheet("""
            QPushButton {
                background-color: #D81B60;
                color: white;
                height: 38px;
                min-width: 220px;
            }
            QPushButton:hover {
                background-color: #C2185B;
            }
            QPushButton:pressed {
                background-color: #AD1457;
            }
        """)
        
        batch_btn = QPushButton("Ejecutar Todos Los Procesos")
        batch_btn.setStyleSheet("""
            QPushButton {
                background-color: #2E7D32;
                color: white;
                height: 38px;
                
            }
            QPushButton:hover {
                background-color: #1B5E20;
            }
            QPushButton:pressed {
                background-color: #1B4D1B;
            }
        """)
        
        individual_btn.clicked.connect(self.show_process_selector)
        batch_btn.clicked.connect(self.execute_all_processes)
        
        execution_layout.addWidget(individual_btn)
        execution_layout.addWidget(batch_btn)
        
        # Agregar los frames al layout padre
        parent_layout.addWidget(config_frame)
        parent_layout.addWidget(execution_frame)
        parent_layout.addStretch()
        
        # Agregar los frames de software y subdirección
        self.setup_software_frame(parent_layout)
        self.setup_subdir_frame(parent_layout)
        
        
    def setup_process_buttons(self):
        """Configura los botones de proceso"""
        process_frame = QFrame()
        process_layout = QHBoxLayout(process_frame)
        process_layout.setContentsMargins(20, 10, 20, 10)
        process_layout.setSpacing(20)  # Espacio entre botones
        
        process_names = [
            "1. Estructuración de Datos y Topologia",
            "2. Reportes"
        ]
        
        for i, name in enumerate(process_names):
            btn = QPushButton(name)
            btn.setStyleSheet("""
                QPushButton {
                    background-color: #a97a59;
                    color: white;
                    font-weight: bold;
                    padding: 10px;
                    border-radius: 4px;
                    min-width: 200px;
                }
                QPushButton:hover {
                    background-color: #a56d5d;
                }
                QPushButton:disabled {
                    background-color: #cccccc;
                    color: #666666;
                }
            """)
            
            if i == 0:
                btn.clicked.connect(self.run_data_structuring)
            elif i == 1:
                btn.clicked.connect(self.run_process_2)
            elif i == 2:
                btn.clicked.connect(self.run_process_3)
                
            self.process_buttons.append(btn)
            process_layout.addWidget(btn)
        
        self.main_layout.addWidget(process_frame)
    
    def create_software_frame(self):
        """Crea el frame de software requerido"""
        software_frame = QFrame()
        software_layout = QVBoxLayout(software_frame)
        
        # Contenedor para imágenes
        small_images_frame = QFrame()
        small_images_layout = QHBoxLayout(small_images_frame)
        small_images_layout.setSpacing(20)
        
        # Agregar imagen de ArcGIS Pro
        arcgis_path = os.path.join(self.project_root, "Scripts", "img", "ArcGIS-Pro.png")
        if os.path.exists(arcgis_path):
            img_label = self.create_image_label(arcgis_path, 40, 40)
            small_images_layout.addWidget(img_label)
        
        software_layout.addWidget(small_images_frame)
        
        # Texto Software Requeridos
        software_title = QLabel("Software Requeridos")
        software_title.setFont(QFont("Segoe UI", 12, QFont.Bold))
        software_title.setAlignment(Qt.AlignCenter)
        software_layout.addWidget(software_title)
        
        return software_frame

    def create_igac_frame(self):
        """Crea el frame de IGAC"""
        igac_frame = QFrame()
        igac_layout = QVBoxLayout(igac_frame)
        
        # Contenedor para imágenes
        igac_images_frame = QFrame()
        igac_images_layout = QHBoxLayout(igac_images_frame)
        igac_images_layout.setSpacing(25)
        
        # Agregar imágenes IGAC y proyectos
        igac_path = os.path.join(self.project_root, "Scripts", "img", "IGAC.png")
        proyectos_path = os.path.join(self.project_root, "Scripts", "img", "proyectos.svg")
        
        if os.path.exists(igac_path):
            igac_label = self.create_image_label(igac_path, 45, 45)
            igac_images_layout.addWidget(igac_label)
            
        if os.path.exists(proyectos_path):
            proyectos_label = self.create_image_label(proyectos_path, 45, 45)
            igac_images_layout.addWidget(proyectos_label)
        
        igac_layout.addWidget(igac_images_frame)
        
        # Texto Subdirección
        text_label = QLabel("Subdirección de Proyectos")
        text_label.setFont(QFont("Segoe UI", 12, QFont.Bold))
        text_label.setAlignment(Qt.AlignCenter)
        text_label.setStyleSheet("font-weight: bold; color: #4A4A4A;")
        igac_layout.addWidget(text_label)
        
        return igac_frame

    def create_image_label(self, image_path, width, height):
        """Crea un QLabel con una imagen escalada"""
        label = QLabel()
        pixmap = QPixmap(image_path)
        if not pixmap.isNull():
            pixmap = pixmap.scaled(width, height, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            label.setPixmap(pixmap)
            label.setFixedSize(width, height)
        return label

    def show_process_selector(self):
        """Muestra el diálogo de selección de procesos"""
        if not self.has_inputs:
            QMessageBox.warning(self, "Error", 
                            "Debe configurar los insumos antes de ejecutar procesos.")
            return
        
        # Crear un diálogo personalizado para el modelo LADM 1.2
        dialog = ProcessSelectorDialog(self, self.process_manager)
        
        # Actualizar la lista de procesos para el modelo LADM 1.2
        dialog.processes = [
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
        
        if dialog.exec():
            if not dialog.selected_processes:
                self.add_log("No se seleccionaron procesos para ejecutar")
                return
                
            self.process_manager.run_selected_processes(dialog.selected_processes)
            
    def show_config_dialog(self):
        """Muestra el diálogo de configuración"""
        from utils.config_dialog_ladm_12 import ConfigDialogLADM12  # CORRECTO
        dialog = ConfigDialogLADM12(self)  # CORRECTO
        if dialog.exec():
            self.has_inputs = True
            self.update_button_states()
            self.add_log("Configuración de insumos LADM 1.2 guardada")

    def show_zones_dialog(self):
        """Muestra el diálogo de configuración de zonas"""
        dialog = ZonesDialog(self)
        if dialog.exec():
            self.has_zones = True
            self.update_button_states()
            self.add_log("Configuración de zonas guardada")

    def check_existing_configuration(self):
        """Verifica si existe configuración previa"""
        json_path = os.path.join(self.project_root, "Files", "Temporary_Files", 
                                "Ruta_Insumos", "rutas_archivos_ladm_1_2.json")  # Cambio de ruta
        config_path = os.path.join(self.project_root, "Files", "Temporary_Files", 
                                "array_config.txt")
        
        self.has_inputs = os.path.exists(json_path)
        self.has_zones = os.path.exists(config_path)
        
        self.update_button_states()
        
        if self.has_inputs and self.has_zones:
            self.add_log("Configuración previa cargada correctamente")
        else:
            if not self.has_inputs:
                self.add_log("No se encontró configuración de insumos")
            if not self.has_zones:
                self.add_log("No se encontró configuración de zonas")
                
    def update_button_states(self):
        """Actualiza el estado de los botones según la configuración"""
        enabled = self.has_inputs and self.has_zones
        
        for btn in self.process_buttons:
            btn.setEnabled(enabled)
        
        if hasattr(self, 'individual_btn'):
            self.individual_btn.setEnabled(enabled)
        if hasattr(self, 'batch_btn'):
            self.batch_btn.setEnabled(enabled)

    def execute_all_processes(self):
        """Ejecuta todos los procesos del modelo LADM 1.2"""
        if not self.has_inputs:
            QMessageBox.warning(self, "Error", 
                            "Debe configurar los insumos antes de ejecutar procesos.")
            return

        # Verificar archivos existentes
        temp_dir = os.path.join(self.project_root, "Files", "Temporary_Files")
        modelo_ladm_dir = os.path.join(temp_dir, "MODELO_LADM_1_2")  # Cambiado de MODELO_INTERNO_1_0
        
        if os.path.exists(modelo_ladm_dir) and any(os.scandir(modelo_ladm_dir)):
            response = QMessageBox.warning(
                self,
                "Archivos existentes",
                "Se eliminarán archivos anteriores del modelo LADM 1.2 para iniciar este proceso. ¿Desea Continuar?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if response == QMessageBox.No:
                return
                
            try:
                for item in os.listdir(modelo_ladm_dir):
                    item_path = os.path.join(modelo_ladm_dir, item)
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                self.add_log("Contenido de MODELO_LADM_1_2 eliminado exitosamente")
            except Exception as e:
                self.add_log(f"Error al limpiar MODELO_LADM_1_2: {str(e)}")
                return

        # Configurar scripts para LADM 1.2
        try:
            scripts_dir = os.path.join(self.project_root, "Scripts", "MODELO_LADM_1_2")  # Cambiado
            scripts = [
                "01_Copiar_Archivos_necesarios.py",
                "02_convertir_gpkg_a_gdb.py",
                "03_Procesar_Conteo_de_Elementos.py",
                "04_Toolbox_validaciones.py",
                "05_Habilitar_column_excepciones.py",
                "06_Generar_DB_registro_Errores.py",
                "07_Diligenciar_Err._y_Excep._a_Excel.py",
                "08_Encabecado_Formato_Consitencia_Logica.py",
                "09_Reportes_Finales.py",
                "10_Compilación_Datos.py"
            ]
            
            script_paths = [(i+1, os.path.join(scripts_dir, script)) 
                        for i, script in enumerate(scripts)]
            
            self.process_manager.script_queue = script_paths
            self.process_manager.is_running = True
            self.process_manager.execute_next_script()
            
        except Exception as e:
            self.add_log(f"Error al configurar la ejecución de todos los procesos: {str(e)}")
            return False
    
    def run_data_structuring(self):
        try:
            if self.process_manager.is_running:
                return False
                    
            # Verificar directorio específico para cada modelo LADM
            temp_dir = os.path.join(self.project_root, "Files", "Temporary_Files")
            modelo_dir = os.path.join(temp_dir, "MODELO_LADM_1_2")  # o MODELO_LADM_1_0 según corresponda
            
            if os.path.exists(modelo_dir) and any(os.scandir(modelo_dir)):
                response = QMessageBox.warning(
                    self,
                    "Archivos existentes",
                    f"Se eliminarán los archivos dentro de {os.path.basename(modelo_dir)} para iniciar este proceso. ¿Desea Continuar?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                
                if response == QMessageBox.No:
                    return False

                try:
                    for item in os.listdir(modelo_dir):
                        item_path = os.path.join(modelo_dir, item)
                        if os.path.isfile(item_path):
                            os.remove(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                    self.add_log(f"Contenido de {os.path.basename(modelo_dir)} eliminado exitosamente")
                except Exception as e:
                    self.add_log(f"Error al limpiar {os.path.basename(modelo_dir)}: {str(e)}")
                    return False

            # Usar los scripts específicos de LADM
            scripts = [
                "01_Copiar_Archivos_necesarios.py",
                "02_convertir_gpkg_a_gdb.py",
                "03_Procesar_Conteo_de_Elementos.py",
                "04_Toolbox_validaciones.py",
                "05_Habilitar_column_excepciones.py"
            ]
            
            scripts_dir = os.path.join(self.project_root, "Scripts", "MODELO_LADM_1_2")  # o MODELO_LADM_1_0
            script_paths = [(i+1, os.path.join(scripts_dir, script)) for i, script in enumerate(scripts)]
            
            self.process_manager.script_queue = script_paths
            self.process_manager.is_running = True
            self.process_manager.execute_next_script()
            return True

        except Exception as e:
            self.add_log(f"Error al iniciar el proceso de estructuración: {str(e)}")
            return False
        
    def run_reports(self):
        """Ejecuta los últimos 5 procesos del modelo LADM 1.0"""
        try:
            if not self.process_manager.is_running:
                scripts = [
                    "06_Generar_DB_registro_Errores.py",
                    "07_Diligenciar_Err._y_Excep._a_Excel.py",
                    "08_Encabecado_Formato_Consitencia_Logica.py",
                    "09_Reportes_Finales.py",
                    "10_Compilación_Datos.py"
                ]
                
                scripts_dir = os.path.join(self.project_root, "Scripts", "MODELO_LADM_1_0")  # Cambio de directorio
                script_paths = [(i+6, os.path.join(scripts_dir, script)) for i, script in enumerate(scripts)]
                
                self.process_manager.script_queue = script_paths
                self.process_manager.is_running = True
                self.process_manager.execute_next_script()
            else:
                self.add_log("Hay procesos en ejecución. Espere a que terminen.")
        except Exception as e:
            self.add_log(f"Error al iniciar el proceso de reportes: {str(e)}")
                    
    def run_process_2(self):
        """Ejecuta el proceso 2"""
        try:
            if not self.process_manager.run_process_2():
                QMessageBox.warning(
                    self,
                    "Error",
                    "No se pudo iniciar el proceso 2. Revise el log para más detalles."
                )
        except Exception as e:
            self.add_log(f"Error al iniciar el proceso 2: {str(e)}")

    def run_process_3(self):
        """Ejecuta el proceso 3"""
        try:
            if not self.process_manager.run_process_3():
                QMessageBox.warning(
                    self,
                    "Error",
                    "No se pudo iniciar el proceso 3. Revise el log para más detalles."
                )
        except Exception as e:
            self.add_log(f"Error al iniciar el proceso 3: {str(e)}")

    def setup_status_and_console(self, parent_layout):
        """Configura el panel de estado y consola"""
        content_widget = QWidget()
        content_layout = QHBoxLayout(content_widget)
        content_layout.setContentsMargins(10, 5, 10, 5)
        
        # Panel de estado con estilo correcto
        status_frame = QFrame()
        status_frame.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 1px solid #d2b48c;
                border-radius: 4px;
            }
        """)
        status_layout = QVBoxLayout(status_frame)
        status_layout.setContentsMargins(10, 5, 10, 5)
        
        status_label = QLabel("Estado de Procesos")
        status_label.setAlignment(Qt.AlignCenter)
        status_label.setFont(QFont("Segoe UI", 12, QFont.Bold))
        status_layout.addWidget(status_label)
        
        # Lista actualizada de procesos para el modelo interno
        process_descriptions = [
            ("Copiar Archivos necesarios", False),
            ("Convertir GPKG (Output) a GDB", False),
            ("Procesar Conteo de Elementos", False),
            ("Toolbox Validaciones", False),
            ("Habilitar Columnas de Excepciones", True),
            ("Generar DB registro Errores", False),
            ("Diligenciar Err. y Excep. a Excel", False),
            ("Encabecado Formato Consitencia Logica", False),
            ("Reportes Finales", False),
            ("Compilación Datos", False)
        ]
        
        # Grid de indicadores
        grid = QGridLayout()
        self.status_indicators = {}
        
        for i, (description, should_color) in enumerate(process_descriptions):
            indicator = StatusIndicator()
            label = QLabel(f"{i+1}. {description}")
            
            if should_color:
                label.setStyleSheet("""
                    QLabel {
                        color: #FF1493;
                        font-weight: bold;
                    }
                """)
            else:
                label.setStyleSheet("color: #191a19;")
            
            row = i // 2
            col = (i % 2) * 2
            
            grid.addWidget(indicator, row, col)
            grid.addWidget(label, row, col + 1)
            
            self.status_indicators[i+1] = indicator
        
        status_layout.addLayout(grid)
        status_layout.addStretch()
        
        # Consola
        console_frame = QFrame()
        console_frame.setObjectName("consoleFrame")
        console_frame.setStyleSheet("""
            QFrame#consoleFrame {
                background-color: #1e1e1e;
                border: 1px solid #D2B48C;
                border-radius: 4px;
                margin: 5px;
            }
        """)
        console_layout = QVBoxLayout(console_frame)
        console_layout.setContentsMargins(10, 10, 10, 10)
        
        console_label = QLabel("Consola")
        console_label.setFont(QFont("Segoe UI", 10, QFont.Bold))
        console_label.setStyleSheet("color: white;")
        console_layout.addWidget(console_label)
        
        self.console = RichTextEdit()
        console_layout.addWidget(self.console)
        
        # Agregar los frames al layout principal con proporciones
        content_layout.addWidget(status_frame, 1)
        content_layout.addWidget(console_frame, 2)
        
        parent_layout.addWidget(content_widget)
        
    def setup_software_frame(self, parent_layout):
        """Configura el frame de software requerido"""
        software_frame = QFrame()
        software_frame.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 1px solid #D2B48C;
                border-radius: 4px;
            }
            QLabel {
                font-weight: bold;
                color: #4A4A4A;
            }
        """)
        
        software_layout = QVBoxLayout(software_frame)
        software_layout.setContentsMargins(10, 10, 10, 10)
        
        # Imagen de ArcGIS Pro
        arcgis_path = os.path.join(self.project_root, "Scripts", "img", "ArcGIS-Pro.png")
        if os.path.exists(arcgis_path):
            img_label = self.create_image_label(arcgis_path, 55, 55)
            software_layout.addWidget(img_label, alignment=Qt.AlignCenter)
        
        # Texto Software Requeridos
        software_title = QLabel("Software Requeridos")
        software_title.setFont(QFont("Segoe UI", 12, QFont.Bold))
        software_title.setAlignment(Qt.AlignCenter)
        software_layout.addWidget(software_title)
        
        parent_layout.addWidget(software_frame)

    def setup_subdir_frame(self, parent_layout):
        """Configura el frame de subdirección"""
        subdir_frame = QFrame()
        subdir_frame.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 1px solid #D2B48C;
                border-radius: 4px;
            }
            QLabel {
                font-weight: bold;
                color: #4A4A4A;
            }
        """)
        
        subdir_layout = QVBoxLayout(subdir_frame)
        subdir_layout.setContentsMargins(10, 10, 10, 10)
        
        # Contenedor para las imágenes
        images_layout = QHBoxLayout()
        images_layout.setSpacing(20)
        
        # Imágenes IGAC y proyectos
        igac_path = os.path.join(self.project_root, "Scripts", "img", "IGAC.png")
        proyectos_path = os.path.join(self.project_root, "Scripts", "img", "proyectos.svg")
        
        if os.path.exists(igac_path):
            igac_label = self.create_image_label(igac_path, 60, 55)
            images_layout.addWidget(igac_label)
        
        if os.path.exists(proyectos_path):
            proyectos_label = self.create_image_label(proyectos_path, 55, 55)
            images_layout.addWidget(proyectos_label)
        
        subdir_layout.addLayout(images_layout)
        
        # Texto Subdirección
        subdir_title = QLabel("Subdirección de Proyectos")
        subdir_title.setFont(QFont("Segoe UI", 12, QFont.Bold))
        subdir_title.setAlignment(Qt.AlignCenter)
        subdir_layout.addWidget(subdir_title)
        
        parent_layout.addWidget(subdir_frame)


        def setup_footer(self, parent_layout):
            """Configura el pie de página"""
            footer_widget = QWidget()
            footer_layout = QHBoxLayout(footer_widget)
            footer_layout.setContentsMargins(10, 5, 10, 5)
            
            clear_btn = QPushButton("Limpiar Consola")
            clear_btn.setStyleSheet("""
                QPushButton {
                    background-color: #FF8C00;
                    color: white;
                    margin: 0px 5px;
                }
            """)
            clear_btn.clicked.connect(self.clear_console)
            
            stop_btn = QPushButton("Detener Procesos")
            stop_btn.setStyleSheet("""
                QPushButton {
                    background-color: #e74c3c;
                    color: white;
                    margin: 0px 5px;
                }
            """)
            stop_btn.clicked.connect(self.stop_processes)
            
            footer_layout.addWidget(clear_btn)
            footer_layout.addWidget(stop_btn)
            
            # Agregar el widget del footer al layout padre
            parent_layout.addWidget(footer_widget)

    def stop_processes(self):
        """Detiene todos los procesos en ejecución"""
        self.process_manager.stop_all()
        self.add_log("Deteniendo procesos...")

    def clear_console(self):
        """Limpia la consola"""
        try:
            self.console.clear()
            self.add_log("Consola limpiada")
        except Exception as e:
            print(f"Error al limpiar consola: {str(e)}")