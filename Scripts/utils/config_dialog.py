from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                            QHBoxLayout, QPushButton, QLabel, QFileDialog,
                            QMessageBox, QLineEdit, QFrame, QCheckBox, QDialog,
                            QTextEdit, QScrollArea, QGridLayout, QTabWidget)
from PySide6.QtCore import QThread, Signal, Qt, QSize
from PySide6.QtGui import QColor, QPainter, QFont, QIcon, QPixmap, QTextCursor, QTextOption
from PySide6.QtCore import QTimer, QMetaObject, Qt
from PySide6.QtCore import Slot as pyqtSlot
import sys
import os
import json

class ConfigDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        self.setWindowTitle("Configuración de Insumos")
        self.setMinimumWidth(800)  # Aumentado para mejor visualización
        self.setMinimumHeight(600)
        self.setup_directories()
        self.setup_ui()
        self.load_existing_data()

    def setup_directories(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while os.path.basename(current_dir) != "GeoValidaTool" and current_dir != os.path.dirname(current_dir):
            current_dir = os.path.dirname(current_dir)
        
        self.project_root = current_dir
        self.json_dir = os.path.join(self.project_root, "Files", "Temporary_Files", "Ruta_Insumos")
        os.makedirs(self.json_dir, exist_ok=True)
        self.json_path = os.path.join(self.json_dir, "rutas_archivos.json")

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(3)
        self.setStyleSheet("""
            QDialog {
                background-color: #F5F5F5;
            }
            QTabWidget::pane {
                border: 1px solid #D2B48C;
                border-radius: 4px;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #DEB887;
                color: #4A4A4A;
                padding: 5px 10px;
                margin: 1px;
                border: 1px solid #D2B48C;
                border-radius: 4px 4px 0 0;
            }
            QTabBar::tab:selected {
                background-color: white;
                border-bottom: none;
            }
            QTabBar::tab:hover {
                background-color: #E6CCB3;
            }
            QFrame {
                border: 1px solid #BDC3C7;
                border-radius: 4px;
                padding: 5px;
                background-color: white;
                margin: 1px 0px;
            }
            QFrame:hover {
                background-color: #F8F9F9;
            }
        """)
        
        # Crear el widget de pestañas
        self.tab_widget = QTabWidget()
        
        # Primera pestaña - CICA
        cica_tab = QWidget()
        self.setup_cica_tab(cica_tab)
        self.tab_widget.addTab(cica_tab, "Insumos CICA")
        
        # Segunda pestaña - CONSERVACIÓN
        conservacion_tab = QWidget()
        self.setup_conservacion_tab(conservacion_tab)
        self.tab_widget.addTab(conservacion_tab, "Insumos CONSERVACIÓN")
        
        layout.addWidget(self.tab_widget)
        
        # Botones principales
        button_layout = QHBoxLayout()
        save_btn = QPushButton("Guardar Configuración")
        save_btn.setStyleSheet("""
            QPushButton {
                background-color: #2ECC71;
                color: white;
                padding: 8px 15px;
                border-radius: 4px;
                font-weight: bold;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #27AE60;
            }
        """)
        save_btn.clicked.connect(self.accept)
        
        cancel_btn = QPushButton("Cancelar")
        cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #95A5A6;
                color: white;
                padding: 8px 15px;
                border-radius: 4px;
                font-weight: bold;
                min-width: 150px;
            }
            QPushButton:hover {
                background-color: #7F8C8D;
            }
        """)
        cancel_btn.clicked.connect(self.reject)
        
        button_layout.addStretch()
        button_layout.addWidget(cancel_btn)
        button_layout.addWidget(save_btn)
        layout.addLayout(button_layout)

    def setup_cica_tab(self, tab):
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)
        
        # Header con banner informativo
        header_frame = QFrame()
        header_frame.setStyleSheet("""
            QFrame {
                background-color: #D4E6F1;
                border: 1px solid #A9CCE3;
                border-radius: 4px;
                margin: 5px 0px;
                padding: 10px;
            }
        """)
        header_layout = QHBoxLayout(header_frame)
        
        title_label = QLabel("📁 Configuración de Rutas de Archivos CICA")
        title_label.setStyleSheet("""
            font-weight: bold;
            font-size: 14px;
            color: #2C3E50;
        """)
        
        clear_btn = QPushButton("Limpiar Datos")
        clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #E74C3C;
                color: white;
                padding: 5px 10px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #C0392B;
            }
        """)
        clear_btn.clicked.connect(lambda: self.clear_data("cica"))
        
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        header_layout.addWidget(clear_btn)
        layout.addWidget(header_frame)
        
        # Banner informativo
        info_frame = QFrame()
        info_frame.setStyleSheet("""
            QFrame {
                background-color: #EBF5FB;
                border: 1px solid #AED6F1;
                border-radius: 4px;
                margin: 5px 0px;
                padding: 10px;
            }
        """)
        info_layout = QHBoxLayout(info_frame)
        info_label = QLabel(
            "ℹ️ Seleccione los archivos necesarios para el modelo CICA. "
            "Asegúrese de incluir tanto la GDB como los archivos CSV correspondientes."
        )
        info_label.setStyleSheet("color: #2471A3; font-style: italic;")
        info_label.setWordWrap(True)
        info_layout.addWidget(info_label)
        layout.addWidget(info_frame)

        # Configuración de archivos CICA
        self.cica_configs = {
            'gdb': {
                'label': 'Archivo de la GDB',
                'description': 'Seleccione el archivo GDB principal',
                'filter': 'GDB Files (*.gdb)',
                'validation_keywords': []
            },
            'predio': {
                'label': 'CSV de Datos Predio',
                'description': 'Seleccione el archivo CSV con los datos del predio',
                'filter': 'CSV Files (*.csv)',
                'validation_keywords': ['Datos', 'Predio']
            },
            'construccion': {
                'label': 'CSV de Unidad de Construcción',
                'description': 'Seleccione el archivo CSV de unidades de construcción',
                'filter': 'CSV Files (*.csv)',
                'validation_keywords': ['Unidad', 'Construccion']
            }
        }


        for key, config in self.cica_configs.items():
            frame = self.create_file_frame(config, key, "cica")
            layout.addWidget(frame)

    def setup_conservacion_tab(self, tab):
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)
        
        # Header con banner informativo
        header_frame = QFrame()
        header_frame.setStyleSheet("""
            QFrame {
                background-color: #FCF3CF;
                border: 1px solid #F7DC6F;
                border-radius: 4px;
                margin: 5px 0px;
                padding: 10px;
            }
        """)
        header_layout = QHBoxLayout(header_frame)
        
        title_label = QLabel("📁 Configuración de Rutas de Archivos CONSERVACIÓN")
        title_label.setStyleSheet("""
            font-weight: bold;
            font-size: 14px;
            color: #7D6608;
        """)
        
        clear_btn = QPushButton("Limpiar Datos")
        clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #E74C3C;
                color: white;
                padding: 5px 10px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #C0392B;
            }
        """)
        clear_btn.clicked.connect(lambda: self.clear_data("conservacion"))
        
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        header_layout.addWidget(clear_btn)
        layout.addWidget(header_frame)
        
        # Banner informativo
        info_frame = QFrame()
        info_frame.setStyleSheet("""
            QFrame {
                background-color: #FEF9E7;
                border: 1px solid #FAD7A0;
                border-radius: 4px;
                margin: 5px 0px;
                padding: 10px;
            }
        """)
        info_layout = QHBoxLayout(info_frame)
        info_label = QLabel(
            "ℹ️ Seleccione los archivos necesarios para el modelo de CONSERVACIÓN. "
            "Asegúrese de incluir la GDB y el archivo APEX correspondiente."
        )
        info_label.setStyleSheet("color: #7D6608; font-style: italic;")
        info_label.setWordWrap(True)
        info_layout.addWidget(info_label)
        layout.addWidget(info_frame)
        
        # Configuración de archivos CONSERVACIÓN
        self.conservacion_configs = {
            'gdb': {
                'label': 'Archivo de la GDB',
                'description': 'Seleccione el archivo GDB principal',
                'filter': 'GDB Files (*.gdb)',
                'validation_keywords': []
            },
            'apex_conservacion': {
                'label': 'CSV (APEX CONSERVACION)',
                'description': 'Seleccione el archivo CSV de APEX CONSERVACION',
                'filter': 'CSV Files (*.csv)',
                'validation_keywords': ['APEX']
            }
        }
        
        for key, config in self.conservacion_configs.items():
            frame = self.create_file_frame(config, key, "conservacion")
            layout.addWidget(frame)

    def create_file_frame(self, config, key, tab_type):
        frame = QFrame()
        frame.setStyleSheet("""
            QFrame {
                border: 1px solid #BDC3C7;
                border-radius: 4px;
                padding: 5px;
                background-color: white;
                margin: 1px 0px;
            }
            QFrame:hover {
                background-color: #F8F9F9;
            }
        """)
        frame_layout = QVBoxLayout(frame)
        frame_layout.setSpacing(3)
        frame_layout.setContentsMargins(4, 3, 4, 3)
        
        # Título y descripción en una sola línea
        header_layout = QHBoxLayout()
        # Alternativa con el emoji 🛢
        icon = "🛢" if "gdb" in key.lower() else "📄"  # 💾🛢 para GDB, 📄 para CSV
        label = QLabel(f"{icon} {config['label']}")
        
        label.setStyleSheet("""
            font-weight: bold;
            font-size: 12px;
            color: #2C3E50;
        """)
        
        description = QLabel(config['description'])
        description.setStyleSheet("""
            color: #7F8C8D;
            font-style: italic;
            margin-left: 10px;
        """)
        
        header_layout.addWidget(label)
        header_layout.addWidget(description)
        header_layout.addStretch()
        frame_layout.addLayout(header_layout)
        
        # Input y botón
        input_layout = QHBoxLayout()
        input_layout.setContentsMargins(0, 0, 0, 0)  # Sin márgenes
        line_edit = QLineEdit()
        line_edit.setReadOnly(True)
        line_edit.setPlaceholderText("Ningún archivo seleccionado...")
        line_edit.setStyleSheet("""
            QLineEdit {
                padding: 5px 10px;
                border: 1px solid #BDC3C7;
                border-radius: 4px;
                background-color: #F8F9F9;
            }
        """)
        
        select_btn = QPushButton("Seleccionar Archivo")
        select_btn.setStyleSheet("""
            QPushButton {
                background-color: #3498DB;
                color: white;
                padding: 5px 15px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2980B9;
            }
        """)
        select_btn.clicked.connect(lambda: self.select_file(key, tab_type))
        
        input_layout.addWidget(line_edit)
        input_layout.addWidget(select_btn)
        frame_layout.addLayout(input_layout)
        
        # Status label
        status_label = QLabel("⚪ No se ha seleccionado ningún archivo")
        status_label.setStyleSheet("""
            color: #95A5A6;
            font-style: italic;
            margin-top: 2px;  /* Reducido de 5px a 2px */
            font-size: 11px;  /* Texto más pequeño */
        """)
        frame_layout.addWidget(status_label)
        
        configs = self.cica_configs if tab_type == "cica" else self.conservacion_configs
        configs[key]['line_edit'] = line_edit
        configs[key]['status_label'] = status_label
        
        return frame
    
    def select_file(self, key, tab_type):
        configs = self.cica_configs if tab_type == "cica" else self.conservacion_configs
        config = configs[key]
        
        if key == 'gdb':
            dialog = QFileDialog(self)
            dialog.setFileMode(QFileDialog.Directory)
            dialog.setOption(QFileDialog.ShowDirsOnly, False)
            dialog.setNameFilter("GDB Files (*.gdb)")
            dialog.setViewMode(QFileDialog.Detail)
            
            if dialog.exec():
                file_paths = dialog.selectedFiles()
                if file_paths:
                    file_path = file_paths[0]
                    if not file_path.lower().endswith('.gdb'):
                        QMessageBox.warning(self, "Error", "Por favor seleccione un archivo .gdb válido")
                        return
                    
                    config['path'] = file_path
                    config['line_edit'].setText(file_path)
                    self.update_status_label(key, tab_type)
                    self.save_to_json()
        else:
            file_path, _ = QFileDialog.getOpenFileName(
                self,
                "Seleccionar " + config['label'],
                "",
                config['filter']
            )
            
            if file_path:
                if self.validate_file_name(key, file_path, tab_type):
                    config['path'] = file_path
                    config['line_edit'].setText(file_path)
                    self.update_status_label(key, tab_type)
                    self.save_to_json()

    def validate_file_name(self, key, file_path, tab_type):
        configs = self.cica_configs if tab_type == "cica" else self.conservacion_configs
        config = configs[key]
        
        if not config['validation_keywords']:
            return True

        file_name = os.path.basename(file_path).lower()
        keywords_present = any(keyword.lower() in file_name for keyword in config['validation_keywords'])
        
        if not keywords_present:
            msg = (f"⚠️ El archivo seleccionado no contiene ninguna de las palabras clave esperadas: "
                  f"{', '.join(config['validation_keywords'])}.\n\n"
                  f"¿Está seguro que este es el archivo correcto?")
            return QMessageBox.question(
                self,
                "Verificación de Archivo",
                msg,
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            ) == QMessageBox.Yes
        return True

    def update_status_label(self, key, tab_type):
        configs = self.cica_configs if tab_type == "cica" else self.conservacion_configs
        config = configs[key]
        
        if 'path' in config and config['path'] and os.path.exists(config['path']):
            config['status_label'].setText(f"✅ Archivo seleccionado: {os.path.basename(config['path'])}")
            config['status_label'].setStyleSheet("""
                color: #27AE60;
                font-style: normal;
                font-weight: bold;
                margin-top: 5px;
            """)
        else:
            config['status_label'].setText("❌ No se ha seleccionado ningún archivo válido")
            config['status_label'].setStyleSheet("""
                color: #E74C3C;
                font-style: italic;
                margin-top: 5px;
            """)

    def save_to_json(self):
        try:
            data = {}
            
            # Guardar datos de CICA
            for key, config in self.cica_configs.items():
                if 'path' in config and config['path']:
                    data[key] = config['path']
            
            # Guardar datos de CONSERVACIÓN
            for key, config in self.conservacion_configs.items():
                if 'path' in config and config['path']:
                    if key == 'gdb' and 'gdb' not in data:
                        data[key] = config['path']
                    elif key != 'gdb':
                        data[key] = config['path']
            
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                
        except Exception as e:
            QMessageBox.warning(
                self,
                "Error de Guardado",
                f"Error al guardar la configuración:\n{str(e)}",
                QMessageBox.Ok
            )

    def load_existing_data(self):
        try:
            if os.path.exists(self.json_path):
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Cargar datos para CICA
                    for key, path in data.items():
                        if key in self.cica_configs and os.path.exists(path):
                            self.cica_configs[key]['path'] = path
                            self.cica_configs[key]['line_edit'].setText(path)
                            self.update_status_label(key, "cica")
                    
                    # Cargar datos para CONSERVACIÓN
                    for key, path in data.items():
                        if key in self.conservacion_configs and os.path.exists(path):
                            self.conservacion_configs[key]['path'] = path
                            self.conservacion_configs[key]['line_edit'].setText(path)
                            self.update_status_label(key, "conservacion")
                            
        except Exception as e:
            QMessageBox.warning(
                self,
                "Error de Carga",
                f"Error al cargar los datos existentes:\n{str(e)}",
                QMessageBox.Ok
            )

    def clear_data(self, tab_type):
        configs = self.cica_configs if tab_type == "cica" else self.conservacion_configs
        
        reply = QMessageBox.question(
            self,
            'Confirmar Limpieza',
            f'¿Está seguro de que desea limpiar todos los datos de {tab_type.upper()}?',
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                for config in configs.values():
                    config['line_edit'].clear()
                    config['status_label'].setText("⚪ No se ha seleccionado ningún archivo")
                    config['status_label'].setStyleSheet("""
                        color: #95A5A6;
                        font-style: italic;
                        margin-top: 5px;
                    """)
                    if 'path' in config:
                        del config['path']
                
                self.save_to_json()
                QMessageBox.information(
                    self,
                    "Limpieza Exitosa",
                    f"Los datos de {tab_type.upper()} han sido limpiados correctamente",
                    QMessageBox.Ok
                )
                
            except Exception as e:
                QMessageBox.critical(
                    self,
                    "Error de Limpieza",
                    f"Ha ocurrido un error al limpiar los datos:\n{str(e)}",
                    QMessageBox.Ok
                )