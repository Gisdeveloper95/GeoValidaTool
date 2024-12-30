from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                            QHBoxLayout, QPushButton, QLabel, QFileDialog,
                            QMessageBox, QLineEdit, QFrame, QCheckBox, QDialog,
                            QTextEdit, QScrollArea, QGridLayout)
from PySide6.QtCore import QThread, Signal, Qt, QSize
from PySide6.QtGui import QColor, QPainter, QFont, QIcon, QPixmap, QTextCursor,QTextOption
from PySide6.QtCore import QTimer, QMetaObject, Qt
from PySide6.QtCore import Slot as pyqtSlot
import sys
import os
from rich.console import Console
from rich.text import Text
import io
import datetime
import subprocess
import json

import sys
sys.stdout.reconfigure(encoding='utf-8')

class ZonesDialog(QDialog):
    """Diálogo para selección de zonas"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Zonas de Trabajo")
        self.setFixedSize(350, 400)
        self.setup_directories()
        self.setup_ui()
        self.load_existing_zones()
    
    def setup_directories(self):
        """Configura los directorios necesarios para guardar configuraciones"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Navegamos hacia arriba hasta encontrar la carpeta raíz del proyecto
        while os.path.basename(current_dir) != "GeoValidaTool" and current_dir != os.path.dirname(current_dir):
            current_dir = os.path.dirname(current_dir)
        
        # Definir la ruta del directorio temporal
        self.temp_dir = os.path.join(current_dir, "Files", "Temporary_Files")
        os.makedirs(self.temp_dir, exist_ok=True)
        self.config_path = os.path.join(self.temp_dir, "array_config.txt")
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Añadir label informativo
        info_label = QLabel("Seleccione las zonas de trabajo:")
        info_label.setStyleSheet("font-weight: bold; margin-bottom: 10px;")
        layout.addWidget(info_label)
        
        # Scroll area para los checkboxes
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        # Lista de zonas en el orden específico
        self.elementos = [
            "URBANO_CTM12",
            "RURAL_CTM12",
            "URBANO",
            "RURAL"
        ]
        
        # Crear checkboxes
        self.checkboxes = {}
        for elemento in self.elementos:
            cb = QCheckBox(elemento)
            self.checkboxes[elemento] = cb
            scroll_layout.addWidget(cb)
        
        scroll_layout.addStretch()
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)
        
        # Botones de selección
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("Seleccionar Todo")
        select_all_btn.clicked.connect(self.select_all)
        deselect_all_btn = QPushButton("Deseleccionar Todo")
        deselect_all_btn.clicked.connect(self.deselect_all)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        layout.addLayout(btn_layout)
        
        # Botones de acción
        action_btn_layout = QHBoxLayout()
        save_btn = QPushButton("Guardar")
        save_btn.clicked.connect(self.save_zones)
        cancel_btn = QPushButton("Cancelar")
        cancel_btn.clicked.connect(self.reject)
        action_btn_layout.addWidget(save_btn)
        action_btn_layout.addWidget(cancel_btn)
        layout.addLayout(action_btn_layout)

    def select_all(self):
        """Selecciona todos los checkboxes"""
        for checkbox in self.checkboxes.values():
            checkbox.setChecked(True)

    def deselect_all(self):
        """Deselecciona todos los checkboxes"""
        for checkbox in self.checkboxes.values():
            checkbox.setChecked(False)

    def load_existing_zones(self):
        """Carga la configuración existente si existe"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Procesar cada línea
                    for line in content.split('\n'):
                        line = line.strip()
                        if line and '"' in line:
                            # Extraer el nombre de la zona
                            zone = line.split('"')[1]
                            if zone in self.checkboxes:
                                # Si la línea no tiene #, marcar como seleccionado
                                self.checkboxes[zone].setChecked('#' not in line)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Error al cargar configuración existente: {str(e)}")

    def save_zones(self):
        """Guarda las zonas en el formato específico"""
        try:
            # Crear el array con los elementos
            array = []
            for elemento in self.elementos:
                if not self.checkboxes[elemento].isChecked():
                    array.append(f'#"{elemento}"')  # # antes de las comillas para no seleccionados
                else:
                    array.append(f'"{elemento}"')   # solo comillas para seleccionados

            # Guardar en archivo
            with open(self.config_path, "w", encoding='utf-8') as f:
                f.write("[\n    " + ",\n    ".join(array) + "\n]")

            QMessageBox.information(self, "Éxito", "Configuración guardada correctamente")
            self.accept()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al guardar la configuración: {str(e)}")
