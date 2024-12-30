from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton, 
                              QLabel, QLineEdit, QFrame, QFileDialog, QMessageBox)
import os
import json

class ConfigDialogInterno(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        self.setWindowTitle("Configuración de Insumos Modelo Interno")
        self.setMinimumWidth(600)
        self.setup_directories()
        self.setup_ui()
        self.load_existing_data()

    def setup_directories(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while os.path.basename(current_dir) != "GeoValidaTool" and current_dir != os.path.dirname(current_dir):
            current_dir = os.path.dirname(current_dir)
        
        self.json_dir = os.path.join(current_dir, "Files", "Temporary_Files", "Ruta_Insumos")
        os.makedirs(self.json_dir, exist_ok=True)
        self.json_path = os.path.join(self.json_dir, "rutas_archivos_interno.json")

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        
        # Header con estilo distintivo para Modelo Interno
        header_layout = QHBoxLayout()
        title_label = QLabel("Configuración de Rutas GPKG - Modelo Interno")
        title_label.setStyleSheet("""
            font-weight: bold;
            font-size: 14px;
            color: #1B4F72;
        """)
        
        clear_btn = QPushButton("Limpiar Datos")
        clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #CD6155;
                color: white;
                padding: 5px 10px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #B03A2E;
            }
        """)
        clear_btn.clicked.connect(self.clear_data)
        
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        header_layout.addWidget(clear_btn)
        layout.addLayout(header_layout)
        
        # Banner informativo específico para Modelo Interno
        info_frame = QFrame()
        info_frame.setStyleSheet("""
            QFrame {
                background-color: #D4E6F1;
                border: 1px solid #A9CCE3;
                border-radius: 4px;
                margin: 5px 0px;
                padding: 10px;
            }
        """)
        info_layout = QHBoxLayout(info_frame)
        info_label = QLabel(
            "ℹ️ Nota: Para el modelo interno, seleccione el archivo GPKG original y "
            "su correspondiente versión transformada ETL."
        )
        info_label.setStyleSheet("color: #1B4F72; font-style: italic;")
        info_label.setWordWrap(True)
        info_layout.addWidget(info_label)
        layout.addWidget(info_frame)
        
        # Frame para GPKG Original
        self.setup_gpkg_frame("GPKG Base (Original)", "original")
        
        # Frame para GPKG Modificado
        self.setup_gpkg_frame("GPKG Modificado (Transformación ETL)", "modified")
        
        # Botones de acción
        button_layout = QHBoxLayout()
        save_btn = QPushButton("Guardar Configuración")
        save_btn.setStyleSheet("""
            QPushButton {
                background-color: #2874A6;
                color: white;
                padding: 8px 15px;
                border-radius: 4px;
                font-weight: bold;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #1A5276;
            }
        """)
        save_btn.clicked.connect(self.save_config)
        
        cancel_btn = QPushButton("Cancelar")
        cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #95A5A6;
                color: white;
                padding: 8px 15px;
                border-radius: 4px;
                font-weight: bold;
                min-width: 120px;
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

    def setup_gpkg_frame(self, title, gpkg_type):
        frame = QFrame()
        frame.setStyleSheet("""
            QFrame {
                border: 1px solid #AED6F1;
                border-radius: 6px;
                padding: 15px;
                background-color: #F8FBFD;
                margin: 5px 0px;
            }
        """)
        frame_layout = QVBoxLayout(frame)
        
        label = QLabel(f"📦 {title}")
        label.setStyleSheet("""
            font-weight: bold;
            font-size: 12px;
            color: #1B4F72;
            margin-bottom: 5px;
        """)
        
        input_layout = QHBoxLayout()
        line_edit = QLineEdit()
        line_edit.setReadOnly(True)
        line_edit.setMinimumWidth(400)
        line_edit.setPlaceholderText("Seleccione un archivo GPKG...")
        line_edit.setStyleSheet("""
            QLineEdit {
                padding: 5px;
                border: 1px solid #AED6F1;
                border-radius: 4px;
                background-color: white;
            }
        """)
        setattr(self, f"gpkg_{gpkg_type}_edit", line_edit)
        
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
                background-color: #2E86C1;
            }
        """)
        select_btn.clicked.connect(lambda: self.select_gpkg(gpkg_type))
        
        input_layout.addWidget(line_edit)
        input_layout.addWidget(select_btn)
        
        status_label = QLabel("⚪ No se ha seleccionado ningún archivo")
        status_label.setStyleSheet("""
            color: #7F8C8D;
            font-style: italic;
            margin-top: 5px;
        """)
        setattr(self, f"status_{gpkg_type}", status_label)
        
        frame_layout.addWidget(label)
        frame_layout.addLayout(input_layout)
        frame_layout.addWidget(status_label)
        
        self.layout().addWidget(frame)

    def select_gpkg(self, gpkg_type):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            f"Seleccionar archivo GPKG {gpkg_type.title()}",
            "",
            "GeoPackage Files (*.gpkg)"
        )
        
        if file_path:
            line_edit = getattr(self, f"gpkg_{gpkg_type}_edit")
            status_label = getattr(self, f"status_{gpkg_type}")
            
            line_edit.setText(file_path)
            status_label.setText(f"✅ Archivo seleccionado: {os.path.basename(file_path)}")
            status_label.setStyleSheet("""
                color: #27AE60;
                font-style: normal;
                font-weight: bold;
                margin-top: 5px;
            """)

    def save_config(self):
        gpkg_original = self.gpkg_original_edit.text()
        gpkg_modified = self.gpkg_modified_edit.text()
        
        if not gpkg_original or not gpkg_modified:
            QMessageBox.warning(
                self,
                "Configuración Incompleta",
                "Por favor seleccione ambos archivos GPKG (Original y Modificado)",
                QMessageBox.Ok
            )
            return
            
        if gpkg_original == gpkg_modified:
            QMessageBox.warning(
                self,
                "Error de Configuración",
                "El archivo GPKG original y el modificado no pueden ser el mismo.",
                QMessageBox.Ok
            )
            return
            
        config = {
            "gpkg_original": gpkg_original,
            "gpkg_modified": gpkg_modified,
            "model_type": "interno"
        }
        
        try:
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
            QMessageBox.information(
                self,
                "Configuración Guardada",
                "La configuración se ha guardado exitosamente",
                QMessageBox.Ok
            )
            self.accept()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"Error al guardar la configuración:\n{str(e)}",
                QMessageBox.Ok
            )

    def clear_data(self):
        reply = QMessageBox.question(
            self,
            'Confirmar Limpieza',
            '¿Está seguro de que desea limpiar todos los datos configurados?',
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                if os.path.exists(self.json_path):
                    os.remove(self.json_path)
                
                for gpkg_type in ['original', 'modified']:
                    line_edit = getattr(self, f"gpkg_{gpkg_type}_edit")
                    status_label = getattr(self, f"status_{gpkg_type}")
                    
                    line_edit.clear()
                    status_label.setText("⚪ No se ha seleccionado ningún archivo")
                    status_label.setStyleSheet("""
                        color: #7F8C8D;
                        font-style: italic;
                        margin-top: 5px;
                    """)
                
                QMessageBox.information(
                    self,
                    "Datos Limpiados",
                    "Se han limpiado todos los datos configurados",
                    QMessageBox.Ok
                )
                
            except Exception as e:
                QMessageBox.critical(
                    self,
                    "Error",
                    f"Error al limpiar los datos:\n{str(e)}",
                    QMessageBox.Ok
                )

    def load_existing_data(self):
        try:
            if os.path.exists(self.json_path):
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    for gpkg_type in ['original', 'modified']:
                        key = f"gpkg_{gpkg_type}"
                        if key in data and os.path.exists(data[key]):
                            line_edit = getattr(self, f"gpkg_{gpkg_type}_edit")
                            status_label = getattr(self, f"status_{gpkg_type}")
                            
                            line_edit.setText(data[key])
                            status_label.setText(f"✅ Archivo seleccionado: {os.path.basename(data[key])}")
                            status_label.setStyleSheet("""
                                color: #27AE60;
                                font-style: normal;
                                font-weight: bold;
                                margin-top: 5px;
                            """)
                        
        except Exception as e:
            QMessageBox.warning(
                self,
                "Error al Cargar Datos",
                f"No se pudieron cargar los datos existentes:\n{str(e)}",
                QMessageBox.Ok
            )