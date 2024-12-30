import os
import arcpy
from pathlib import Path
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QLabel, 
                            QPushButton, QFrame, QApplication)
from PySide6.QtCore import Qt, QPropertyAnimation
import sys
import subprocess

class AnimatedButton(QPushButton):
    def __init__(self, text, parent=None):
        super().__init__(text, parent)
        self.setFixedHeight(45)
        self.setCursor(Qt.PointingHandCursor)
        
        self.setStyleSheet("""
            QPushButton {
                background-color: #2ecc71;
                border: none;
                border-radius: 22px;
                color: white;
                font-size: 14px;
                font-weight: bold;
                padding: 10px 20px;
            }
            QPushButton:hover {
                background-color: #27ae60;
            }
            QPushButton:pressed {
                background-color: #219a52;
            }
        """)

class AlertWindow(QMainWindow):
    def __init__(self, validation_path):
        super().__init__()
        self.validation_path = validation_path
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle("⚠️ Revisión de Excepciones Requerida")
        self.setFixedSize(650, 480)
        self.setWindowFlags(Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint)
        self.setAttribute(Qt.WA_TranslucentBackground)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(20, 20, 20, 20)
        
        main_frame = QFrame()
        main_frame.setObjectName("mainFrame")
        main_frame.setStyleSheet("""
            QFrame#mainFrame {
                background-color: white;
                border-radius: 20px;
                border: 2px solid #e0e0e0;
            }
        """)
        frame_layout = QVBoxLayout(main_frame)
        frame_layout.setSpacing(10)
        
        title_label = QLabel("⚠️ REVISIÓN DE EXCEPCIONES EN SHAPEFILES ⚠️")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("""
            QLabel {
                color: #e74c3c;
                font-size: 22px;
                font-weight: bold;
                padding: 10px;
            }
        """)
        
        message = """
        <div style='font-size: 15px; line-height: 1.4;'>
            <p><b>Se ha completado el proceso de agregar la columna de excepciones.</b></p>
            
            <p style='margin: 5px 0;'><b>Por favor, revisa los shapefiles en el directorio de Validaciones_Calidad:</b></p>
            
            <p style='margin: 5px 0;'>Para marcar excepciones:</p>
            <p style='margin: 3px 0;'>⚠️ Utiliza la columna 'isExceptio' en cada shapefile</p>
            <p style='margin: 3px 0;'>⚠️ Marca con valor 1 las geometrías que consideres como excepciones</p>
            <p style='margin: 3px 0;'>⚠️ Guarda los cambios después de cada edición</p>
            
            <p style='color: #e74c3c; font-weight: bold; font-size: 16px; margin-top: 10px;'>
            ¡Mantén los archivos en su ubicación original!</p>
        </div>
        """
        
        message_label = QLabel(message)
        message_label.setWordWrap(True)
        message_label.setAlignment(Qt.AlignLeft)
        message_label.setStyleSheet("""
            QLabel {
                color: #2c3e50;
                padding: 10px 20px;
            }
        """)
        
        button_layout = QVBoxLayout()
        button_layout.setSpacing(10)
        
        self.accept_button = AnimatedButton("✔️ Abrir Directorio y Revisar Excepciones")
        self.accept_button.clicked.connect(self.accept_and_open)
        
        self.cancel_button = AnimatedButton("❌ Cerrar")
        self.cancel_button.setStyleSheet("""
            QPushButton {
                background-color: #e74c3c;
                border: none;
                border-radius: 22px;
                color: white;
                font-size: 14px;
                font-weight: bold;
                padding: 10px 20px;
                margin: 5px;
            }
            QPushButton:hover {
                background-color: #c0392b;
            }
            QPushButton:pressed {
                background-color: #a93226;
            }
        """)
        self.cancel_button.clicked.connect(self.close_and_open)
        
        frame_layout.addWidget(title_label)
        frame_layout.addWidget(message_label)
        frame_layout.addLayout(button_layout)
        button_layout.addWidget(self.accept_button)
        button_layout.addWidget(self.cancel_button)
        
        layout.addWidget(main_frame)
        
        self.setStyleSheet("""
            QMainWindow {
                background-color: transparent;
            }
        """)
        
        self.animation = QPropertyAnimation(self, b"windowOpacity")
        self.animation.setDuration(250)
        self.animation.setStartValue(0.0)
        self.animation.setEndValue(1.0)
        self.animation.start()
        
    def accept_and_open(self):
        self.abrir_directorio()
        self.close()
        
    def close_and_open(self):
        self.abrir_directorio()
        self.close()
        
    def abrir_directorio(self):
        if os.path.exists(self.validation_path):
            if os.name == 'nt':  # Windows
                os.startfile(str(self.validation_path))
            elif os.name == 'posix':  # macOS y Linux
                subprocess.run(['xdg-open' if os.name == 'posix' else 'open', str(self.validation_path)])

def get_root_path():
    current_script = Path(__file__)
    root = current_script.parent
    while root.name != "GeoValidaTool":
        root = root.parent
        if root == root.parent:
            raise Exception("No se encontró el directorio GeoValidaTool")
    return root

def mostrar_alerta(validation_path):
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    window = AlertWindow(validation_path)
    window.setGeometry(
        QApplication.primaryScreen().geometry().center().x() - window.width() // 2,
        QApplication.primaryScreen().geometry().center().y() - window.height() // 2,
        window.width(),
        window.height()
    )
    window.show()
    app.exec()

def add_exception_column():
    try:
        # Obtener la ruta raíz del proyecto
        root_path = get_root_path()
        
        # Construir la ruta al directorio de validaciones
        validaciones_path = root_path / "Files" / "Temporary_Files" / "MODELO_LADM_1_2" / "Validaciones_Calidad"
        
        # Verificar que el directorio existe
        if not validaciones_path.exists():
            raise Exception(f"El directorio {validaciones_path} no existe")
        
        # Iterar sobre todos los subdirectorios
        for subdir in validaciones_path.glob("*"):
            if subdir.is_dir():
                print(f"Procesando directorio: {subdir}")
                
                # Buscar todos los archivos .shp en el subdirectorio
                for shapefile in subdir.glob("*.shp"):
                    print(f"Procesando shapefile: {shapefile}")
                    
                    try:
                        # Verificar si la columna ya existe
                        field_names = [field.name for field in arcpy.ListFields(str(shapefile))]
                        
                        if "isExceptio" not in field_names:
                            # Agregar el campo si no existe
                            arcpy.AddField_management(
                                str(shapefile),
                                "isExceptio",
                                "SHORT",
                                field_length=1
                            )
                            
                            # Calcular el valor predeterminado de 0
                            arcpy.CalculateField_management(
                                str(shapefile),
                                "isExceptio",
                                "0"
                            )
                            print(f"Columna 'isExceptio' agregada exitosamente a {shapefile}")
                        else:
                            print(f"La columna 'isExceptio' ya existe en {shapefile}")
                            
                    except Exception as e:
                        print(f"Error procesando {shapefile}: {str(e)}")
                        continue
        
        print("Proceso completado exitosamente")
        # Mostrar la ventana de alerta al finalizar
        mostrar_alerta(str(validaciones_path))
        
    except Exception as e:
        print(f"Error general: {str(e)}")

if __name__ == "__main__":
    add_exception_column()