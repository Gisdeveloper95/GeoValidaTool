import os
import arcpy
import pandas as pd
import sqlite3
import openpyxl
from pathlib import Path
import json
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                              QPushButton, QLabel, QFrame)
from PySide6.QtCore import Qt, QTimer, QPropertyAnimation, QEasingCurve, QSize, Property
from PySide6.QtGui import QColor, QPalette, QFont, QIcon
import sys
import subprocess
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
        "No se encontró el directorio raíz del proyecto.\n"
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
        
        # Verificar permisos antes de crear
        parent_dir = ruta.parent
        if parent_dir.exists() and not os.access(parent_dir, os.W_OK):
            raise PermissionError(f"No tiene permisos de escritura en: {parent_dir}")
        
        os.makedirs(ruta, exist_ok=True)
        
        if not ruta.exists():
            raise Exception(f"No se pudo crear el directorio: {ruta}")
            
        if not os.access(ruta, os.W_OK):
            raise PermissionError(f"No tiene permisos de escritura en: {ruta}")
            
        print(f"Directorio de inconsistencias creado/verificado en: {ruta}")
        return ruta
        
    except Exception as e:
        print(f"Error al crear directorio de inconsistencias: {str(e)}")
        raise

def obtener_excel_topologia(raiz_proyecto, dataset):
    """
    Encuentra el archivo Excel en el directorio de topología para el dataset específico.
    Solo copia desde Templates si no existe en el directorio destino.
    """
    ruta_topologia = raiz_proyecto / 'Files' / 'Temporary_Files' / 'MODELO_IGAC' / '02_TOPOLOGIA' / dataset
    print(f"Buscando Excel en: {ruta_topologia}")
    
    # Verificar si existe el directorio de destino, si no, crearlo
    if not ruta_topologia.exists():
        try:
            os.makedirs(ruta_topologia)
            print(f"Directorio creado: {ruta_topologia}")
        except Exception as e:
            raise Exception(f"No se pudo crear el directorio: {ruta_topologia}. Error: {str(e)}")
    
    # Buscar Excel en la ruta de destino
    archivos_excel = list(ruta_topologia.glob('*.xlsx'))
    
    # SOLO si no hay archivo Excel en el destino, copiar desde Templates
    if not archivos_excel:
        print("No se encontró Excel en la ruta de destino. Buscando en Templates...")
        ruta_template = raiz_proyecto / 'Files' / 'Templates' / '02_TOPOLOGIA' / dataset
        
        if not ruta_template.exists():
            raise Exception(f"No se encontró el directorio de templates: {ruta_template}")
        
        archivos_excel_template = list(ruta_template.glob('*.xlsx'))
        
        if not archivos_excel_template:
            raise Exception(f"No se encontró archivo Excel en templates: {ruta_template}")
        
        try:
            import shutil
            archivo_origen = archivos_excel_template[0]
            archivo_destino = ruta_topologia / archivo_origen.name
            
            # Solo copiar si no existe el archivo en destino
            if not archivo_destino.exists():
                print(f"Copiando Excel desde: {archivo_origen}")
                print(f"Hacia: {archivo_destino}")
                shutil.copy2(archivo_origen, archivo_destino)
                print("Archivo copiado exitosamente")
            
            archivos_excel = [archivo_destino]
            
        except Exception as e:
            raise Exception(f"Error al copiar el archivo Excel: {str(e)}")
    else:
        print(f"Se encontró Excel existente en: {archivos_excel[0]}")
    
    # Verificar que el archivo es accesible
    if not os.access(archivos_excel[0], os.R_OK):
        raise PermissionError(f"No tiene permisos de lectura en: {archivos_excel[0]}")
    
    return archivos_excel[0]
    
def obtener_datos_sqlite(raiz_proyecto, dataset, columna):
    """Obtiene el valor más reciente de la columna específica en la tabla del dataset"""
    db_path = raiz_proyecto / 'Files' / 'Temporary_Files' / 'MODELO_IGAC'/ 'db' / 'errores_consistencia_formato.db'
    print(f"Conectando a base de datos: {db_path}")
    conn = sqlite3.connect(str(db_path))
    
    query = f"""
    SELECT {columna} 
    FROM {dataset} 
    ORDER BY fecha_proceso DESC 
    LIMIT 1
    """
    
    try:
        resultado = pd.read_sql_query(query, conn)[columna].iloc[0]
        print(f"Valor para columna {columna}: {resultado}")
        return resultado
    except Exception as e:
        print(f"Error al consultar columna {columna}: {str(e)}")
        return 0
    finally:
        conn.close()

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
        for celda, columna in mapeo_celdas[dataset].items():
            print(f"Procesando celda {celda} para columna {columna}")
            # Obtener el valor de la base SQLite
            valor = obtener_datos_sqlite(raiz_proyecto, dataset, columna)
            
            # Actualizar la celda específica en el Excel
            hoja[celda] = valor
        
        # Guardar los cambios
        print(f"Guardando cambios en {ruta_excel}")
        wb.save(ruta_excel)
        print(f"Procesamiento completado para {dataset}")
        
    except Exception as e:
        print(f"Error procesando {dataset}: {str(e)}")


class AnimatedButton(QPushButton):
    def __init__(self, text, parent=None):
        super().__init__(text, parent)
        self.setFixedHeight(45)
        self.setCursor(Qt.PointingHandCursor)
        
        # Estilo base del botón
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
    def __init__(self, raiz_proyecto, dataset):
        super().__init__()
        self.raiz_proyecto = raiz_proyecto
        self.dataset = dataset
        self.initUI()
        
    def initUI(self):
        # Configuración de la ventana principal - Reducida la altura
        self.setWindowTitle("⚠️ Revisión de Excepciones Requerida")
        self.setFixedSize(650, 480)  # Altura reducida a 400
        self.setWindowFlags(Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint)
        self.setAttribute(Qt.WA_TranslucentBackground)

        # Widget central
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Marco principal
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
        
        # Título
        title_label = QLabel("⚠️ REVISIÓN DE EXCEPCIONES CONSISTENCIA FORMATO ⚠️")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("""
            QLabel {
                color: #e74c3c;
                font-size: 22px;
                font-weight: bold;
                padding: 10px;
            }
        """)
        
        # Mensaje principal - Reformateado para mejor visibilidad
        message = """
        <div style='font-size: 15px; line-height: 1.4;'>
            <p><b>Antes de continuar, Por favor:</b></p>
            
            <p style='margin: 5px 0;'><b>Revisa si en los siguientes shape files Hay alguna Excepción</b></p>
            
            <p style='margin: 5px 0;'>Si encuentras excepciones:</p>
            <p style='margin: 3px 0;'>⚠️ NO elimines los registros, en la columna Excepcion_, Justifica la Excepcion</p>
            <p style='margin: 3px 0;'>⚠️ Guarda la edición</p>
            
            <p style='margin: 5px 0;'>Estos shapefiles serán evaluados nuevamente para contar tus Excepciones Identificadas.</p>
            
            <p style='color: #e74c3c; font-weight: bold; font-size: 16px; margin-top: 10px;'>
            ¡Trabaja sobre ellos Y NO LOS CAMBIES DE LUGAR!</p>
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
        
        # Botones
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
        
        # Agregar widgets al layout
        frame_layout.addWidget(title_label)
        frame_layout.addWidget(message_label)
        frame_layout.addLayout(button_layout)
        button_layout.addWidget(self.accept_button)
        button_layout.addWidget(self.cancel_button)
        
        layout.addWidget(main_frame)
        
        # Efecto de sombra
        self.setStyleSheet("""
            QMainWindow {
                background-color: transparent;
            }
        """)
        
        # Animación de entrada
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
        ruta_directorio = self.raiz_proyecto / 'Files' / 'Temporary_Files' / 'MODELO_IGAC' / '03_INCONSISTENCIAS' / 'CONSISTENCIA_FORMATO' / self.dataset
        
        if ruta_directorio.exists():
            if os.name == 'nt':  # Windows
                os.startfile(str(ruta_directorio))
            elif os.name == 'posix':  # macOS y Linux
                subprocess.run(['xdg-open' if os.name == 'posix' else 'open', str(ruta_directorio)])

def mostrar_alerta_y_abrir_directorio(raiz_proyecto, dataset):
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    window = AlertWindow(raiz_proyecto, dataset)
    # Centrar la ventana en la pantalla
    window.setGeometry(
        QApplication.primaryScreen().geometry().center().x() - window.width() // 2,
        QApplication.primaryScreen().geometry().center().y() - window.height() // 2,
        window.width(),
        window.height()
    )
    window.show()
    app.exec()

    
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

    # Mapeo de celdas y columnas en la tabla
    MAPEO_CELDAS = {
        
        "URBANO_CTM12":{
            "G5": "U_BARRIO_CTM12",
            "G6": "U_SECTOR_CTM12",
            "G7": "U_MANZANA_CTM12",
            "G8": "U_TERRENO_CTM12",
            "G9": "U_CONSTRUCCION_CTM12",
            "G10": "U_UNIDAD_CTM12",
            "G11": "U_NOMEN_DOMICILIARIA_CTM12",
            "G12": "U_NOMENCLATURA_VIAL_CTM12",
            "G13": "U_MANZANA_CTM12_U_SECTOR_CTM12",
            "G14": "U_TERRENO_CTM12_U_MANZANA_CTM12",
            "G15": "U_CONSTRUCCION_CTM12_U_TERRENO_CTM12",
            "G16": "U_UNIDAD_CTM12_U_CONSTRUCCION_CTM12",
            "G17": "U_UNIDAD_CTM12_U_TERRENO_CTM12",
            "G18": "U_NOMEN_DOMICILIARIA_CTM12_U_TERRENO_CTM12"

            
        },
        
        "RURAL_CTM12":
        {
            "G20": "R_SECTOR_CTM12",
            "G21": "R_VEREDA_CTM12",
            "G22": "R_TERRENO_CTM12",
            "G23": "R_CONSTRUCCION_CTM12",
            "G24": "R_UNIDAD_CTM12",
            "G25": "R_NOMEN_DOMICILIARIA_CTM12",
            "G26": "R_NOMENCLATURA_VIAL_CTM12",
            "G27": "R_VEREDA_CTM12_R_SECTOR_CTM12",
            "G28": "R_TERRENO_CTM12_R_VEREDA_CTM12",
            "G29": "R_CONSTRUCCION_CTM12_R_TERRENO_CTM12",
            "G30": "R_UNIDAD_CTM12_R_CONSTRUCCION_CTM12",
            "G31": "R_UNIDAD_CTM12_R_TERRENO_CTM12",
            "G32": "R_NOMEN_DOMICILIARIA_CTM12_R_TERRENO_CTM12",           
        },
        
        "URBANO": {
            "G5": "U_BARRIO",
            "G6": "U_SECTOR",
            "G7": "U_MANZANA",
            "G8": "U_TERRENO",
            "G9": "U_CONSTRUCCION",
            "G10": "U_UNIDAD",
            "G11": "U_NOMENCLATURA_DOMICILIARIA",
            "G12": "U_NOMENCLATURACLATURA_VIAL",
            "G13": "U_MANZANA_U_SECTOR",
            "G14": "U_TERRENO_U_MANZANA",
            "G15": "U_CONSTRUCCION_U_TERRENO",
            "G16": "U_UNIDAD_U_CONSTRUCCION",
            "G17": "U_UNIDAD_U_TERRENO",
            "G18": "U_NOMENCLATURA_DOMICILIARIA_U_TERRENO"

        },
        "RURAL": {
            "G20": "R_SECTOR",
            "G21": "R_VEREDA",
            "G22": "R_TERRENO",
            "G23": "R_CONSTRUCCION",
            "G24": "R_UNIDAD",
            "G25": "R_NOMENCLATURA_DOMICILIARIA",
            "G26": "R_NOMENCLATURACLATURA_VIAL",
            "G27": "R_VEREDA_R_SECTOR",
            "G28": "R_TERRENO_R_VEREDA",
            "G29": "R_CONSTRUCCION_R_TERRENO",
            "G30": "R_UNIDAD_R_CONSTRUCCION",
            "G31": "R_UNIDAD_R_TERRENO",
            "G32": "R_NOMENCLATURA_DOMICILIARIA_R_TERRENO"

        }
    }

    try:
        # Encontrar la raíz del proyecto
        raiz_proyecto = encontrar_raiz_proyecto()
        print(f"Raíz del proyecto encontrada: {raiz_proyecto}")

        # Crear directorio de inconsistencias
        dir_inconsistencias = crear_directorio_inconsistencias(raiz_proyecto)
        print(f"Directorio de inconsistencias creado: {dir_inconsistencias}")

        # Procesar cada dataset
        for dataset in DATASETS_TO_PROCESS:
            if dataset in MAPEO_CELDAS:
                print(f"\nProcesando dataset: {dataset}")
                procesar_dataset(raiz_proyecto, dataset, MAPEO_CELDAS)

    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")
        
    try:
        # Encontrar la raíz del proyecto
        raiz_proyecto = encontrar_raiz_proyecto()
        print(f"Raíz del proyecto encontrada: {raiz_proyecto}")

        # Crear directorio de inconsistencias
        dir_inconsistencias = crear_directorio_inconsistencias(raiz_proyecto)
        print(f"Directorio de inconsistencias creado: {dir_inconsistencias}")

        # Procesar cada dataset
        for dataset in DATASETS_TO_PROCESS:
            if dataset in MAPEO_CELDAS:
                print(f"\nProcesando dataset: {dataset}")
                procesar_dataset(raiz_proyecto, dataset, MAPEO_CELDAS)
        
        # Después de procesar todos los datasets, mostrar la alerta para URBANO_CTM12
        if "URBANO_CTM12" in DATASETS_TO_PROCESS:
            mostrar_alerta_y_abrir_directorio(raiz_proyecto, "URBANO_CTM12")

    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")

if __name__ == "__main__":
    main()