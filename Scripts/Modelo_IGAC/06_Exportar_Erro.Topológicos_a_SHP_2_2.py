import arcpy
import os
from pathlib import Path
import json
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QLabel, 
                            QPushButton, QFrame, QApplication)
from PySide6.QtCore import Qt, QPropertyAnimation
import shutil
import sys
import subprocess
from TopologyRuleProcessor import TopologyRuleProcessor
import sys
sys.stdout.reconfigure(encoding='utf-8')



def map_shapefiles_to_dataset(temp_folder, dataset, conditions):
    """
    Mapea los shapefiles generados con su dataset correspondiente
    """
    dataset_shapefiles = []
    for input_fc, erase_fc, _ in conditions.get(dataset, []):
        shapefile_name = f"{input_fc}_must_be_covered_by_{erase_fc}.shp"
        shapefile_path = os.path.join(temp_folder, shapefile_name)
        if os.path.exists(shapefile_path):
            dataset_shapefiles.append({
                'shapefile': shapefile_path,
                'input_fc': input_fc,
                'erase_fc': erase_fc
            })
    return dataset_shapefiles

def import_topology_errors(error_gdb, dataset, shapefiles_info):
    """
    Importa los errores topológicos al feature class correspondiente
    """
    error_fc = os.path.join(error_gdb, f"{dataset}_errors_poly")
    
    if not arcpy.Exists(error_fc):
        print(f"WARNING: No se encontró el feature class de errores para {dataset}")
        return

    # Crear un cursor de inserción
    fields = ["OriginObjectClassName", "OriginObjectID", "DestinationObjectClassName", 
              "DestinationObjectID", "RuleType", "RuleDescription", "isException"]
    
    with arcpy.da.InsertCursor(error_fc, ["SHAPE@"] + fields) as cursor:
        for shapefile_info in shapefiles_info:
            shapefile = shapefile_info['shapefile']
            
            # Leer las geometrías del shapefile
            with arcpy.da.SearchCursor(shapefile, ["SHAPE@"]) as search_cursor:
                for row in search_cursor:
                    # Preparar los atributos
                    attributes = [
                        shapefile_info['input_fc'],  # OriginObjectClassName
                        1,  # OriginObjectID (número secuencial)
                        shapefile_info['erase_fc'],  # DestinationObjectClassName
                        1,  # DestinationObjectID
                        "esriTRTAreaCoveredByAreaClass",  # RuleType
                        "Must Be Covered By Feature Class Of",  # RuleDescription
                        0  # isException
                    ]
                    
                    # Insertar la geometría y sus atributos
                    cursor.insertRow(row + tuple(attributes))
def find_project_root():
    """
    Encuentra la raíz del proyecto verificando la estructura de directorios esperada.
    """
    # Empezar desde el directorio del script actual
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    
    # Subir dos niveles para llegar a la raíz del proyecto (desde Modelo_IGAC/script.py hasta GeoValidaTool)
    ruta_proyecto = os.path.abspath(os.path.join(ruta_actual, '..', '..'))
    
    # Verifica la estructura característica del proyecto
    rutas_requeridas = [
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC"),
        os.path.join(ruta_proyecto, "Files", "Temporary_Files", "array_config.txt"),
        os.path.join(ruta_proyecto, "Scripts")
    ]
    
    # Para debug, imprimir las rutas que está verificando
    print("\nVerificando rutas del proyecto:")
    for ruta in rutas_requeridas:
        existe = os.path.exists(ruta)
        print(f"Ruta: {ruta}")
        print(f"¿Existe?: {existe}")
    
    if all(os.path.exists(ruta) for ruta in rutas_requeridas):
        print(f"\nRaíz del proyecto encontrada en: {ruta_proyecto}")
        
        # Crear directorios necesarios si no existen
        topology_errors_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "Topology_Errors")
        temp_topology_dir = os.path.join(ruta_proyecto, "Files", "Temporary_Files", "MODELO_IGAC", "temp_topology")
        
        for directory in [topology_errors_dir, temp_topology_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                print(f"Directorio creado: {directory}")
        
        return Path(ruta_proyecto)  # Devolver como objeto Path
    
    raise Exception(
        "No se encontró la raíz del proyecto.\n"
        "Verifique que está ejecutando el script desde dentro del proyecto "
        "y que existe la siguiente estructura:\n"
        "- Files/Temporary_Files/MODELO_IGAC\n"
        "- Files/Temporary_Files/array_config.txt\n"
        "- Files/Temporary_Files/MODELO_IGAC/Topology_Errors\n"
        "- Files/Temporary_Files/MODELO_IGAC/temp_topology"
    )

def find_gdb(base_path, subfolder=""):
    """
    Busca una geodatabase en la ruta especificada
    """
    if isinstance(base_path, str):
        base_path = Path(base_path)
    
    search_path = base_path / "Files" / "Temporary_Files" / "MODELO_IGAC"
    if subfolder:
        search_path = search_path / subfolder
        
    print(f"Buscando GDB en: {search_path}")
    
    for item in search_path.glob("*.gdb"):
        if item.is_dir():
            print(f"GDB encontrada: {item}")
            return str(item)
            
    raise FileNotFoundError(f"No se encontró ninguna geodatabase en la ruta: {search_path}")

def create_temp_topology_folder(gdb_path):
    """
    Crea la carpeta temp_topology si no existe
    """
    if isinstance(gdb_path, str):
        gdb_path = Path(gdb_path)
        
    topology_folder = gdb_path.parent / "temp_topology"
    topology_folder.mkdir(exist_ok=True)
    print(f"Carpeta de topología temporal creada/verificada en: {topology_folder}")
    return str(topology_folder)
def process_topology():
    # Definir datasets a procesar


    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
        config_path = os.path.join(proyecto_dir,  "Files", "Temporary_Files","array_config.txt")

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


    # Definir condiciones de topología
    conditions = {
        "URBANO_CTM12": {
            ("U_TERRENO_CTM12", "U_ZONA_HOMO_GEOECONOMICA_CTM12", "Must Be Covered By Feature Class Of"),
            ("U_TERRENO_CTM12", "U_ZONA_HOMOGENEA_FISICA_CTM12", "Must Be Covered By Feature Class Of")

        },
        "RURAL_CTM12": {
            ("R_TERRENO_CTM12", "R_ZONA_HOMO_GEOECONOMICA_CTM12", "Must Be Covered By Feature Class Of"),
            ("R_TERRENO_CTM12", "R_ZONA_HOMOGENEA_FISICA_CTM12", "Must Be Covered By Feature Class Of")
        },
        "URBANO": {
            ("U_TERRENO", "U_ZONA_HOMOGENEA_GEOECONOMICA", "Must Be Covered By Feature Class Of"),
            ("U_TERRENO", "U_ZONA_HOMOGENEA_FISICA", "Must Be Covered By Feature Class Of")
            
        },
        "RURAL": {
            ("R_TERRENO", "R_ZONA_HOMOGENEA_GEOECONOMICA", "Must Be Covered By Feature Class Of"),
            ("R_TERRENO", "R_ZONA_HOMOGENEA_FISICA", "Must Be Covered By Feature Class Of")
        }
    }


    try:
        project_path = find_project_root()

        # Encontrar las geodatabases
        gdb_path = find_gdb(project_path)
        error_gdb_path = find_gdb(project_path, "Topology_Errors")
        arcpy.env.workspace = gdb_path

        # Crear carpeta para resultados
        output_folder = create_temp_topology_folder(gdb_path)

        # Procesar cada dataset habilitado
        for dataset in DATASETS_TO_PROCESS:
            if dataset in conditions:
                print(f"\nProcesando dataset: {dataset}")
                
                for input_fc, erase_fc, rule in conditions[dataset]:
                    try:
                        input_path = os.path.join(gdb_path, dataset, input_fc)
                        erase_path = os.path.join(gdb_path, dataset, erase_fc)
                        
                        # Verificar si los feature classes existen y no están vacíos
                        if not arcpy.Exists(input_path):
                            print(f"WARNING: El feature class {input_fc} no existe")
                            continue
                        
                        if not arcpy.Exists(erase_path):
                            print(f"WARNING: El feature class {erase_fc} no existe")
                            continue

                        # Crear nombre del archivo de salida
                        output_name = f"{input_fc}_must_be_covered_by_{erase_fc}.shp"
                        output_path = os.path.join(output_folder, output_name)

                        # Ejecutar Erase
                        print(f"Ejecutando Erase: {input_fc} con {erase_fc}")
                        arcpy.analysis.Erase(input_path, erase_path, output_path)

                        # Verificar si el resultado está vacío
                        result_count = int(arcpy.GetCount_management(output_path)[0])
                        if result_count == 0:
                            print(f"WARNING 000117: Warning empty output generated para {output_name}")
                        else:
                            print(f"Proceso completado exitosamente: {output_name}")

                    except arcpy.ExecuteError:
                        print(f"Error en el procesamiento: {arcpy.GetMessages(2)}")
                        continue
                    except Exception as e:
                        print(f"Error inesperado: {str(e)}")
                        continue

                # Mapear shapefiles para el dataset actual
                dataset_shapefiles = map_shapefiles_to_dataset(output_folder, dataset, conditions)
                
                # Importar errores topológicos
                if dataset_shapefiles:
                    print(f"\nImportando errores topológicos para {dataset}")
                    import_topology_errors(error_gdb_path, dataset, dataset_shapefiles)
                else:
                    print(f"\nNo se encontraron shapefiles para importar en {dataset}")
        
        
        # Ejecutar Topology_Unidades.py
        import Topology_Unidades
        Topology_Unidades.main()
        
        # Mostrar alerta una sola vez al final
        from TopologyRuleProcessor import TopologyRuleProcessor
        processor = TopologyRuleProcessor()
        processor.process_topology_errors()
        
        
        
        project_root = find_project_root()
        mostrar_alerta_y_abrir_directorio(project_root)  

    except Exception as e:
        print(f"Error en el proceso principal: {str(e)}")


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
    def __init__(self, raiz_proyecto):  # Removí el parámetro dataset ya que no lo necesitamos
        super().__init__()
        self.raiz_proyecto = raiz_proyecto
        self.initUI()
        
    def initUI(self):
        # Configuración de la ventana principal
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
        
        title_label = QLabel("⚠️ REVISIÓN DE EXCEPCIONES TOPOLOGICAS⚠️")
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
            <p><b>Antes de continuar, Por favor:</b></p>
            
            <p style='margin: 5px 0;'><b>Revisa si en los siguientes FeaturesClass por si Hay alguna Excepción Topologica</b></p>
            
            <p style='margin: 5px 0;'>Si encuentras excepciones:</p>
            <p style='margin: 3px 0;'>⚠️ Marcar con valor 1 en la Columna Exception las que consideres como Excepciones</p>
            <p style='margin: 3px 0;'>⚠️ Guarda la edición</p>
            
            <p style='margin: 5px 0;'>Esta GDB será evaluada nuevamente para contar tus Excepciones Identificadas.</p>
            
            <p style='color: #e74c3c; font-weight: bold; font-size: 16px; margin-top: 10px;'>
            ¡Trabaja sobre esta GDB Y NO LOS CAMBIES DE LUGAR!</p>
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
        # Modificamos la ruta para que apunte al directorio Topology_Errors
        ruta_directorio = self.raiz_proyecto / 'Files' / 'Temporary_Files' / 'MODELO_IGAC' / 'Topology_Errors'
        
        if ruta_directorio.exists():
            if os.name == 'nt':  # Windows
                os.startfile(str(ruta_directorio))
            elif os.name == 'posix':  # macOS y Linux
                subprocess.run(['xdg-open' if os.name == 'posix' else 'open', str(ruta_directorio)])

def mostrar_alerta_y_abrir_directorio(raiz_proyecto):  # Removí el parámetro dataset
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    window = AlertWindow(raiz_proyecto)  # Ya no pasamos el dataset
    window.setGeometry(
        QApplication.primaryScreen().geometry().center().x() - window.width() // 2,
        QApplication.primaryScreen().geometry().center().y() - window.height() // 2,
        window.width(),
        window.height()
    )
    window.show()
    app.exec() 
    


if __name__ == "__main__":
    process_topology()
