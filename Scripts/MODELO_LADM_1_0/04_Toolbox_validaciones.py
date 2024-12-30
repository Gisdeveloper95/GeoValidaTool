#!/usr/bin/env python
# -*- coding: utf-8 -*-
import arcpy
import os
import glob
import shutil
import logging
from datetime import datetime

def print_banner(message):
    """Imprime un mensaje importante de manera simple"""
    logging.info("\n" + "=" * 80)
    logging.info(message.center(80))
    logging.info("=" * 80 + "\n")

class GDBProcessor(object):
    def setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(message)s'
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(message)s'))
        logging.getLogger('').addHandler(console)

    def log_message(self, message):
        """Registra un mensaje en el log"""
        logging.info(message)
        print(message)

    def log_error(self, error):
        """Registra un error en el log"""
        error_msg = "ERROR: %s" % error
        logging.error(error_msg)
        print(error_msg)

    def __init__(self):
        # Primero configuramos las rutas
        script_dir = os.path.dirname(os.path.abspath(__file__))
        current_dir = script_dir
        while current_dir and not os.path.basename(current_dir) == "GeoValidaTool":
            parent = os.path.dirname(current_dir)
            if parent == current_dir:  # Llegamos a la raiz
                raise Exception("No se pudo encontrar el directorio GeoValidaTool")
            current_dir = parent
            
        base_dir = current_dir
        
        # Definir todas las rutas necesarias
        self.temp_path = os.path.join(base_dir, "Files", "Temporary_Files", "MODELO_LADM_1_0")
        self.transform_path = os.path.join(self.temp_path, "Transformacion_Datos")
        self.base_datos_path = os.path.join(self.transform_path, "BASE_DE_DATOS")
        self.toolbox_path = os.path.join(base_dir, "Files", "Templates", "MODELO_LADM_1_0", "XTFGDB.tbx")
        
        # Configurar el logging después de tener las rutas
        self.start_time = datetime.now()
        self.log_file = os.path.join(self.temp_path, "procesamiento_log_%s.txt" % 
                                    self.start_time.strftime('%Y%m%d_%H%M%S'))
        
        # Asegurar que existe el directorio para el log
        log_dir = os.path.dirname(self.log_file)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        self.setup_logging()
        
        # Definicion de los modelos
        self.model_names = {
            "1_Reporte_Topologia_rural": {
                "name": "Model2_XTFGDB",
                "params": ["topology_path", "output_path"]
            },
            "2_Reporte_Topologia_urbano": {
                "name": "Model3_XTFGDB",
                "params": ["output_path", "topology_path"]
            },
            "3_Unidades_construccion_superpuestas": {
                "name": "Model4_XTFGDB",
                "params": ["rural_path", "urban_path", "gdb_path"]
            },
            "4_Inconsitencias_superposicion_titularidad": {
                "name": "Model5_XTFGDB",
                "params": ["output_path"]
            },
            "5_Superposicion_direccion": {
                "name": "Model6_XTFGDB",
                "params": ["output_path"]
            },
            "1_Temporales": {
                "name": "Model7_XTFGDB",
                "params": ["output_path"]
            },
            "2_Reportes_submodelo_cartografia": {
                "name": "Model13_XTFGDB",
                "params": ["output_path"]
            }
        }
        
        # Iniciar el proceso después de tener todo configurado
        print_banner("INICIANDO PROCESO DE TRANSFORMACION DE DATOS")
        self.log_message("Fecha y hora de inicio: %s" % self.start_time.strftime('%Y-%m-%d %H:%M:%S'))
        self.log_message("Archivo de log: %s" % self.log_file)
        
        try:
            self.log_message("\nVerificando rutas principales:")
            self.log_message("- Directorio temporal: %s" % self.temp_path)
            self.log_message("- Directorio transformacion: %s" % self.transform_path)
            self.log_message("- Directorio BASE_DE_DATOS: %s" % self.base_datos_path)
            self.log_message("- Ruta de toolbox: %s" % self.toolbox_path)
            
            self.log_message("\nImportando toolbox...")
            arcpy.ImportToolbox(self.toolbox_path)
            self.log_message("Toolbox importada exitosamente desde: %s" % self.toolbox_path)
        except Exception as e:
            raise Exception("Error al inicializar: %s" % str(e))
                            
    def clean_and_setup_directories(self):
        """Limpia y configura los directorios necesarios"""
        try:
            print_banner("PREPARANDO DIRECTORIOS Y ARCHIVOS")
            
            # 1. Buscar GPKG y GDB duplicada
            self.log_message("Buscando archivos GPKG en: %s" % self.temp_path)
            gpkg_files = glob.glob(os.path.join(self.temp_path, "*.gpkg"))
            if gpkg_files:
                gpkg_path = gpkg_files[0]
                gpkg_name = os.path.splitext(os.path.basename(gpkg_path))[0]
                self.log_message("GPKG encontrado: %s" % gpkg_path)
                
                duplicate_gdb = os.path.join(self.temp_path, gpkg_name + ".gdb")
                if os.path.exists(duplicate_gdb):
                    self.log_message("Eliminando GDB duplicada: %s" % duplicate_gdb)
                    shutil.rmtree(duplicate_gdb)
                    self.log_message("GDB duplicada eliminada exitosamente")
            else:
                self.log_message("No se encontraron archivos GPKG")

            # 2. Crear directorio BASE_DE_DATOS
            self.log_message("\nConfigurando directorios de trabajo...")
            if not os.path.exists(self.base_datos_path):
                os.makedirs(self.base_datos_path)
                self.log_message("Directorio BASE_DE_DATOS creado en: %s" % self.base_datos_path)

            # 3. Copiar y renombrar GDB restante
            self.log_message("\nBuscando GDB para procesar...")
            remaining_gdbs = [f for f in os.listdir(self.temp_path) 
                            if f.endswith('.gdb') and os.path.isdir(os.path.join(self.temp_path, f))]
            
            if remaining_gdbs:
                source_gdb = os.path.join(self.temp_path, remaining_gdbs[0])
                target_gdb = os.path.join(self.base_datos_path, "GDB.gdb")
                self.log_message("GDB encontrada: %s" % source_gdb)
                
                if os.path.exists(target_gdb):
                    self.log_message("Eliminando GDB.gdb existente...")
                    shutil.rmtree(target_gdb)
                
                self.log_message("Copiando GDB a: %s" % target_gdb)
                shutil.copytree(source_gdb, target_gdb)
                self.log_message("GDB copiada y renombrada exitosamente")

            # Crear directorios adicionales
            self.log_message("\nCreando directorios para inconsistencias...")
            inconsistencias_paths = {
                "RURALES": os.path.join(self.base_datos_path, "INCONSITENCIAS_RURALES"),
                "URBANAS": os.path.join(self.base_datos_path, "INCONSITENCIAS_URBANAS")
            }
            for tipo, path in inconsistencias_paths.items():
                if not os.path.exists(path):
                    os.makedirs(path)
                    self.log_message("Directorio INCONSISTENCIAS_%s creado en: %s" % (tipo, path))

        except Exception as e:
            self.log_error("Error en setup de directorios: %s" % str(e))
            raise

    def execute_model(self, model_display_name):
        """Ejecuta un modelo especifico con sus parametros correspondientes"""
        try:
            print_banner("EJECUTANDO MODELO: %s" % model_display_name)
            
            model_info = self.model_names[model_display_name]
            model_name = model_info["name"]
            
            self.log_message("Nombre del modelo: %s" % model_name)
            self.log_message("Configurando parametros...")
            
            # Configurar parametros según el modelo
            params = []
            if model_display_name == "1_Reporte_Topologia_rural":
                topology_path = os.path.join(self.base_datos_path, "GDB.gdb", "RURAL_CTM12", "RURAL_CTM12_Topology")
                params = [topology_path, self.base_datos_path]
                self.log_message("- Topologia rural: %s" % topology_path)
                self.log_message("- Ruta salida: %s" % self.base_datos_path)
                
            elif model_display_name == "2_Reporte_Topologia_urbano":
                topology_path = os.path.join(self.base_datos_path, "GDB.gdb", "URBANO_CTM12", "URBANO_CTM12_Topology")
                params = [self.base_datos_path, topology_path]
                self.log_message("- Ruta salida: %s" % self.base_datos_path)
                self.log_message("- Topologia urbana: %s" % topology_path)
                
            elif model_display_name == "3_Unidades_construccion_superpuestas":
                rural_path = os.path.join(self.base_datos_path, "INCONSITENCIAS_RURALES")
                urban_path = os.path.join(self.base_datos_path, "INCONSITENCIAS_URBANAS")
                params = [rural_path, urban_path, self.base_datos_path]
                self.log_message("- Ruta inconsistencias rurales: %s" % rural_path)
                self.log_message("- Ruta inconsistencias urbanas: %s" % urban_path)
                self.log_message("- Ruta GDB: %s" % self.base_datos_path)
                
            elif model_display_name == "4_Inconsitencias_superposicion_titularidad":
                params = [self.transform_path]
                self.log_message("- Ruta transformacion: %s" % self.transform_path)
                
            else:  # Modelos con un solo parametro
                params = [self.base_datos_path]
                self.log_message("- Ruta BASE_DE_DATOS: %s" % self.base_datos_path)

            self.log_message("\nEjecutando modelo...")
            tool = getattr(arcpy, model_name)
            result = tool(*params)
            self.log_message("Modelo completado exitosamente")
            return True
            
        except Exception as e:
            self.log_error("Error en modelo %s: %s" % (model_display_name, str(e)))
            return False

    def run_processing(self):
        """Ejecuta todo el proceso"""
        try:
            # Preparar directorios
            self.clean_and_setup_directories()
            
            # Ejecutar modelos en secuencia
            models_to_run = [
                "1_Reporte_Topologia_rural",
                "2_Reporte_Topologia_urbano",
                "3_Unidades_construccion_superpuestas",
                "4_Inconsitencias_superposicion_titularidad",
                "5_Superposicion_direccion",
                "1_Temporales",
                "2_Reportes_submodelo_cartografia"
            ]
            
            print_banner("INICIANDO EJECUCION DE MODELOS")
            self.log_message("Se ejecutaran %d modelos en secuencia" % len(models_to_run))
            
            for i, model in enumerate(models_to_run, 1):
                self.log_message("\nProcesando modelo %d de %d" % (i, len(models_to_run)))
                self.execute_model(model)
            
            end_time = datetime.now()
            elapsed_time = end_time - self.start_time
            
            print_banner("PROCESO COMPLETADO")
            self.log_message("Fecha y hora de finalizacion: %s" % end_time.strftime('%Y-%m-%d %H:%M:%S'))
            self.log_message("Tiempo total de ejecucion: %s" % str(elapsed_time))
            
        except Exception as e:
            self.log_error("Error en procesamiento: %s" % str(e))
            raise

def main():
    processor = GDBProcessor()
    processor.run_processing()

if __name__ == "__main__":
    main()