#!/usr/bin/env python
# -*- coding: utf-8 -*-

import arcpy
import os
import glob
import logging
from datetime import datetime

def print_banner(message):
    """Imprime un mensaje importante de manera simple"""
    logging.info("\n" + "=" * 40)
    logging.info(message)
    logging.info("=" * 40 + "\n")

class SimplifiedValidationToolbox(object):
    def __init__(self):
        # Definición de los modelos a ejecutar
        self.model_names = {
            "1_pasar_a_gdb": {
                "name": "Model_XTFGDB",
                "description": "Pasar a GDB",
                "params_count": 2
            },
            "2_estructurar_gdb": {
                "name": "Model1_XTFGDB",
                "description": "Estructurar GDB",
                "params_count": 1
            }
        }
        
        # Configuración de rutas
        script_dir = os.path.dirname(os.path.abspath(__file__))
        current_dir = script_dir
        while current_dir and not os.path.basename(current_dir) == "GeoValidaTool":
            parent = os.path.dirname(current_dir)
            if parent == current_dir:  # Llegamos a la raíz
                raise Exception("No se pudo encontrar el directorio GeoValidaTool")
            current_dir = parent
        
        base_dir = current_dir
        self.temp_path = os.path.join(base_dir, "Files", "Temporary_Files", "MODELO_LADM_1_0")
        self.toolbox_path = os.path.join(base_dir, "Files", "Templates", "MODELO_LADM_1_0", "XTFGDB.tbx")
        
        # Crear directorio temporal si no existe
        if not os.path.exists(self.temp_path):
            os.makedirs(self.temp_path)
        
        # Configuración del logging
        self.log_file = os.path.join(
            self.temp_path, 
            "validacion_log_{0}.txt".format(datetime.now().strftime('%Y%m%d_%H%M%S'))
        )
        self.setup_logging()
        
        # Importar toolbox
        print_banner("INICIANDO PROCESO DE VALIDACION")
        try:
            arcpy.ImportToolbox(self.toolbox_path)
            logging.info("Toolbox importada exitosamente")
        except Exception as e:
            logging.error("Error al importar toolbox: %s" % str(e))
            raise

    def setup_logging(self):
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(message)s'
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(message)s'))
        logging.getLogger('').addHandler(console)

    def find_gpkg_file(self):
        """Buscar archivo GPKG en el directorio temporal"""
        if not os.path.exists(self.temp_path):
            raise Exception("El directorio temporal no existe: %s" % self.temp_path)
            
        gpkg_files = glob.glob(os.path.join(self.temp_path, "*.gpkg"))
        if not gpkg_files:
            # Mostrar contenido del directorio para debug
            logging.info("Contenido del directorio temporal:")
            for file in os.listdir(self.temp_path):
                logging.info("- %s" % file)
            raise Exception("No se encontro archivo GPKG en: %s" % self.temp_path)
            
        logging.info("GPKG encontrado: %s" % gpkg_files[0])
        return gpkg_files[0]

    def execute_model(self, model_display_name, output_folder, gpkg_path=None):
        try:
            logging.info("\nEjecutando: %s" % self.model_names[model_display_name]['description'])
            
            model_info = self.model_names[model_display_name]
            model_name = model_info['name']
            
            if model_display_name == "1_pasar_a_gdb":
                params = [output_folder, gpkg_path]
            else:  # 2_estructurar_gdb
                params = [output_folder]
            
            tool = getattr(arcpy, model_name)
            result = tool(*params)
            logging.info("Modelo completado exitosamente")
            return True
            
        except Exception as e:
            logging.error("Error en modelo: %s" % str(e))
            return False

    def run_validation(self):
        try:
            # Crear carpeta de salida
            output_folder = os.path.join(self.temp_path, "Transformacion_Datos")
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
            
            # Encontrar archivo GPKG
            gpkg_path = self.find_gpkg_file()
            logging.info("Usando archivo GPKG: %s" % gpkg_path)
            
            # Ejecutar modelos en secuencia
            self.execute_model("1_pasar_a_gdb", output_folder, gpkg_path)
            self.execute_model("2_estructurar_gdb", output_folder)
            
            logging.info("\nPROCESO DE VALIDACION COMPLETADO")
            
        except Exception as e:
            logging.error("\nError en validacion: %s" % str(e))
            raise

def main():
    validator = SimplifiedValidationToolbox()
    validator.run_validation()

if __name__ == "__main__":
    main()