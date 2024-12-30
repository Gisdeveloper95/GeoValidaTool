#!/usr/bin/env python
# -*- coding: utf-8 -*-
import arcpy
import os
import datetime
import logging
import shutil
from arcpy import env

def print_banner(message):
    """Imprime un mensaje importante de manera simple"""
    logging.info("\n" + "=" * 40)
    logging.info(message)
    logging.info("=" * 40 + "\n")

class MigrarBases(object):
    def __init__(self):
        # Obtener la ruta del script actual
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Buscar el directorio GeoValidaTool
        current_dir = script_dir
        while current_dir and not os.path.basename(current_dir) == "GeoValidaTool":
            parent = os.path.dirname(current_dir)
            if parent == current_dir:  # Llegamos a la raíz
                raise Exception("No se pudo encontrar el directorio GeoValidaTool")
            current_dir = parent
            
        base_dir = current_dir
        
        # Configurar rutas relativas desde base_dir
        self.temp_path = os.path.join(base_dir, "Files", "Temporary_Files", "MODELO_LADM_1_0")
        self.input_folder = os.path.join(self.temp_path, "Transformacion_Datos", "TEMPORAL")
        self.output_folder = self.temp_path
        
        # Asegurar que las carpetas existan
        for path in [self.temp_path, self.input_folder, self.output_folder]:
            if not os.path.exists(path):
                os.makedirs(path)
        
        # Configurar logging
        self.log_file = os.path.join(self.temp_path, "migracion_log_{0}.txt".format(
            datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
        self.setup_logging()
        
        # Log de rutas para verificación
        self.log_message("Rutas configuradas:")
        self.log_message("Script ubicado en: {0}".format(script_dir))
        self.log_message("Directorio base GeoValidaTool: {0}".format(base_dir))
        self.log_message("Carpeta temporal: {0}".format(self.temp_path))
        self.log_message("Carpeta entrada: {0}".format(self.input_folder))
        self.log_message("Carpeta salida: {0}".format(self.output_folder))
        
        self.start = datetime.datetime.now()

    def setup_logging(self):
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(message)s'))
        logging.getLogger('').addHandler(console)

    def log_message(self, message):
        logging.info(message)
        print(message)

    def log_error(self, error):
        logging.error(error)
        print("Error: %s" % error)

    def find_input_gdb(self):
        """Buscar la GDB en el directorio temporal"""
        if not os.path.exists(self.temp_path):
            raise Exception("El directorio temporal no existe: {0}".format(self.temp_path))
            
        for file in os.listdir(self.temp_path):
            if file.endswith('.gdb'):
                self.log_message("GDB encontrada: {0}".format(file))
                return file
                
        # Si no encuentra, mostrar contenido del directorio
        self.log_message("Contenido del directorio temporal:")
        for file in os.listdir(self.temp_path):
            self.log_message("- {0}".format(file))
            
        raise Exception("No se encontró una GDB en el directorio temporal: {0}".format(self.temp_path))
    
    def ejecutar(self):
        try:
            print_banner("INICIANDO PROCESO DE MIGRACIÓN")
            
            # Obtener nombre de la GDB original y crear nombre de salida
            input_gdb_name = self.find_input_gdb()
            out_gdb_name = os.path.splitext(input_gdb_name)[0] + "_LADM_1_0.gdb"
            output_gdb_path = os.path.join(self.output_folder, out_gdb_name)
            template_path = os.path.join(self.temp_path, input_gdb_name)
            
            self.log_message("GDB entrada: {0}".format(template_path))
            self.log_message("GDB salida: {0}".format(output_gdb_path))
            
            # Crear GDB de salida desde template
            if os.path.exists(output_gdb_path):
                shutil.rmtree(output_gdb_path)
            
            if not os.path.exists(template_path):
                raise Exception("No se encontró la GDB template en: {0}".format(template_path))
                
            shutil.copytree(template_path, output_gdb_path)
            
            # Procesar todas las GDBs en la carpeta de entrada
            gdbs_encontradas = False
            for file in os.listdir(self.input_folder):
                if file.endswith('.gdb'):
                    gdbs_encontradas = True
                    fgdb_path = os.path.join(self.input_folder, file)
                    self.log_message("Procesando GDB: {0}".format(fgdb_path))
                    self.convertir_municipio(fgdb_path, output_gdb_path)
            
            if not gdbs_encontradas:
                raise Exception("No se encontraron GDBs para procesar en: {0}".format(self.input_folder))
            
            self.stop = datetime.datetime.now()
            print_banner("PROCESO COMPLETADO")
            self.log_message("Tiempo total: {0}".format(self.stop - self.start))
            
        except Exception as e:
            self.log_error(str(e))
            raise e

    def convertir_municipio(self, fgdb, output_gdb_path):
        
        self.log_message("**************************************************************************")
        self.log_message("********************** convertir_municipio ****************************** ")
        self.log_message("** fgdb:" + fgdb)
        self.log_message("** output_gdb_path:" + output_gdb_path)
        self.log_message("Unir features")
        ##############################################################################
        features = [
			{"src_feature" : "barrio", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_BARRIO_CTM12" },
			{"src_feature" : "centropoblado", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_CENTRO_POBLADO_CTM12"}, 
		        {"src_feature" : "construccionU", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_CONSTRUCCION_CTM12"},
			{"src_feature" : "construccionU_Info", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_CONSTRUCCION_INFORMAL"},
			{"src_feature" : "corregimiento", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_CORREGIMIENTO_CTM12"},
			{"src_feature" : "Limite_municipio", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_LIMITEMUN_CTM12"},
			{"src_feature" : "Localidad_comuna", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_LOCALIDADCOM_CTM12"},
			{"src_feature" : "Manzana", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_MANZANA_CTM12"},
			{"src_feature" : "Perimetro", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_PERIMETRO_CTM12"},
			{"src_feature" : "Sector_urbano", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_SECTOR_CTM12"},
			{"src_feature" : "terrenoU", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_TERRENO_CTM12"},
			{"src_feature" : "terrenoU_info", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_TERRENO_INFORMAL"},
			{"src_feature" : "unidadU", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_UNIDAD_CTM12"},
			{"src_feature" : "unidadU_info", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_UNIDAD_INFORMAL"},
			{"src_feature" : "U_nomenclatura_for", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_DIRECCION_FORMAL_CTM12"},
			{"src_feature" : "U_nomenclatura_info", "trg_ds": "URBANO_CTM12" , "trg_feature": "U_DIRECCION_INFORMAL_CTM12"},
			
			{"src_feature" : "centropoblado", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_CENTRO_POBLADO_CTM12"}, 
            {"src_feature" : "construccionR", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_CONSTRUCCION_CTM12"},
			
			{"src_feature" : "corregimiento", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_CORREGIMIENTO_CTM12"},
			{"src_feature" : "Limite_municipio", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_LIMITEMUN_CTM12"},
			{"src_feature" : "Vereda", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_VEREDA_CTM12"},
			{"src_feature" : "Sector_rural", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_SECTOR_CTM12"},
			{"src_feature" : "terrenoR", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_TERRENO_CTM12"},
			{"src_feature" : "terrenoR_info", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_TERRENO_INFORMAL"},
			{"src_feature" : "unidadR", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_UNIDAD_CTM12"},
			{"src_feature" : "unidadR_info", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_UNIDAD_INFORMAL"},
			{"src_feature" : "R_nomenclatura_for", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_DIRECCION_FORMAL_CTM12"},
			{"src_feature" : "R_nomenclatura_info", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_DIRECCION_INFORMAL_CTM12"},
			{"src_feature" : "construccionR_Info", "trg_ds": "RURAL_CTM12" , "trg_feature": "R_CONSTRUCCION_INFORMAL"}


         ]

        self.log_message("****************************************************************")
        self.log_message("Creando fgdb de salida...")
        #output_gdb_path = os.path.join(output_gdb_path,code_municipality +".gdb")
        self.log_message("** output_gdb_path:" + output_gdb_path)

        #if  os.path.exists(output_gdb_path):
        #    shutil.rmtree(output_gdb_path)
        #shutil.copytree(fgdb_template, output_gdb_path) 
        #self.log_message("****************************************************************")

        self.log_message("****************************************************************")
        self.log_message("Unir features...")

        
   
        for row in features:
            try: 
	        self.log_message("***********************************")
                self.log_message("** feature: {} ".format(row) )
		
                source_feature = row["src_feature"]
                target_ds = row["trg_ds"]
	        target_feature = row["trg_feature"]

                env.workspace = fgdb 
                self.log_message("Cargando datos... ")             
                sourceFeatures = fgdb         + '/' + source_feature
                outputFeature =  output_gdb_path   + '/' + target_ds + '/'  + target_feature
                
                edit = arcpy.da.Editor(output_gdb_path)
		edit.startEditing(False, True)  # Modifica los parámetros según sea necesario
		edit.startOperation()
                
		if arcpy.Exists(sourceFeatures):
                    self.log_message("sourceFeature:" + sourceFeatures)
                    self.log_message("outputFeature:" + outputFeature)
                    sourceCountFeatures = int(arcpy.GetCount_management(sourceFeatures).getOutput(0))
                    self.log_message("Feature: "+ source_feature +" -  Cantidad :" + str(sourceCountFeatures))
    
    		    if sourceCountFeatures > 0:
			arcpy.management.AddSpatialIndex(sourceFeatures)
		        arcpy.Append_management(sourceFeatures, outputFeature , 'NO_TEST')
        	        self.log_message("Union exitosa para la caracteristica: " + source_feature)
		        
                else:
                    self.log_message("La característica de origen " + source_feature + " no existe.")
	        
                edit.stopOperation()
                edit.stopEditing(True)              
    
            except Exception as e:
                self.log_error(e)
        
        pass

def main():
    migrador = MigrarBases()
    migrador.ejecutar()

if __name__ == "__main__":
    main()