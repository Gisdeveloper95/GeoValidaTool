import arcpy
import os
import glob
import logging
from datetime import datetime

class ValidationToolbox:
    def __init__(self):
        # Obtener los datasets a procesar desde el archivo de configuración
        self.tipos_validacion = self.load_datasets_config()
        
        # Diccionario de nombres de modelos
        self.model_names = {
            "1_Topologia_Rural": {
                "name": "Model_VALIDACIONESCALIDAD",
                "description": "Validación de Topología Rural",
                "needs_topology": True,
                "topology_type": "normal"
            },
            "2_Topologia_Urbana": {
                "name": "Model2_VALIDACIONESCALIDAD",
                "description": "Validación de Topología Urbana",
                "needs_topology": True,
                "topology_type": "normal"
            },
            "3_Unidades_Rurales_Superpuestas": {
                "name": "Model3_VALIDACIONESCALIDAD",
                "description": "Validación de Unidades Rurales Superpuestas",
                "needs_topology": False
            },
            "4_Unidades_Urbanas_Superpuestas": {
                "name": "Model1_VALIDACIONESCALIDAD",
                "description": "Validación de Unidades Urbanas Superpuestas",
                "needs_topology": False
            },
            "5_Inconsistencias_submodelo_cartografia_rural": {
                "name": "Model6_VALIDACIONESCALIDAD",
                "description": "Validación de Inconsistencias en Cartografía Rural",
                "needs_topology": False
            },
            "6_Inconsistencias_submodelo_cartografia_urbano": {
                "name": "Model5_VALIDACIONESCALIDAD",
                "description": "Validación de Inconsistencias en Cartografía Urbana",
                "needs_topology": False
            },
            "7_Titularidad_Rural(20_21)": {
                "name": "Model7_VALIDACIONESCALIDAD",
                "description": "Validación de Titularidad Rural",
                "needs_topology": False
            },
            "8_Titularidad_Urbano(20_21)": {
                "name": "Model4_VALIDACIONESCALIDAD",
                "description": "Validación de Titularidad Urbana",
                "needs_topology": False
            },
            "9_Inconsistencias_Zonas_Homogeneas_Rurales": {
                "name": "Mode9_VALIDACIONESCALIDAD",
                "description": "Validación de Inconsistencias en Zonas Homogéneas Rurales",
                "needs_topology": True,
                "topology_type": "zonas_homogeneas"
            },
            "10_Inconsistencias_Zonas_Homogeneas_Urbanas": {
                "name": "Model9_VALIDACIONESCALIDAD",
                "description": "Validación de Inconsistencias en Zonas Homogéneas Urbanas",
                "needs_topology": True,
                "topology_type": "zonas_homogeneas"
            }
        }
        
        # Configuración de rutas
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
        self.temp_path = os.path.join(base_dir, "GeoValidaTool", "Files", "Temporary_Files", "MODELO_INTERNO_1_0")
        self.toolbox_path = os.path.join(base_dir, "GeoValidaTool", "Files", "Templates", "MODELO_INTERNO_1_0", "VALIDACIONESCALIDAD.atbx")
        
        # Configurar logging
        self.log_file = os.path.join(self.temp_path, f"validacion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        self.setup_logging()
        
        # Importar toolbox
        try:
            arcpy.ImportToolbox(self.toolbox_path)
        except Exception as e:
            logging.error(f"Error al importar toolbox: {str(e)}")
            raise

    def load_datasets_config(self):
        """Carga la configuración de datasets desde el archivo txt"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
        config_path = os.path.join(proyecto_dir, "Files", "Temporary_Files", "array_config.txt")
        
        datasets_to_process = []
        try:
            with open(config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        dataset_name = line.strip('",[]').strip()
                        if dataset_name:
                            datasets_to_process.append(dataset_name)
            
            return datasets_to_process
            
        except FileNotFoundError:
            raise FileNotFoundError(f"No se encontró el archivo de configuración en: {config_path}")
        except Exception as e:
            raise Exception(f"Error al leer el archivo de configuración: {str(e)}")

    def get_topology_path(self, input_gdb, tipo, topology_type):
        """Obtiene la ruta de topología según el tipo"""
        if topology_type == "normal":
            return os.path.join(input_gdb, tipo, f"{tipo}_Topology")
        elif topology_type == "zonas_homogeneas":
            prefix = "ZONA_HOMOGENEA_RURAL" if "RURAL" in tipo else "ZONA_HOMOGENEA_URBANO"
            return os.path.join(input_gdb, f"{prefix}_CTM12", f"{prefix}_CTM12_Topology")
        return None

    def execute_model(self, model_display_name, input_gdb, output_folder, topology_path=None):
        """Ejecuta un modelo específico de la toolbox"""
        try:
            # Obtener el nombre real del modelo
            model_info = self.model_names[model_display_name]
            model_name = model_info['name']
            
            # Crear subdirectorio específico para ciertos modelos
            if model_display_name in ["3_Unidades_Rurales_Superpuestas", "4_Unidades_Urbanas_Superpuestas"]:
                specific_output = os.path.join(
                    output_folder, 
                    "UNIDADES_RURALES_SUPERPUESTAS" if "Rural" in model_display_name else "UNIDADES_URBANAS_SUPERPUESTAS"
                )
                os.makedirs(specific_output, exist_ok=True)
                output_to_use = specific_output
            else:
                output_to_use = output_folder
            
            # Preparar parámetros según el tipo específico de modelo
            if model_display_name in ["9_Inconsistencias_Zonas_Homogeneas_Rurales", "10_Inconsistencias_Zonas_Homogeneas_Urbanas"]:
                params = [topology_path, input_gdb, output_to_use]
            elif model_display_name in ["4_Unidades_Urbanas_Superpuestas", "8_Titularidad_Urbano(20_21)"]:
                params = [output_to_use, input_gdb]
            elif model_display_name in ["1_Topologia_Rural", "2_Topologia_Urbana"]:
                params = [topology_path, output_to_use]
            else:
                params = [input_gdb, output_to_use]
            
            # Ejecutar el modelo usando el nombre correcto
            tool = getattr(arcpy, model_name)
            result = tool(*params)
            
            return True
            
        except Exception as e:
            logging.error(f"Error al ejecutar el modelo {model_display_name}: {str(e)}")
            return False
    def run_validation(self):
        """Ejecuta el proceso completo de validación"""
        print("Iniciando Validacion Topologica para  Modelo Interno 1.0")
        try:
            # Encontrar GDB de entrada y crear carpeta de salida
            input_gdb = self.find_input_gdb()
            output_folder = self.create_output_folder()
            
            # Validar datasets de topología
            valid_datasets = self.validate_topology_datasets(input_gdb)
            
            # Filtrar solo los datasets que están en la configuración
            valid_datasets = [ds for ds in valid_datasets if ds in self.tipos_validacion]
            if not valid_datasets:
                raise ValueError("Ninguno de los datasets configurados fue encontrado en la geodatabase")
            
            # Definir modelos a ejecutar según los datasets válidos
            models_to_run = []
            if "RURAL_CTM12" in valid_datasets:
                models_to_run.extend([
                    "1_Topologia_Rural",
                    "3_Unidades_Rurales_Superpuestas",
                    "5_Inconsistencias_submodelo_cartografia_rural",
                    "7_Titularidad_Rural(20_21)",
                    "9_Inconsistencias_Zonas_Homogeneas_Rurales"
                ])
            
            if "URBANO_CTM12" in valid_datasets:
                models_to_run.extend([
                    "2_Topologia_Urbana",
                    "4_Unidades_Urbanas_Superpuestas",
                    "6_Inconsistencias_submodelo_cartografia_urbano",
                    "8_Titularidad_Urbano(20_21)",
                    "10_Inconsistencias_Zonas_Homogeneas_Urbanas"
                ])
            
            # Ejecutar modelos
            for model in models_to_run:
                model_info = self.model_names[model]
                if model_info['needs_topology']:
                    tipo = "RURAL_CTM12" if "Rural" in model else "URBANO_CTM12"
                    topology_path = self.get_topology_path(input_gdb, tipo, model_info['topology_type'])
                    self.execute_model(model, input_gdb, output_folder, topology_path)
                else:
                    self.execute_model(model, input_gdb, output_folder)
            
        except Exception as e:
            logging.error(f"Error en la validación: {str(e)}")
            raise
    
    def setup_logging(self):
        """Configura el sistema de logging de manera más minimal"""
        logging.basicConfig(
            filename=self.log_file,
            level=logging.ERROR,  # Cambio: Solo registra errores
            format='%(asctime)s - %(levelname)s: %(message)s'
        )

    def find_input_gdb(self):
        """Busca la geodatabase en la carpeta temporal"""
        gdbs = glob.glob(os.path.join(self.temp_path, "*.gdb"))
        if not gdbs:
            raise FileNotFoundError("No se encontró ninguna geodatabase en la carpeta temporal")
        return gdbs[0]

    def validate_topology_datasets(self, gdb_path):
        """Valida la existencia de los datasets de topología"""
        valid_datasets = []
        
        for tipo in self.tipos_validacion:
            dataset_path = os.path.join(gdb_path, tipo, f"{tipo}_Topology")
            if arcpy.Exists(dataset_path):
                valid_datasets.append(tipo)
                
                # Verificar también la existencia de los datasets de zonas homogéneas
                prefix = "ZONA_HOMOGENEA_RURAL" if "RURAL" in tipo else "ZONA_HOMOGENEA_URBANO"
                zh_topology_path = os.path.join(gdb_path, f"{prefix}_CTM12", f"{prefix}_CTM12_Topology")
                if arcpy.Exists(zh_topology_path):
                    pass  # Solo se valida, sin logging
                
        if not valid_datasets:
            raise ValueError("No se encontraron datasets de topología válidos")
        return valid_datasets

    def create_output_folder(self):
        """Crea la carpeta de salida para los resultados"""
        output_folder = os.path.join(self.temp_path, "Validaciones_Calidad")
        os.makedirs(output_folder, exist_ok=True)
        return output_folder

def main():
    validator = ValidationToolbox()
    validator.run_validation()

if __name__ == "__main__":
    main()
    