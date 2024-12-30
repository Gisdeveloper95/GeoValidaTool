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

class ValidationToolbox:
    def __init__(self):
        self.tipos_validacion = self.load_datasets_config()
        
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
            }
        }
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
        self.temp_path = os.path.join(base_dir, "GeoValidaTool", "Files", "Temporary_Files", "MODELO_LADM_1_2")
        self.toolbox_path = os.path.join(base_dir, "GeoValidaTool", "Files", "Templates", "MODELO_LADM_1_2", "VALIDACIONESCALIDAD.atbx")
        
        self.log_file = os.path.join(self.temp_path, f"validacion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        self.setup_logging()
        
        print_banner("INICIANDO PROCESO DE VALIDACIÓN")
        try:
            arcpy.ImportToolbox(self.toolbox_path)
            logging.info("Toolbox importada exitosamente")
        except Exception as e:
            logging.error(f"Error al importar toolbox: {str(e)}")
            raise

    def load_datasets_config(self):
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
            
            if not datasets_to_process:
                raise ValueError("No se encontraron datasets para procesar")
            return datasets_to_process
            
        except Exception as e:
            raise Exception(f"Error en configuración: {str(e)}")
    
    def get_topology_path(self, input_gdb, tipo, topology_type):
        if topology_type == "normal":
            return os.path.join(input_gdb, tipo, f"{tipo}_Topology")
        return None

    def execute_model(self, model_display_name, input_gdb, output_folder, topology_path=None):
        try:
            logging.info(f"\nEjecutando: {self.model_names[model_display_name]['description']}")
            
            model_info = self.model_names[model_display_name]
            model_name = model_info['name']
            
            if model_display_name == "3_Unidades_Rurales_Superpuestas":
                output_to_use = os.path.join(output_folder, "UNIDADES_RURALES_SUPERPUESTAS")
                os.makedirs(output_to_use, exist_ok=True)
            elif model_display_name == "4_Unidades_Urbanas_Superpuestas":
                output_to_use = os.path.join(output_folder, "UNIDADES_URBANAS_SUPERPUESTAS")
                os.makedirs(output_to_use, exist_ok=True)
            else:
                output_to_use = output_folder
            
            if model_display_name in ["4_Unidades_Urbanas_Superpuestas", "8_Titularidad_Urbano(20_21)"]:
                params = [output_to_use, input_gdb]
            elif model_display_name in ["1_Topologia_Rural", "2_Topologia_Urbana"]:
                params = [topology_path, output_to_use]
            else:
                params = [input_gdb, output_to_use]
            
            tool = getattr(arcpy, model_name)
            result = tool(*params)
            logging.info("Modelo completado exitosamente")
            return True
            
        except Exception as e:
            logging.error(f"Error en modelo: {str(e)}")
            return False
    
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

    def find_input_gdb(self):
        gdbs = glob.glob(os.path.join(self.temp_path, "*.gdb"))
        if not gdbs:
            raise FileNotFoundError("No se encontró geodatabase en carpeta temporal")
        return gdbs[0]

    def validate_topology_datasets(self, gdb_path):
        valid_datasets = []
        for tipo in self.tipos_validacion:
            dataset_path = os.path.join(gdb_path, tipo, f"{tipo}_Topology")
            if arcpy.Exists(dataset_path):
                valid_datasets.append(tipo)
                
        if not valid_datasets:
            raise ValueError("No se encontraron datasets de topología válidos")
        return valid_datasets

    def create_output_folder(self):
        output_folder = os.path.join(self.temp_path, "Validaciones_Calidad")
        os.makedirs(output_folder, exist_ok=True)
        return output_folder

    def run_validation(self):
        try:
            input_gdb = self.find_input_gdb()
            output_folder = self.create_output_folder()
            valid_datasets = self.validate_topology_datasets(input_gdb)
            valid_datasets = [ds for ds in valid_datasets if ds in self.tipos_validacion]
            
            if not valid_datasets:
                raise ValueError("Ningún dataset configurado encontrado en la geodatabase")
            
            models_to_run = []
            if "RURAL_CTM12" in valid_datasets:
                models_to_run.extend([
                    "1_Topologia_Rural",
                    "3_Unidades_Rurales_Superpuestas",
                    "5_Inconsistencias_submodelo_cartografia_rural",
                    "7_Titularidad_Rural(20_21)"
                ])
            
            if "URBANO_CTM12" in valid_datasets:
                models_to_run.extend([
                    "2_Topologia_Urbana",
                    "4_Unidades_Urbanas_Superpuestas",
                    "6_Inconsistencias_submodelo_cartografia_urbano",
                    "8_Titularidad_Urbano(20_21)"
                ])
            
            for model in models_to_run:
                model_info = self.model_names[model]
                if model_info['needs_topology']:
                    tipo = "RURAL_CTM12" if "Rural" in model else "URBANO_CTM12"
                    topology_path = self.get_topology_path(input_gdb, tipo, model_info['topology_type'])
                    self.execute_model(model, input_gdb, output_folder, topology_path)
                else:
                    self.execute_model(model, input_gdb, output_folder)
            
            logging.info("\nPROCESO DE VALIDACIÓN COMPLETADO")
            
        except Exception as e:
            logging.error(f"\nError en validación: {str(e)}")
            raise

def main():
    validator = ValidationToolbox()
    validator.run_validation()

if __name__ == "__main__":
    main()