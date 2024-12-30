import os



import sys
sys.stdout.reconfigure(encoding='utf-8')


class PythonVersionManager:
    """Gestiona la ejecución de scripts con diferentes versiones de Python"""
    
    def __init__(self):
        self.python3_path = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

        
         
        #Mapeo para proceso 1
        self.process1_script_versions = {
            "01_Copiar_Archivos_necesarios.py": self.python3_path,
            "02_Procesar_Conteo_de_Elementos.py": self.python3_path,
            "03_Crear_Topologías.py": self.python3_path,
            "04_Aplicar_Reglas_Topologicas.py": self.python3_path,
            "05_Exportar_Erro.Topológicos_a_SHP_1_2.py": self.python3_path,
            "06_Exportar_Erro.Topológicos_a_SHP_2_2.py": self.python3_path
            


        }

        # Mapeo para proceso 2
        self.process2_script_versions = {
            "07_Generar_DB_registro_Errores.py": self.python3_path,
            "08_Exportar_Err.Topologicos_segun_reglas_a_SHP.py": self.python3_path,
            "09_Diligenciar_Err._y_Excep._a_Excel.py": self.python3_path,
            "09_Diligenciar_Err._y_Excep._a_Excel_Rural.py": self.python3_path,
            "09_Diligenciar_Err._y_Excep._a_Excel_Urbano.py": self.python3_path,
            "10_Encabecado_Formato_Consitencia_Logica.py": self.python3_path,
            "11_Toolbox_Consistencia_Formato.py": self.python3_path,
            "12_Toolbox_Interseccion_Consistencia.py": self.python3_path,
            "13_Generar_shp_Consistencia_Formato.py": self.python3_path,
            "14_Generar_DB_registro_Errores_Consistencia.py": self.python3_path,
            "15_Diligenciar_Err._Consitencia_a_Excel.py": self.python3_path,

        }

        
        # Mapeo específico para el proceso 3   .python27_path
        self.process3_script_versions = {
            "16_Generar_DB_registro_Excepciones_Consistencia.py": self.python3_path,
            "17_Diligenciar_Excepciones_Consitencia_a_Excel.py": self.python3_path,
            "18_Toolbox_Omision_Comision.py": self.python3_path,
            "19_Conteo_Duplicados.py": self.python3_path,
            "20_Análisis_Omision_Comision.py": self.python3_path,
            "21_Reportes_Finales.py": self.python3_path,
            "21_Reporte_final_RURAL.py": self.python3_path,
            "21_Reporte_final_URBANO.py": self.python3_path,
            "22_Compilación_Datos.py": self.python3_path,


            
        }

        # Mapeo para proceso 4
        self.process4_script_versions = {
            
        }

        # Nuevo mapeo para proceso 5
        self.process5_script_versions = {
          

        }

        # Nuevo mapeo para modelo interno
        self.modelo_interno_script_versions = {
            "01_Copiar_Archivos_necesarios.py": self.python3_path,
            "02_convertir_gpkg_a_gdb.py": self.python3_path,
            "03_Procesar_Conteo_de_Elementos.py": self.python3_path,
            "04_Toolbox_validaciones.py": self.python3_path,
            "05_Habilitar_column_excepciones.py": self.python3_path,
            "06_Generar_DB_registro_Errores.py": self.python3_path,
            "07_Diligenciar_Err._y_Excep._a_Excel.py": self.python3_path,
            "08_Encabecado_Formato_Consitencia_Logica.py": self.python3_path,
            "09_Reportes_Finales.py": self.python3_path,
            "10_Compilación_Datos.py": self.python3_path
        }
    
    
    def get_python_path(self, script_name):
        """Obtiene la ruta de Python apropiada para un script específico"""
        # Buscar en todos los procesos
        for mapping in [self.process1_script_versions, 
                       self.process2_script_versions,
                       self.process3_script_versions,
                       self.process4_script_versions,
                       self.process5_script_versions,
                       self.modelo_interno_script_versions]:  # Agregamos el nuevo mapeo
            if script_name in mapping:
                return mapping[script_name]
        return None
    
    def validate_environment(self):
        """Valida que las versiones necesarias de Python estén disponibles"""
        errors = []
        
        # Verificar Python 3
        if not os.path.exists(self.python3_path):
            errors.append("No se encontró Python 3 (ArcGIS Pro)")
        
        
            
        return len(errors) == 0, errors
    
