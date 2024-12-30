import os
import sys
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                            QHBoxLayout, QPushButton, QLabel, QFileDialog,
                            QMessageBox, QLineEdit, QFrame, QCheckBox, QDialog,
                            QTextEdit, QScrollArea, QGridLayout)
sys.stdout.reconfigure(encoding='utf-8')
class BatchProcessor:
    def __init__(self, process_manager):
        self.process_manager = process_manager
        self.processes = [
            "01_Copiar_Archivos_necesarios.py",
            "02_Procesar_Conteo_de_Elementos.py",
            "03_Crear_Topologías.py",
            "04_Aplicar_Reglas_Topologicas.py",
            "05_Exportar_Erro.Topológicos_a_SHP_1_2.py",
            "06_Exportar_Erro.Topológicos_a_SHP_2_2.py",
            "07_Generar_DB_registro_Errores.py",
            "08_Exportar_Err.Topologicos_segun_reglas_a_SHP.py",
            "09_Diligenciar_Err._y_Excep._a_Excel.py",
            "10_Encabecado_Formato_Consitencia_Logica.py",
            "11_Toolbox_Consistencia_Formato.py",
            "12_Toolbox_Interseccion_Consistencia.py",
            "13_Generar_shp_Consistencia_Formato.py",
            "14_Generar_DB_registro_Errores_Consistencia.py",
            "15_Diligenciar_Err._Consitencia_a_Excel.py",
            "16_Generar_DB_registro_Excepciones_Consistencia.py",
            "17_Diligenciar_Excepciones_Consitencia_a_Excel.py",
            "18_Toolbox_Omision_Comision.py",
            "19_Conteo_Duplicados.py",
            "20_Análisis_Omision_Comision.py",
            "21_Reportes_Finales.py",
            "22_Compilación_Datos.py"
        ]
    
    def execute_all(self):
        """Ejecuta todos los procesos en orden"""
        try:
            # Verificar que el process_manager no esté ocupado
            if self.process_manager.is_running:
                return False
                    
            # Validar el entorno
            if not self.process_manager.validate_environment():
                return False

            # Verificar directorio MODELO_IGAC
            temp_dir = os.path.join(self.process_manager.parent.project_root, "Files", "Temporary_Files")
            modelo_igac_dir = os.path.join(temp_dir, "MODELO_IGAC")
            
            if os.path.exists(modelo_igac_dir) and any(os.scandir(modelo_igac_dir)):
                response = QMessageBox.warning(
                    self.process_manager.parent,
                    "Archivos existentes",
                    "Se eliminarán los archivos dentro de MODELO_IGAC para iniciar este proceso. ¿Desea Continuar?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                
                if response == QMessageBox.No:
                    return False
                
                try:
                    # Limpiar el contenido de MODELO_IGAC
                    for item in os.listdir(modelo_igac_dir):
                        item_path = os.path.join(modelo_igac_dir, item)
                        if os.path.isfile(item_path):
                            os.remove(item_path)
                        elif os.path.isdir(item_path):
                            import shutil
                            shutil.rmtree(item_path)
                    self.process_manager.parent.add_log("Contenido de MODELO_IGAC eliminado exitosamente")
                except Exception as e:
                    self.process_manager.parent.add_log(f"Error al limpiar MODELO_IGAC: {str(e)}")
                    return False
                    
            # Preparar la cola de scripts
            scripts_dir = os.path.join(self.process_manager.parent.project_root, "Scripts", "Modelo_IGAC")
            script_paths = []
            
            for i, script_name in enumerate(self.processes, start=1):
                script_path = os.path.join(scripts_dir, script_name)
                if os.path.exists(script_path):
                    script_paths.append((i, script_path))
                    self.process_manager.parent.add_log(f"Preparando script: {script_name}")
            
            if not script_paths:
                self.process_manager.parent.add_log("No se encontraron scripts para ejecutar")
                return False
            
            # Iniciar la ejecución
            self.process_manager.script_queue = script_paths
            self.process_manager.is_running = True
            self.process_manager.execute_next_script()
            return True
                
        except Exception as e:
            if hasattr(self.process_manager, 'parent'):
                self.process_manager.parent.add_log(f"Error al ejecutar todos los procesos: {str(e)}")
            return False