from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                            QHBoxLayout, QPushButton, QLabel, QFileDialog,
                            QMessageBox, QLineEdit, QFrame, QCheckBox, QDialog,
                            QTextEdit, QScrollArea, QGridLayout)
from PySide6.QtCore import QThread, Signal, Qt, QSize
from PySide6.QtGui import QColor, QPainter, QFont, QIcon, QPixmap, QTextCursor,QTextOption
from PySide6.QtCore import QTimer, QMetaObject, Qt
from PySide6.QtCore import Slot as pyqtSlot
import os
from rich.console import Console
from rich.text import Text
import io
import datetime
import subprocess
import json
from process_selector import ProcessSelectorDialog
from batch_processor import BatchProcessor
import shutil
from dependency_checker import DependencyChecker
from .python_version_manager import PythonVersionManager
import sys
sys.stdout.reconfigure(encoding='utf-8')

class ScriptRunner(QThread):
    progress = Signal(str)
    status_update = Signal(int, str)
    script_finished = Signal(int, bool)

    def __init__(self, script_path, script_index, script_name, python_version_manager):
        super().__init__()
        self.script_path = script_path
        self.script_index = script_index
        self.script_name = script_name
        self.python_manager = python_version_manager
        self.process = None
        self.should_stop = False

    def is_log_message(self, text):
        """Determina si un mensaje es un log normal o un error real"""
        # Lista de indicadores de mensaje normal
        normal_indicators = [
            "INFO", "DEBUG", "===",
            "Dataset:", "Total", "Features",
            "Iniciando", "Progreso:", "Completado",
            "PROCESO", "RESUMEN", "ESTADÍSTICAS"
        ]
        return any(indicator in text for indicator in normal_indicators)

    def clean_message(self, message):
        """Limpia el mensaje de prefijos innecesarios"""
        # Eliminar el prefijo de timestamp si existe
        if " - " in message:
            parts = message.split(" - ", 1)
            if len(parts) > 1 and any(level in parts[1] for level in ["INFO", "DEBUG"]):
                message = parts[1]

        # Si el mensaje comienza con "Error: " y parece ser un log normal, eliminar el prefijo
        if message.startswith("Error: ") and self.is_log_message(message):
            message = message[7:]

        return message

    def run(self):
        try:
            self.status_update.emit(self.script_index, "running")
            self.progress.emit(f"Iniciando script: {self.script_name}")

            python_path = self.python_manager.get_python_path(self.script_name)
            if not python_path:
                raise ValueError(f"No se encontró versión de Python para el script: {self.script_name}")

            self.progress.emit(f"Usando Python: {python_path}")
            
            # Buffer grande y modo línea por línea
            self.process = subprocess.Popen(
                [python_path, self.script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,  # Modo línea por línea
                universal_newlines=True,
                creationflags=subprocess.CREATE_NO_WINDOW
            )

            def read_output(pipe, is_error=False):
                buffer = []
                buffer_size = 0
                max_buffer_size = 50  # Número de líneas antes de emitir

                def flush_buffer():
                    nonlocal buffer, buffer_size
                    if buffer:
                        message = "\n".join(buffer)
                        self.progress.emit(message)
                        buffer = []
                        buffer_size = 0
                    QApplication.processEvents()

                try:
                    while True:
                        line = pipe.readline()
                        if not line and self.process.poll() is not None:
                            flush_buffer()  # Asegurar que se emita el último buffer
                            break
                        
                        if line:
                            line = line.strip()
                            if line:
                                if is_error and not self.is_log_message(line):
                                    self.progress.emit(f"Error: {line}")
                                else:
                                    buffer.append(line)
                                    buffer_size += 1
                                    
                                    if buffer_size >= max_buffer_size:
                                        flush_buffer()

                except Exception as e:
                    self.progress.emit(f"Error en lectura de salida: {str(e)}")
                    flush_buffer()

            import threading
            stdout_thread = threading.Thread(target=read_output, args=(self.process.stdout,))
            stderr_thread = threading.Thread(target=read_output, args=(self.process.stderr, True))
            
            stdout_thread.daemon = True
            stderr_thread.daemon = True
            
            stdout_thread.start()
            stderr_thread.start()
            
            stdout_thread.join()
            stderr_thread.join()
            
            return_code = self.process.wait()
            success = return_code == 0

            if success:
                self.status_update.emit(self.script_index, "completed")
                self.progress.emit(f"Script completado exitosamente: {self.script_name}")
            else:
                self.status_update.emit(self.script_index, "error")
                if return_code != 0:
                    self.progress.emit(f"Script terminado con código de error: {return_code}")

            self.process = None
            self.script_finished.emit(self.script_index, success)

        except Exception as e:
            self.progress.emit(f"Error al ejecutar script {self.script_name}: {str(e)}")
            self.status_update.emit(self.script_index, "error")
            self.script_finished.emit(self.script_index, False)
            
        finally:
            if self.process:
                try:
                    self.process.kill()
                except:
                    pass

    def stop(self):
        self.should_stop = True


class ProcessManager:
    """Clase para gestionar la ejecución de múltiples scripts"""
    def __init__(self, parent):
        self.parent = parent
        self.current_runners = []
        self.script_queue = []
        self.is_running = False
        self.python_manager = PythonVersionManager()
        
        # Inicialización de estructuras de datos
        self.process_status = {}  # Diccionario para mantener el estado de cada proceso
        self.last_process_number = 0

    def extract_process_number(self, script_name):
        """Extrae el número de proceso del nombre del script"""
        try:
            return int(script_name.split('_')[0])
        except (ValueError, IndexError):
            return 0

    def update_process_status(self, script_name, status):
        """Actualiza el estado de un proceso específico"""
        try:
            process_number = self.extract_process_number(script_name)
            if process_number <= 0:
                return

            # Actualizar estado en el diccionario
            self.process_status[process_number] = status

            # Actualizar indicador visual
            if hasattr(self.parent, 'status_indicators'):
                if process_number in self.parent.status_indicators:
                    self.parent.status_indicators[process_number].set_status(status)

        except Exception as e:
            self.parent.add_log(f"Error al actualizar estado del proceso {script_name}: {str(e)}")

    def is_process_completed(self, process_number):
        """Verifica si un proceso está completado"""
        return self.process_status.get(process_number) == "completed"

    def get_active_processes(self):
        """Obtiene la lista de procesos activos"""
        return [num for num, status in self.process_status.items() if status == "running"]

    def get_completed_processes(self):
        """Obtiene la lista de procesos completados"""
        return [num for num, status in self.process_status.items() if status == "completed"]

    def run_selected_processes(self, selected_processes):
        """Ejecuta los procesos seleccionados"""
        try:
            if self.is_running:
                self.parent.add_log("Hay procesos en ejecución. Espere a que terminen.")
                return False
                        
            if not self.validate_environment():
                return False

            # Determinar el tipo de modelo
            model_type = str(type(self.parent)).lower()
            
            # Mapeo de modelos a directorios
            model_mapping = {
                'interno_tab': ("MODELO_INTERNO_1_0", "MODELO_INTERNO_1_0"),
                'ladm_10_tab': ("MODELO_LADM_1_0", "MODELO_LADM_1_0"),
                'ladm_12_tab': ("MODELO_LADM_1_2", "MODELO_LADM_1_2"),
                'cica_tab': ("Modelo_IGAC", "MODELO_IGAC")
            }
            
            # Obtener la configuración del modelo actual
            model_key = next((key for key in model_mapping.keys() if key in model_type), 'cica_tab')
            scripts_subdir, temp_subdir = model_mapping[model_key]
            
            scripts_dir = os.path.join(self.parent.project_root, "Scripts", scripts_subdir)
            
            # Solo verificar y limpiar si el primer script está en la selección
            if "01_Copiar_Archivos_necesarios.py" in selected_processes:
                temp_dir = os.path.join(self.parent.project_root, "Files", "Temporary_Files")
                model_temp_dir = os.path.join(temp_dir, temp_subdir)
                
                if os.path.exists(model_temp_dir) and any(os.scandir(model_temp_dir)):
                    response = QMessageBox.warning(
                        self.parent,
                        "Archivos existentes",
                        f"Se eliminarán los archivos dentro de {temp_subdir} para iniciar este proceso. ¿Desea Continuar?",
                        QMessageBox.Yes | QMessageBox.No,
                        QMessageBox.No
                    )
                    
                    if response == QMessageBox.No:
                        return False
                    
                    try:
                        for item in os.listdir(model_temp_dir):
                            item_path = os.path.join(model_temp_dir, item)
                            if os.path.isfile(item_path):
                                os.remove(item_path)
                            elif os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                        self.parent.add_log(f"Contenido de {temp_subdir} eliminado exitosamente")
                    except Exception as e:
                        self.parent.add_log(f"Error al limpiar {temp_subdir}: {str(e)}")
                        return False
            
            # Preparar la cola de scripts
            script_paths = []
            for i, script_name in enumerate(selected_processes, start=1):
                script_path = os.path.join(scripts_dir, script_name)
                if os.path.exists(script_path):
                    script_paths.append((i, script_path))
                    self.parent.add_log(f"Preparando script: {script_name}")
            
            if not script_paths:
                self.parent.add_log("No se encontraron scripts válidos para ejecutar")
                return False
            
            # Iniciar la ejecución
            self.script_queue = script_paths
            self.is_running = True
            self.execute_next_script()
            return True
                
        except Exception as e:
            self.parent.add_log(f"Error al ejecutar los procesos seleccionados: {str(e)}")
            return False
    
    def stop_all(self):
        """Detiene todos los procesos"""
        try:
            self.is_running = False
            self.script_queue.clear()
            
            # Marcar todos los procesos activos como pendientes
            active_processes = self.get_active_processes()
            for process_number in active_processes:
                self.update_process_status(f"{process_number:02d}_dummy.py", "pending")
            
            # Limpiar estados
            self.process_status.clear()
            
            # Detener runners
            for runner in self.current_runners:
                try:
                    runner.stop()
                except Exception as e:
                    self.parent.add_log(f"Error al detener runner: {str(e)}")
            
            self.current_runners.clear()
            self.parent.add_log("Todos los procesos han sido detenidos")
            
        except Exception as e:
            self.parent.add_log(f"Error al detener procesos: {str(e)}")

    def reset_status_indicators(self):
        """Reinicia todos los indicadores de estado"""
        try:
            # Limpiar estados
            self.process_status.clear()
            self.current_runners.clear()
            
            # Resetear indicadores visuales
            if hasattr(self.parent, 'status_indicators'):
                for process_number in self.parent.status_indicators:
                    self.parent.status_indicators[process_number].set_status("pending")
                    
            self.is_running = False
            
        except Exception as e:
            self.parent.add_log(f"Error al reiniciar indicadores: {str(e)}")
    
    def validate_environment(self):
        """Valida el entorno antes de ejecutar los scripts"""
        try:
            # Determinar el tipo de modelo analizando la clase del parent
            model_type = str(type(self.parent)).lower()
            
            # Validar Python y software requerido
            if not self.python_manager.validate_environment()[0]:
                return False
                
            # Mapeo de modelos a sus directorios
            model_dirs = {
                'interno_tab': "MODELO_INTERNO_1_0",
                'ladm_10_tab': "MODELO_LADM_1_0",
                'ladm_12_tab': "MODELO_LADM_1_2",
                'cica_tab': "MODELO_IGAC"
            }
            
            # Determinar el directorio correcto basado en el tipo de modelo
            model_key = next((key for key in model_dirs.keys() if key in model_type), 'cica_tab')
            model_dir = model_dirs[model_key]
            
            scripts_dir = os.path.join(self.parent.project_root, "Scripts", model_dir)
            
            if not os.path.exists(scripts_dir):
                self.parent.add_log(f"Error: No se encuentra el directorio de scripts: {scripts_dir}")
                return False
                
            # Mapeo de modelos a sus archivos de configuración
            config_files = {
                'interno_tab': "rutas_archivos_interno.json",
                'ladm_10_tab': "rutas_archivos_ladm_1_0.json",
                'ladm_12_tab': "rutas_archivos_ladm_1_2.json",
                'cica_tab': "rutas_archivos.json"
            }
            
            config_file = config_files[model_key]
            config_path = os.path.join(
                self.parent.project_root,
                "Files",
                "Temporary_Files",
                "Ruta_Insumos",
                config_file
            )
            
            if not os.path.exists(config_path):
                self.parent.add_log(f"Error: No se ha configurado los insumos necesarios")
                return False
                
            return True
            
        except Exception as e:
            self.parent.add_log(f"Error en la validación del entorno: {str(e)}")
            return False

    def show_completion_message(self, show_folder=False):
        """Muestra el mensaje de finalización y opcionalmente abre la carpeta de resultados"""
        try:
            results_path = os.path.join(self.parent.project_root, "Reportes")
            
            if os.path.exists(results_path):
                response = QMessageBox.information(
                    self.parent,
                    "Proceso Completado",
                    "¡Todos los procesos han finalizado exitosamente!\n\n"
                    "¿Desea abrir la carpeta de resultados?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.Yes
                )
                
                if response == QMessageBox.Yes:
                    try:
                        os.startfile(os.path.abspath(results_path))
                    except Exception as e:
                        QMessageBox.warning(
                            self.parent,
                            "Error",
                            f"No se pudo abrir la carpeta de resultados: {str(e)}"
                        )
            else:
                QMessageBox.information(
                    self.parent,
                    "Proceso Completado",
                    "¡Los procesos han finalizado exitosamente!"
                )
                
        except Exception as e:
            self.parent.add_log(f"Error en show_completion_message: {str(e)}")
        
    def reset_progress(self):
        """Reinicia el contador de progreso"""
        self.completed_processes = 0

    def start_scripts(self, script_list, start_index=1):
        """Inicia la ejecución de una lista de scripts"""
        if self.is_running:
            return False
            
        if not self.validate_environment():
            return False

        self.script_queue = list(enumerate(script_list, start=start_index))
        self.total_processes = len(script_list)
        self.completed_processes = 0
        self.current_process_number = start_index
        self.is_running = True
        self.execute_next_script()
        return True
    
    def run_data_structuring(self):
        """Ejecuta el proceso de estructuración de datos"""
        try:
            if self.is_running:
                return False
                    
            if not self.validate_environment():
                return False

            # Verificar si estamos en el modelo interno
            is_internal_model = isinstance(self.parent, InternoModelTab)
            
            # Seleccionar el directorio correcto
            temp_dir = os.path.join(self.parent.project_root, "Files", "Temporary_Files")
            model_dir = "MODELO_INTERNO_1_0" if is_internal_model else "MODELO_IGAC"
            working_dir = os.path.join(temp_dir, model_dir)
            
            if os.path.exists(working_dir) and any(os.scandir(working_dir)):
                response = QMessageBox.warning(
                    self.parent,
                    "Archivos existentes",
                    f"Se eliminarán los archivos dentro de {model_dir} para iniciar este proceso. ¿Desea Continuar?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                
                if response == QMessageBox.No:
                    return False
                    
                try:
                    for item in os.listdir(working_dir):
                        item_path = os.path.join(working_dir, item)
                        if os.path.isfile(item_path):
                            os.remove(item_path)
                        elif os.path.isdir(item_path):
                            import shutil
                            shutil.rmtree(item_path)
                    self.parent.add_log(f"Contenido de {model_dir} eliminado exitosamente")
                except Exception as e:
                    self.parent.add_log(f"Error al limpiar {model_dir}: {str(e)}")
                    return False

            # Definir scripts según el modelo
            if is_internal_model:
                scripts_dir = os.path.join(self.parent.project_root, "Scripts", "MODELO_INTERNO_1_0")
                process_scripts = [
                    "01_Copiar_Archivos_necesarios.py",
                    "02_convertir_gpkg_a_gdb.py",
                    "03_Procesar_Conteo_de_Elementos.py",
                    "04_Toolbox_validaciones.py",
                    "05_Habilitar_column_excepciones.py"
                ]
            else:

                # Definir scripts del proceso 1
                scripts_dir = os.path.join(self.parent.project_root, "Scripts", "Modelo_IGAC")
                process1_scripts = [
                    "01_Copiar_Archivos_necesarios.py",
                    "02_Procesar_Conteo_de_Elementos.py",
                    "03_Crear_Topologías.py",
                    "04_Aplicar_Reglas_Topologicas.py",
                    "05_Exportar_Erro.Topológicos_a_SHP_1_2.py",
                    "06_Exportar_Erro.Topológicos_a_SHP_2_2.py"
                ]
            
            # Crear la cola de scripts
            self.script_queue = [
                (i, os.path.join(scripts_dir, script_name)) 
                for i, script_name in enumerate(process_scripts, start=1)
            ]
            
            self.is_running = True
            self.execute_next_script()
            return True

        except Exception as e:
            self.parent.add_log(f"Error al iniciar el proceso: {str(e)}")
            return False

    def run_process_2(self):
        """Ejecuta específicamente los scripts del proceso 3"""
        try:
            if self.is_running:
                return False
                
            if not self.validate_environment():
                return False

            # Definir los scripts del proceso 3 en orden
            scripts_dir = os.path.join(self.parent.project_root, "Scripts", "Modelo_IGAC")
            process3_scripts = [
                "07_Generar_DB_registro_Errores.py",
                "08_Exportar_Err.Topologicos_segun_reglas_a_SHP.py",
                "09_Diligenciar_Err._y_Excep._a_Excel.py",
                "10_Encabecado_Formato_Consitencia_Logica.py",
                "11_Toolbox_Consistencia_Formato.py",
                "12_Toolbox_Interseccion_Consistencia.py",
                "13_Generar_shp_Consistencia_Formato.py",
                "14_Generar_DB_registro_Errores_Consistencia.py",
                "15_Diligenciar_Err._Consitencia_a_Excel.py"


            ]
            
            # Crear la cola de scripts con rutas completas
            self.script_queue = [
                (i, os.path.join(scripts_dir, script_name)) 
                for i, script_name in enumerate(process3_scripts, start=6)
            ]
            
            self.is_running = True
            self.execute_next_script()
            return True

        except Exception as e:
            self.parent.add_log(f"Error al iniciar el proceso 3: {str(e)}")
            return False

    def run_process_4(self):
        """Ejecuta el proceso 4"""
        try:
            if self.is_running:
                return False

            # Mostrar alerta de validación
            alert_result = QMessageBox.warning(
                self.parent,
                "Validación de Excepciones Consistencia de Formato",
                "Esta herramienta ejecutará un análisis automatizado para identificar "
                "posibles excepciones Consistencia de Formato en sus datos SIG.\n\n"
                "Consideraciones importantes:\n"
                "• El algoritmo puede generar falsos positivos en algunos casos\n"
                "• Se recomienda verificar manualmente los resultados\n"
                "• Proceda con el proceso siguiente solo después de validar los resultados\n\n"
                "¿Desea continuar?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if alert_result == QMessageBox.No:
                return False

            # Definir y ejecutar el script
            scripts_dir = os.path.join(self.parent.project_root, "Scripts")
            script_name = "21_Análisis_de_Excepciones_Consistencia_Formato.py"
            script_path = os.path.join(scripts_dir, script_name)
            
            self.script_queue = [(4, script_path)]
            self.is_running = True
            self.execute_next_script()
            return True

        except Exception as e:
            self.parent.add_log(f"Error al iniciar el proceso 4: {str(e)}")
            return False

    def run_process_3(self):
        """Ejecuta específicamente los scripts del proceso 3"""
        try:
            if self.is_running:
                return False
                
            if not self.validate_environment():
                return False

            # Definir los scripts del proceso 3 en orden
            scripts_dir = os.path.join(self.parent.project_root, "Scripts", "Modelo_IGAC")
            process3_scripts = [
                "16_Generar_DB_registro_Excepciones_Consistencia.py",
                "17_Diligenciar_Excepciones_Consitencia_a_Excel.py",
                "18_Toolbox_Omision_Comision.py",
                "19_Conteo_Duplicados.py",
                "20_Análisis_Omision_Comision.py",
                "21_Reportes_Finales.py",
                "22_Compilación_Datos.py",


            ]
            
            # Crear la cola de scripts con rutas completas
            self.script_queue = [
                (i, os.path.join(scripts_dir, script_name)) 
                for i, script_name in enumerate(process3_scripts, start=6)
            ]
            
            self.is_running = True
            self.execute_next_script()
            return True

        except Exception as e:
            self.parent.add_log(f"Error al iniciar el proceso 3: {str(e)}")
            return False

    def run_process_5(self):
        """Ejecuta específicamente los scripts del proceso 5"""
        try:
            if self.is_running:
                return False
                
            if not self.validate_environment():
                return False

            # Definir los scripts del proceso 5 en orden
            scripts_dir = os.path.join(self.parent.project_root, "Scripts")
            process5_scripts = [

            ]
            
            # Verificar que estamos iniciando los últimos scripts
            start_index = 22  # Índice inicial para proceso 5
            
            # Crear la cola de scripts con rutas completas
            self.script_queue = [
                (i, os.path.join(scripts_dir, script_name)) 
                for i, script_name in enumerate(process5_scripts, start=start_index)
            ]
            
            self.is_running = True
            self.execute_next_script()
            return True

        except Exception as e:
            self.parent.add_log(f"Error al iniciar el proceso 5: {str(e)}")
            return False
        
    def execute_next_script(self):
        """Ejecuta el siguiente script en la cola"""
        if not self.script_queue:
            if self.is_running:
                self.is_running = False
                self.parent.add_log("Todos los procesos han finalizado")
                # Verificar si el proceso 29 está entre los completados
                completed_processes = self.get_completed_processes()
                self.show_completion_message(show_folder=29 in completed_processes)
            return

        try:
            process_number, script_path = self.script_queue.pop(0)
            script_name = os.path.basename(script_path)
            
            self.parent.add_log(f"="*50)
            self.parent.add_log(f"Ejecutando proceso {script_name}")
            self.parent.add_log(f"="*50)
            
            runner = ScriptRunner(script_path, process_number, script_name, self.python_manager)
            runner.progress.connect(self.parent.add_log)
            runner.status_update.connect(lambda idx, status: self.update_process_status(script_name, status))
            runner.script_finished.connect(lambda idx, success: self.handle_script_completion(script_name, success))
            
            self.current_runners.append(runner)
            self.update_process_status(script_name, "running")
            runner.start()

        except Exception as e:
            self.parent.add_log(f"Error al ejecutar siguiente script: {str(e)}")
            self.stop_all()

    def update_status(self, index, status):
        """Actualiza el estado de un proceso en la interfaz"""
        if hasattr(self.parent, 'status_indicators'):
            self.parent.status_indicators[index].set_status(status)

    def handle_script_completion(self, script_name, success):
        """Maneja la finalización de un script"""
        try:
            # Importar aquí para evitar importación circular
            from model_tabs.interno_tab import InternoModelTab
            
            process_number = self.extract_process_number(script_name)
            is_internal_model = isinstance(self.parent, InternoModelTab)
            
            if success:
                self.update_process_status(script_name, "completed")
                
                # Verificar si es el último proceso según el modelo
                is_last_process = (process_number == 10 if is_internal_model else process_number == 22)
                is_last_in_queue = not self.script_queue
                
                if is_last_in_queue or is_last_process:
                    self.is_running = False
                    self.parent.add_log("Todos los procesos han finalizado")
                    
                    # Mostrar diálogo y abrir carpeta si es el último proceso
                    self.show_completion_message(show_folder=is_last_process)
                else:
                    self.execute_next_script()
            else:
                self.update_process_status(script_name, "error")
                self.stop_all()

        except Exception as e:
            self.parent.add_log(f"Error al manejar finalización del script: {str(e)}")
            self.stop_all()

    def reset_status(self):
        """Reinicia los contadores de estado"""
        self.completed_processes = 0
        self.current_process_number = 0
        self.is_running = False
