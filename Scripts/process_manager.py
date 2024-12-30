from PySide6.QtCore import QThread, Signal
import subprocess
import sys
import os

class ScriptRunner(QThread):
    """Clase para ejecutar scripts en un hilo separado"""
    progress = Signal(str)  # Para enviar mensajes de log
    status_update = Signal(int, str)  # Para actualizar el estado (índice, estado)
    script_finished = Signal(int, bool)  # Para indicar finalización (índice, éxito)

    def __init__(self, script_path, script_index):
        super().__init__()
        self.script_path = script_path
        self.script_index = script_index
        self.process = None
        self.should_stop = False

    def run(self):
        try:
            self.status_update.emit(self.script_index, "running")
            self.progress.emit(f"Iniciando script: {os.path.basename(self.script_path)}")

            self.process = subprocess.Popen(
                [sys.executable, self.script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            while True:
                if self.should_stop:
                    self.process.terminate()
                    self.progress.emit("Proceso detenido por el usuario")
                    self.status_update.emit(self.script_index, "error")
                    break

                output = self.process.stdout.readline()
                if output == '' and self.process.poll() is not None:
                    break
                if output:
                    self.progress.emit(output.strip())

            return_code = self.process.poll()
            success = return_code == 0

            if success:
                self.status_update.emit(self.script_index, "completed")
                self.progress.emit(f"Script completado: {os.path.basename(self.script_path)}")
            else:
                self.status_update.emit(self.script_index, "error")
                stderr = self.process.stderr.read()
                self.progress.emit(f"Error en script {os.path.basename(self.script_path)}: {stderr}")

            self.script_finished.emit(self.script_index, success)

        except Exception as e:
            self.progress.emit(f"Error al ejecutar script: {str(e)}")
            self.status_update.emit(self.script_index, "error")
            self.script_finished.emit(self.script_index, False)

    def stop(self):
        self.should_stop = True

class ProcessManager:
    """Clase para gestionar la ejecución de múltiples scripts"""
    def __init__(self, parent):
        self.parent = parent
        self.current_runners = []
        self.script_queue = []
        self.is_running = False

    def start_scripts(self, script_list, start_index=1):
        """Inicia la ejecución de una lista de scripts"""
        if self.is_running:
            return False

        self.script_queue = list(enumerate(script_list, start=start_index))
        self.is_running = True
        self.execute_next_script()
        return True

    def execute_next_script(self):
        """Ejecuta el siguiente script en la cola"""
        if not self.script_queue:
            self.is_running = False
            self.parent.add_log("Todos los procesos han finalizado")
            return

        script_index, script_path = self.script_queue.pop(0)
        runner = ScriptRunner(script_path, script_index)
        
        runner.progress.connect(self.parent.add_log)
        runner.status_update.connect(self.update_status)
        runner.script_finished.connect(self.handle_script_completion)
        
        self.current_runners.append(runner)
        runner.start()

    def update_status(self, index, status):
        """Actualiza el estado de un proceso en la interfaz"""
        if hasattr(self.parent, 'status_indicators'):
            self.parent.status_indicators[index].set_status(status)

    def handle_script_completion(self, index, success):
        """Maneja la finalización de un script"""
        if success:
            self.execute_next_script()
        else:
            self.stop_all()

    def stop_all(self):
        """Detiene todos los procesos en ejecución"""
        self.is_running = False
        self.script_queue.clear()
        
        for runner in self.current_runners:
            runner.stop()
        
        self.current_runners.clear()
        self.parent.add_log("Todos los procesos han sido detenidos")