from PySide6.QtWidgets import QWidget, QVBoxLayout
from utils.rich_text_edit import RichTextEdit
from utils.status_indicator import StatusIndicator
import shutil  # Añadir esta línea

import sys
sys.stdout.reconfigure(encoding='utf-8')

class BaseModelTab(QWidget):
    def __init__(self, parent=None, model_name="", scripts_dir=""):
        super().__init__(parent)
        self.model_name = model_name
        self.scripts_dir = scripts_dir
        self.project_root = parent.project_root if parent else ""
        
        
        # Crear el layout principal
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.setLayout(self.main_layout)
        
        # Inicializar variables comunes
        self.console = None
        self.status_indicators = {}

    def add_log(self, message):
        """Añade mensaje al log"""
        if hasattr(self, 'console') and self.console:
            self.console.append_styled(message)

    def clear_console(self):
        """Limpia la consola"""
        if hasattr(self, 'console') and self.console:
            self.console.clear()

    def stop_processes(self):
        """Detiene todos los procesos"""
        if hasattr(self, 'process_manager'):
            self.process_manager.stop_all()