import sys
import os

# Función para verificar PySide6
def check_pyside6():
    try:
        import PySide6
        return True
    except ImportError:
        return False

# Solo importamos PySide6 si ya verificamos que está instalado
if check_pyside6():
    from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                                QHBoxLayout, QPushButton, QLabel, QFileDialog,
                                QMessageBox, QLineEdit, QFrame, QCheckBox, QDialog,
                                QTextEdit, QScrollArea, QGridLayout)
    from PySide6.QtCore import QThread, Signal, Qt, QSize
    from PySide6.QtGui import QColor, QPainter, QFont, QIcon, QPixmap, QTextCursor
    from PySide6.QtCore import QTimer, QMetaObject, Qt
    from PySide6.QtCore import Slot as pyqtSlot
    
else:
    print("PySide6 no está instalado correctamente.")
    print("Por favor, ejecute la aplicación usando launcher.py")
    sys.exit(1)
 
import os
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtGui import QIcon
from utils.software_checker import SoftwareChecker
from utils.dependency_checker import DependencyChecker
from main_window import MainWindow

import sys
sys.stdout.reconfigure(encoding='utf-8')


def check_dependencies():
    """Verifica que todas las dependencias estén instaladas"""
    checker = DependencyChecker()
    return checker.check_and_install_dependencies()

def check_required_software():
    """Verifica el software requerido"""
    checker = SoftwareChecker()
    missing_software, found_paths = checker.check_software()
    
    if missing_software:
        warning_text = "ADVERTENCIA: Se requiere el siguiente software para ejecutar este programa:\n\n"
        for software in missing_software:
            warning_text += f"• {software}\n"
        warning_text += "\nPor favor, instale el software faltante antes de continuar."
        return False, warning_text
    return True, ""

def main():
    """Función principal"""
    try:
        # Verificar si ya hay una instancia de QApplication
        app = QApplication.instance()
        if app is None:
            app = QApplication(sys.argv)
            
        app.setStyle("Fusion")
        
        # Verificar dependencias
        if not check_dependencies():
            QMessageBox.critical(None, "Error", 
                               "No se pudieron instalar todas las dependencias necesarias.")
            return sys.exit(1)
            
        # Verificar software requerido
        software_ok, warning_text = check_required_software()
        if not software_ok:
            QMessageBox.warning(None, "Software Requerido Faltante", warning_text)
        
        # Crear y configurar la ventana principal
        window = MainWindow()
        
        # Configurar el ícono de la aplicación
        icon_path = os.path.join(os.path.dirname(__file__), "img", "icono.ico")
        if os.path.exists(icon_path):
            window.setWindowIcon(QIcon(icon_path))
        
        # Mostrar la ventana
        window.show()
        
        # Ejecutar la aplicación
        sys.exit(app.exec())
        
    except Exception as e:
        error_msg = f"Error iniciando la aplicación: {str(e)}"
        print(error_msg)
        QMessageBox.critical(None, "Error Fatal", error_msg)
        sys.exit(1)

if __name__ == "__main__":
    if not check_dependencies():
        sys.exit(1)
    main()