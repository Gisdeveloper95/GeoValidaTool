from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                            QHBoxLayout, QPushButton, QLabel, QFileDialog,
                            QMessageBox, QLineEdit, QFrame, QCheckBox, QDialog,
                            QTextEdit, QScrollArea, QGridLayout)
from PySide6.QtCore import QThread, Signal, Qt, QSize
from PySide6.QtGui import QColor, QPainter, QFont, QIcon, QPixmap, QTextCursor,QTextOption
from PySide6.QtCore import QTimer, QMetaObject, Qt
from PySide6.QtCore import Slot as pyqtSlot
import sys
import os
from rich.console import Console
from rich.text import Text
import io
import datetime
import subprocess
import json

import sys
sys.stdout.reconfigure(encoding='utf-8')

class RichTextEdit(QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        # Establecer codificación UTF-8
        self.document().setDefaultTextOption(QTextOption())
        self.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #ffffff;
                font-family: 'Consolas', monospace;
                font-size: 10pt;
                border: none;
                padding: 5px;
            }
        """)

    def append_styled(self, text, color=None):
        """Añade texto con color HTML con mejor manejo de codificación"""
        try:
            # Normalización de la codificación
            if isinstance(text, bytes):
                text = text.decode('utf-8', errors='replace')
            
            # Aplicar normalización unicode
            import unicodedata
            text = unicodedata.normalize('NFKD', text)
            
            # Reemplazar caracteres problemáticos comunes
            replacements = {
                'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú',
                'Ã±': 'ñ', 'Ã': 'í', '\x91': "'", '\x92': "'", '\x93': '"', '\x94': '"',
                'á': 'á', 'é': 'é', 'í': 'í', 'ó': 'ó', 'ú': 'ú',
                'ñ': 'ñ', 'Á': 'Á', 'É': 'É', 'Í': 'Í', 'Ó': 'Ó',
                'Ú': 'Ú', 'Ñ': 'Ñ', '�': ''
            }
            
            for old, new in replacements.items():
                text = text.replace(old, new)
                
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")

            # Determinar el color basado en el contenido
            if "Error durante" in text or "Error al" in text or "detenido" in text or "ERROR al procesar" in text or "El proceso no tiene acceso" in text and not any(keyword in text for keyword in ["INFO", "DEBUG", "===", "Dataset:", "Total"]):
                color = "#DB0000"  # Rojo para errores reales
            elif "Todos los procesos han finalizado" in text:
                color = "#182ef3"  # Azul
            elif "BUILDER" in text:
                color = "#BF40BF"  # Púrpura
            elif ".py" in text:
                color = "#FFFF00"  # Amarillo
            elif "Iniciando" in text or "Ejecutando" in text:
                color = "#8be9fd"  # Cyan
            elif "completado" in text or "exito" in text or "completado" in text or "finalizado" in text:
                color = "#00ff00"  # Verde
            else:
                color = color or "white"

            # Crear el texto HTML con codificación específica
            formatted_text = f'<span style="color: {color};">[{timestamp}] {text}</span>'
            
            # Agregar el texto con la codificación correcta
            self.append(formatted_text)
            
            # Mover el cursor al final
            cursor = self.textCursor()
            cursor.movePosition(QTextCursor.End)
            self.setTextCursor(cursor)
            
            # Asegurar que el texto nuevo sea visible
            self.ensureCursorVisible()
            self.verticalScrollBar().setValue(self.verticalScrollBar().maximum())
            
            # Procesar eventos pendientes
            QApplication.processEvents()
            
        except Exception as e:
            print(f"Error en append_styled: {str(e)}")
            # Intentar agregar el texto sin formato en caso de error
            self.append(f"[{timestamp}] {text}")