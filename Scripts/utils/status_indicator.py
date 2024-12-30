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

class StatusIndicator(QWidget):
    """Indicador tipo LED para estados"""
    def __init__(self, size=12, parent=None):
        super().__init__(parent)
        self.size = size
        self.color = QColor("#2F3337")  # Gris oscuro 
        self.setFixedSize(QSize(size, size))
    
    def set_status(self, status):
        colors = {
            "pending": "#2F3337",    # Gris oscuro
            "running": "#FF8C00",    # Naranja LED intenso
            "completed": "#32CD32",  # Verde LED brillante
            "error": "#e74c3c"       # Rojo 
        }
        self.color = QColor(colors.get(status, "#2F3337"))
        self.update()
    
    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        # Dibujar círculo exterior (borde)
        painter.setBrush(Qt.NoBrush)
        painter.setPen(QColor("#34495e"))  # Color del borde
        painter.drawEllipse(1, 1, self.size-2, self.size-2)
        
        # Dibujar círculo interior (color de estado)
        painter.setBrush(self.color)
        painter.setPen(Qt.NoPen)
        painter.drawEllipse(2, 2, self.size-4, self.size-4)
