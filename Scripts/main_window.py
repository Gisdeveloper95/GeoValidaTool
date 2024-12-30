from PySide6.QtWidgets import QMainWindow, QTabWidget
from PySide6.QtGui import QIcon
import os
from model_tabs.cica_tab import CICAModelTab
from model_tabs.interno_tab import InternoModelTab
from model_tabs.ladm_10_tab import LADM10ModelTab
from model_tabs.ladm_12_tab import LADM12ModelTab


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("GeoValidaTool     ~ Subdireccion de Proyectos para Procesos de Actualizacion- IGAC      ~ by  andres.osorio@igac.gov.co")
        self.setMinimumSize(1280, 720)
        
        # Configuración de pestañas habilitadas (True = habilitada, False = deshabilitada)
        self.enabled_tabs = {
            'cica': True,      # CICA/CONSERVACION
            'interno': True,   # Modelo Interno 1.0
            'ladm_10': False,  # LADMCOL 1.0 - Deshabilitada inicialmente
            'ladm_12': True    # LADMCOL 1.2
        }
        
        # Configurar el directorio raíz del proyecto
        self.setup_project_root()
        
        # Crear y configurar el widget de pestañas
        self.tab_widget = QTabWidget()
        self.setup_tab_styles()
        self.setCentralWidget(self.tab_widget)
        
        # Configurar las pestañas
        self.setup_tabs()
        
    def setup_tab_styles(self):
        """Configura los estilos de las pestañas incluyendo estado deshabilitado"""
        self.tab_widget.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #D2B48C;
                background: #F5F5DC;
            }
            QTabBar::tab {
                background: #DEB887;
                border: 1px solid #D2B48C;
                padding: 6px 12px;
                margin-right: 2px;
                color: #000000;
            }
            QTabBar::tab:selected {
                background: #F5F5DC;
                border-bottom: none;
                margin-bottom: -1px;
            }
            QTabBar::tab:disabled {
                background: #D3D3D3;
                color: #808080;
                border: 1px solid #A9A9A9;
            }
            QTabBar::tab:disabled:hover {
                background: #D3D3D3;
            }
        """)
        
    def setup_styles(self):
        """Configura los estilos globales de la aplicación"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #F5F5DC;
            }
            QTabWidget::pane {
                border: 1px solid #D2B48C;
                background-color: #F5F5DC;
                margin: 10px;
            }
            QTabBar::tab {
                background-color: #8B5E3C;
                color: white;
                padding: 8px 15px;
                margin: 2px;
                border: 1px solid #6B4C2C;
                border-radius: 4px 4px 0 0;
            }
            QTabBar::tab:selected {
                background-color: #6B4C2C;
                border-bottom: none;
            }
            QTabBar::tab:hover {
                background-color: #7B523C;
            }
            QTabBar::tab:disabled {
                background-color: #D3D3D3;
                color: #808080;
                border: 1px solid #A9A9A9;
            }
            QTabBar::tab:disabled:hover {
                background-color: #D3D3D3;
            }
        """)
        
        # Aplicar margen al widget de pestañas
        self.tab_widget.setContentsMargins(10, 10, 10, 10)
        
    def setup_project_root(self):
        """Configura el directorio raíz del proyecto"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while os.path.basename(current_dir) != "GeoValidaTool" and current_dir != os.path.dirname(current_dir):
            current_dir = os.path.dirname(current_dir)
        self.project_root = current_dir
        
    def setup_tabs(self):
        """Configura las pestañas de la aplicación"""
        scripts_dirs = {
            'cica': os.path.join(self.project_root, "Scripts", "Modelo_IGAC"),
            'interno': os.path.join(self.project_root, "Scripts", "Modelo_Interno"),
            'ladm_1_0': os.path.join(self.project_root, "Scripts", "Modelo_LADM_1_0"),
            'ladm_1_2': os.path.join(self.project_root, "Scripts", "Modelo_LADM_1_2")
        }
        
        # Configuración de pestañas con su estado habilitado/deshabilitado
        tabs_config = [
            ('cica', "Modelo CICA/CONSERVACION", CICAModelTab, scripts_dirs['cica']),
            ('interno', "Modelo Interno 1.0", InternoModelTab, scripts_dirs['interno']),
            ('ladm_10', "Modelo LADMCOL 1.0", LADM10ModelTab, scripts_dirs['ladm_1_0']),
            ('ladm_12', "Modelo LADMCOL 1.2", LADM12ModelTab, scripts_dirs['ladm_1_2'])
        ]
        
        # Crear y agregar cada pestaña
        for tab_id, title, tab_class, scripts_dir in tabs_config:
            tab = tab_class(self, title, scripts_dir)
            tab_index = self.tab_widget.addTab(tab, title)
            # Establecer el estado habilitado/deshabilitado según la configuración
            self.tab_widget.setTabEnabled(tab_index, self.enabled_tabs.get(tab_id, True))

    def set_tab_enabled(self, tab_id: str, enabled: bool):
        """
        Habilita o deshabilita una pestaña específica
        
        Args:
            tab_id: Identificador de la pestaña ('cica', 'interno', 'ladm_10', 'ladm_12')
            enabled: True para habilitar, False para deshabilitar
        """
        tab_mapping = {
            'cica': 0,
            'interno': 1,
            'ladm_10': 2,
            'ladm_12': 3
        }
        
        if tab_id in tab_mapping:
            self.enabled_tabs[tab_id] = enabled
            self.tab_widget.setTabEnabled(tab_mapping[tab_id], enabled)