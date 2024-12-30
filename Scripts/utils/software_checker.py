import os
import sys
sys.stdout.reconfigure(encoding='utf-8')
class SoftwareChecker:
    """Clase para verificar la instalación de software requerido"""
    def __init__(self):
        self.required_software = {
            'ArcGIS Pro': [
                r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe'
            ],
            
        }

    def check_software(self):
        """Verifica la existencia de los programas requeridos"""
        missing_software = []
        found_paths = {}

        for software, paths in self.required_software.items():
            software_found = False
            for path in paths:
                if os.path.exists(path):
                    software_found = True
                    found_paths[software] = path
                    break
            if not software_found:
                missing_software.append(software)

        return missing_software, found_paths
