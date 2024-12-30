import sys
import os
import subprocess
from pathlib import Path

def check_dependencies():
    """Verifica e instala las dependencias necesarias"""
    try:
        # Intentar importar el DependencyChecker
        from dependency_checker import DependencyChecker
        checker = DependencyChecker()
        return checker.check_and_install_dependencies()
    except ImportError:
        # Si no podemos importar el checker, probablemente faltan dependencias básicas
        python_path = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
        
        # Instalar PySide6 directamente
        print("Instalando PySide6...")
        result = subprocess.run(
            [python_path, "-m", "pip", "install", "PySide6"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print("Error instalando PySide6:", result.stderr)
            input("Presione Enter para salir...")
            return False
            
        print("PySide6 instalado correctamente. Reiniciando aplicación...")
        # Reiniciar el script
        os.execl(python_path, python_path, __file__)
    
    return True

def main():
    """Función principal del launcher"""
    if not check_dependencies():
        sys.exit(1)
        
    # Si llegamos aquí, todas las dependencias están instaladas
    # Ahora podemos importar y ejecutar la aplicación principal
    try:
        import Main
        Main.main()
    except Exception as e:
        print(f"Error iniciando la aplicación: {str(e)}")
        input("Presione Enter para salir...")
        sys.exit(1)

if __name__ == "__main__":
    main()