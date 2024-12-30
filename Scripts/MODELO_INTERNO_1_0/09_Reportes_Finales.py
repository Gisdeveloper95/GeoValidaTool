import os
import subprocess
import sys
sys.stdout.reconfigure(encoding='utf-8')
try:
    # Definir la ruta del archivo de configuración
    script_dir = os.path.dirname(os.path.abspath(__file__))
    proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
    config_path = os.path.join(proyecto_dir, "Files", "Temporary_Files","array_config.txt")

    # Leer el archivo y filtrar solo los datasets activos
    DATASETS_TO_PROCESS = []
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()  # Limpiar espacios alrededor
            if line and not line.startswith('#'):
                dataset_name = line.strip('",[]').strip()
                if dataset_name:
                    DATASETS_TO_PROCESS.append(dataset_name)

    print("\nConfiguración de datasets cargada:")
    print("--------------------------------")
    print("Datasets que serán procesados:")
    for ds in DATASETS_TO_PROCESS:
        print(f"  - {ds}")
    print("--------------------------------\n")

    # Ejecutar los scripts según los datasets en DATASETS_TO_PROCESS
    if "RURAL_CTM12" in DATASETS_TO_PROCESS:
        print("Ejecutando 09_Reporte_final_RURAL_CTM12.py...")
        subprocess.run(["python", os.path.join(script_dir, "09_Reporte_final_RURAL_CTM12.py")])

    if "URBANO_CTM12" in DATASETS_TO_PROCESS:
        print("Ejecutando 09_Reporte_final_URBANO_CTM12.py...")
        subprocess.run(["python", os.path.join(script_dir, "09_Reporte_final_URBANO_CTM12.py")])

except Exception as e:
    print(f"Error al cargar configuración: {str(e)}")
    DATASETS_TO_PROCESS = ["URBANO_CTM12", "RURAL_CTM12"]
    print("\nUsando configuración por defecto:")
    print("--------------------------------")
    print("Datasets que serán procesados:")
    for ds in DATASETS_TO_PROCESS:
        print(f"  - {ds}")
    print("--------------------------------\n")

    # Ejecutar los scripts con la configuración por defecto
    print("Ejecutando 09_Reporte_final_RURAL.py...")
    subprocess.run(["python", os.path.join(script_dir, "09_Reporte_final_RURAL_CTM12.py")])
    print("Ejecutando 09_Reporte_final_URBANO.py...")
    subprocess.run(["python", os.path.join(script_dir, "09_Reporte_final_URBANO_CTM12.py")])


