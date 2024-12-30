import arcpy
import sys
from datetime import datetime

# Redirigir la salida estándar a un archivo
# Creamos un nombre de archivo con la fecha actual
filename = f"herramientas_toolbox_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# Guardar la salida original
original_stdout = sys.stdout

# Abrir el archivo para escribir
with open(filename, 'w') as f:
    # Redirigir stdout al archivo
    sys.stdout = f
    
    toolbox_path = r"C:\Users\osori\Desktop\MAS DE GEOVALIDATOOL\GeoValidaTool\Files\Templates\MODELO_LADM_1_0\Toolbox\XTFGDB_arcgispro.atbx"

    # Importar el toolbox
    arcpy.ImportToolbox(toolbox_path, "XTFGDB")

    # Obtener y listar las herramientas del toolbox
    tool_list = arcpy.ListTools("*")
    print("Herramientas disponibles en el toolbox:")
    for tool in tool_list:
        print(tool)

# Restaurar stdout a su valor original
sys.stdout = original_stdout

print(f"La lista de herramientas ha sido guardada en el archivo: {filename}")