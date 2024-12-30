import os
import arcpy
import numpy as np
import pandas as pd
import logging
import glob
import shutil
from datetime import datetime
from pathlib import Path
import sys
sys.stdout.reconfigure(encoding='utf-8')
# Silenciar advertencias y mensajes innecesarios
import warnings
warnings.filterwarnings('ignore')
import logging
logging.getLogger('pandas').setLevel(logging.ERROR)
logging.getLogger('arcpy').setLevel(logging.ERROR)

class OmisionComisionProcessor:
    def __init__(self):
        """Inicializa el procesador con configuración básica"""
        self.start = datetime.now()
        
        # Cambiar esto para obtener la ruta correcta
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        # Subir dos niveles para llegar a GeoValidaTool
        self.project_root = os.path.abspath(os.path.join(self.script_dir, '..', '..'))
        
        print(f"Script directory: {self.script_dir}")
        print(f"Project root: {self.project_root}")
        print(f"Expected GDB path: {os.path.join(self.project_root, 'Files', 'Temporary_Files', 'MODELO_IGAC')}")
        
        self.setup_paths()
        self.setup_logging()
        
        # Inicializar workspace
        arcpy.env.workspace = self.paths['gdb']
        arcpy.env.overwriteOutput = True

    def setup_paths(self):
        """Configura todas las rutas necesarias para el proceso"""
        # Verificar la existencia de directorios clave
        required_paths = [
            os.path.join(self.project_root, "Files", "Temporary_Files", "MODELO_IGAC"),
            os.path.join(self.project_root, "Files", "Temporary_Files", "array_config.txt"),
            os.path.join(self.project_root, "Scripts")
        ]
        
        print("\nVerificando rutas requeridas:")
        for path in required_paths:
            exists = os.path.exists(path)
            print(f"Ruta: {path}")
            print(f"¿Existe?: {exists}")
            
        if not all(os.path.exists(path) for path in required_paths):
            raise Exception(
                "No se encontró la estructura de directorios requerida.\n"
                "Verifique que está ejecutando el script desde la ubicación correcta."
            )

        self.paths = {
            'root': self.project_root,
            'temp_base': os.path.join(self.project_root, "Files", "Temporary_Files", "MODELO_IGAC"),
            'output': os.path.join(self.project_root, "Files", "Temporary_Files", "MODELO_IGAC", "Omision_comision_temp"),
            'apex_terreno': os.path.join(self.project_root, "Files", "Temporary_Files", "MODELO_IGAC", "INSUMOS", "Apex_Terreno"),
            'apex_unidad': os.path.join(self.project_root, "Files", "Temporary_Files", "MODELO_IGAC", "INSUMOS", "Apex_Unidad"),
            'config_file': os.path.join(self.project_root, "Files", "Temporary_Files", "array_config.txt")
        }
        
        # Crear directorios necesarios si no existen
        for path in ['output', 'apex_terreno', 'apex_unidad']:
            if not os.path.exists(self.paths[path]):
                os.makedirs(self.paths[path])
                print(f"Directorio creado: {self.paths[path]}")
        
        # Encontrar la geodatabase
        self.paths['gdb'] = self.find_geodatabase()
        
        # Encontrar archivos CSV
        self.paths['terreno_csv'] = self.find_csv(self.paths['apex_terreno'])
        self.paths['unidad_csv'] = self.find_csv(self.paths['apex_unidad'])

    def find_geodatabase(self):
        """Encuentra la primera geodatabase en el directorio base"""
        try:
            gdb_path = self.paths['temp_base']
            print(f"Buscando GDB en: {gdb_path}")
            
            if not os.path.exists(gdb_path):
                raise Exception(f"El directorio {gdb_path} no existe")
            
            for item in os.listdir(gdb_path):
                if item.endswith('.gdb'):
                    full_path = os.path.join(gdb_path, item)
                    print(f"GDB encontrada: {full_path}")
                    return full_path
                    
            raise Exception(f"No se encontró ninguna geodatabase en {gdb_path}")
        except Exception as e:
            print(f"Error buscando geodatabase: {str(e)}")
            raise
    def find_csv(self, directory):
        """Encuentra el primer archivo CSV en el directorio especificado"""
        try:
            csv_files = glob.glob(os.path.join(directory, "*.csv"))
            if not csv_files:
                raise Exception(f"No se encontró ningún archivo CSV en {directory}")
            return csv_files[0]
        except Exception as e:
            self.log_error(f"Error buscando archivo CSV: {str(e)}")
            raise

    def setup_logging(self):
        """Configura el sistema de logging"""
        try:
            today = datetime.now().date()
            self.log_file = os.path.join(self.paths['output'], f"PROCESO_OMISION_COMISION_{today}.log")
            
            # Configurar logging básico para errores críticos solamente
            logging.basicConfig(
                level=logging.ERROR,  # Solo mostrar errores
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(self.log_file)
                ]
            )
            
            self.logger = logging.getLogger(__name__)
            
            # Silenciar todos los loggers externos
            for log_name, log_obj in logging.Logger.manager.loggerDict.items():
                if log_name != __name__:
                    log_obj.disabled = True
                    
        except Exception as e:
            print(f"Error configurando logging: {str(e)}")
            raise

    def determine_zone(self):
        """Determina la zona a procesar basado en el archivo de configuración"""
        try:
            active_datasets = []
            with open(self.paths['config_file'], 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        dataset_name = line.strip('",')
                        if dataset_name:
                            active_datasets.append(dataset_name)
            
            has_urban = any('URBANO' in ds for ds in active_datasets)
            has_rural = any('RURAL' in ds for ds in active_datasets)
            
            if has_urban and has_rural:
                return "Urbana-Rural"
            elif has_urban:
                return "Urbana"
            elif has_rural:
                return "Rural"
            else:
                raise Exception("No se pudo determinar la zona a procesar")
        except Exception as e:
            self.log_error(f"Error determinando zona: {str(e)}")
            raise

    def clean_temp_data(self):
        """Limpia datos temporales"""
        try:
            self.logger.info("Limpiando datos temporales...")
            
            # Limpiar vistas temporales
            temp_views = ['vista_terreno', 'vista_geografica', 'vista_unidad', 'temp_vista_geografica']
            for view in temp_views:
                if arcpy.Exists(view):
                    arcpy.Delete_management(view)
            
            # Limpiar workspace temporal
            arcpy.Delete_management("in_memory")
            
            self.logger.info("Limpieza de datos temporales completada")
        except Exception as e:
            self.logger.warning(f"Advertencia durante limpieza: {str(e)}")

    def log_error(self, message):
        """Registra un error en el log"""
        self.logger.error(message)
        print(f"ERROR: {message}")

    def log_message(self, message):
        """Registra un mensaje en el log"""
        self.logger.info(message)
        print(message)
        
    def create_r1_tables(self):
        """Crea las tablas R1_TERRENO y R1_UNIDAD si no existen"""
        try:
            self.log_message("Verificando y creando tablas necesarias...")
            
            # Definir la estructura de R1_TERRENO
            if not arcpy.Exists(os.path.join(self.paths['gdb'], "R1_TERRENO")):
                self.log_message("Creando tabla R1_TERRENO...")
                arcpy.CreateTable_management(self.paths['gdb'], "R1_TERRENO")
                terreno_table = os.path.join(self.paths['gdb'], "R1_TERRENO")
                
                # Añadir campos a R1_TERRENO
                arcpy.AddField_management(terreno_table, "Numero_Predial", "TEXT", field_length=255)
                arcpy.AddField_management(terreno_table, "Etapa", "TEXT", field_length=255)
                arcpy.AddField_management(terreno_table, "Destino", "TEXT", field_length=255)
                arcpy.AddField_management(terreno_table, "Estado", "TEXT", field_length=255)
                self.log_message("Tabla R1_TERRENO creada exitosamente")
            else:
                self.log_message("La tabla R1_TERRENO ya existe")

            # Definir la estructura de R1_UNIDAD
            if not arcpy.Exists(os.path.join(self.paths['gdb'], "R1_UNIDAD")):
                self.log_message("Creando tabla R1_UNIDAD...")
                arcpy.CreateTable_management(self.paths['gdb'], "R1_UNIDAD")
                unidad_table = os.path.join(self.paths['gdb'], "R1_UNIDAD")
                
                # Añadir campos a R1_UNIDAD
                arcpy.AddField_management(unidad_table, "Municipio_Codigo", "TEXT", field_length=255)
                arcpy.AddField_management(unidad_table, "Numero_Predial", "TEXT", field_length=255)
                arcpy.AddField_management(unidad_table, "Estado", "TEXT", field_length=255)
                arcpy.AddField_management(unidad_table, "Tipo_Construccion", "TEXT", field_length=255)
                arcpy.AddField_management(unidad_table, "Unidad", "TEXT", field_length=255)
                arcpy.AddField_management(unidad_table, "Etapa", "TEXT", field_length=255)
                self.log_message("Tabla R1_UNIDAD creada exitosamente")
            else:
                self.log_message("La tabla R1_UNIDAD ya existe")

            return True

        except Exception as e:
            self.log_error(f"Error creando las tablas: {str(e)}")
            return False

    def process_table_to_dataframe(self, table_view, fields=None):
        """Función auxiliar para convertir una tabla o vista a DataFrame manejando valores nulos"""
        try:
            if not fields:
                fields = [field.name for field in arcpy.ListFields(table_view)]
            
            data = []
            with arcpy.da.SearchCursor(table_view, fields) as cursor:
                for row in cursor:
                    # Reemplazar None con valores vacíos
                    cleaned_row = ['' if v is None else v for v in row]
                    data.append(cleaned_row)
            
            return pd.DataFrame(data, columns=fields)
        except Exception as e:
            self.log_error(f"Error convirtiendo tabla a DataFrame: {str(e)}")
            raise

    def omision_terrenos(self, zona):
        """Analiza la omisión de terrenos"""
        try:
            self.log_message("Analizando omisión de terrenos...")
            
            apex_terreno = os.path.join(self.paths['gdb'], "R1_TERRENO")
            capa_geografica = os.path.join(self.paths['gdb'], "TERRENO_TOTAL")
            output_excel = os.path.join(self.paths['output'], f"1_Omision_Terrenos_{zona}.xlsx")

            subzonas = ["Urbana", "Rural"] if zona == "Urbana-Rural" else [zona]
            writer = pd.ExcelWriter(output_excel, engine='xlsxwriter')

            for subzona in subzonas:
                try:
                    arcpy.MakeTableView_management(apex_terreno, "vista_terreno")
                    arcpy.MakeTableView_management(capa_geografica, "vista_geografica")

                    arcpy.management.AddJoin(
                        in_layer_or_view="vista_terreno",
                        in_field="Numero_Predial",
                        join_table="vista_geografica",
                        join_field="CODIGO",
                        join_type="KEEP_ALL"
                    )

                    where_clause = "Estado = 'ACTIVO' AND CODIGO IS NULL AND SUBSTRING(Numero_Predial,22,1) IN ('0','8','2')"
                    if subzona == 'Urbana':
                        where_clause += " AND SUBSTRING(Numero_Predial,6,2) <>'00'"
                    elif subzona == 'Rural':
                        where_clause += " AND SUBSTRING(Numero_Predial,6,2) ='00'"

                    arcpy.management.SelectLayerByAttribute(
                        in_layer_or_view="vista_terreno",
                        selection_type="NEW_SELECTION",
                        where_clause=where_clause
                    )

                    df = self.process_table_to_dataframe("vista_terreno")
                    df.to_excel(writer, sheet_name=f'Omision_{subzona}', index=False)
                    
                    count = len(df)
                    self.log_message(f"Cantidad de registros de omisión {subzona}: {count}")

                except Exception as e:
                    self.log_error(f"Error procesando {subzona}: {str(e)}")
                finally:
                    self.clean_temp_data()

            writer.close()
            self.log_message("Análisis de omisión de terrenos completado")

        except Exception as e:
            self.log_error(f"Error en omisión de terrenos: {str(e)}")
            raise

    def comision_terrenos(self, zona):
        """Analiza la comisión de terrenos"""
        try:
            self.log_message("Analizando comisión de terrenos...")
            
            apex_terreno = os.path.join(self.paths['gdb'], "R1_TERRENO")
            capa_geografica = os.path.join(self.paths['gdb'], "TERRENO_TOTAL")
            output_excel = os.path.join(self.paths['output'], f"2_Comision_Terrenos_{zona}.xlsx")

            subzonas = ["Urbana", "Rural"] if zona == "Urbana-Rural" else [zona]
            writer = pd.ExcelWriter(output_excel, engine='xlsxwriter')

            for subzona in subzonas:
                try:
                    arcpy.MakeTableView_management(apex_terreno, "vista_terreno")
                    arcpy.management.MakeFeatureLayer(capa_geografica, "temp_vista_geografica")

                    arcpy.management.AddJoin(
                        in_layer_or_view="temp_vista_geografica",
                        in_field="CODIGO",
                        join_table="vista_terreno",
                        join_field="Numero_Predial",
                        join_type="KEEP_ALL"
                    )

                    where_clause = "Numero_Predial IS NULL AND SUBSTRING(CODIGO,22,1) IN ('0','8','2')"
                    if subzona == 'Urbana':
                        where_clause += " AND SUBSTRING(CODIGO,6,2) <>'00'"
                    elif subzona == 'Rural':
                        where_clause += " AND SUBSTRING(CODIGO,6,2) ='00'"

                    arcpy.management.SelectLayerByAttribute(
                        in_layer_or_view="temp_vista_geografica",
                        selection_type="NEW_SELECTION",
                        where_clause=where_clause
                    )

                    output_subfolder = os.path.join(self.paths['output'], f"2_Shp_Comision_Terrenos_{zona}")
                    if not os.path.exists(output_subfolder):
                        os.makedirs(output_subfolder)

                    output_shp = os.path.join(output_subfolder, f"2_Comision_Terrenos_{subzona}.shp")
                    arcpy.management.CopyFeatures("temp_vista_geografica", output_shp)

                    df = self.process_table_to_dataframe("temp_vista_geografica")
                    df.to_excel(writer, sheet_name=f'Comision_{subzona}', index=False)
                    
                    count = len(df)
                    self.log_message(f"Cantidad de registros de comisión {subzona}: {count}")

                except Exception as e:
                    self.log_error(f"Error procesando {subzona}: {str(e)}")
                finally:
                    self.clean_temp_data()

            writer.close()
            self.log_message("Análisis de comisión de terrenos completado")

        except Exception as e:
            self.log_error(f"Error en comisión de terrenos: {str(e)}")
            raise

    def omision_unidades(self, zona):
        """Analiza la omisión de unidades"""
        try:
            self.log_message("Analizando omisión de unidades...")
            
            apex_unidad = os.path.join(self.paths['gdb'], "R1_UNIDAD")
            capa_geografica = os.path.join(self.paths['gdb'], "UNIDAD_TOTAL")
            output_excel = os.path.join(self.paths['output'], f"3_Omision_Unidades_{zona}.xlsx")

            subzonas = ["Urbana", "Rural"] if zona == "Urbana-Rural" else [zona]
            writer = pd.ExcelWriter(output_excel, engine='xlsxwriter')

            for subzona in subzonas:
                try:
                    arcpy.MakeTableView_management(apex_unidad, "vista_unidad")
                    arcpy.MakeTableView_management(capa_geografica, "vista_geografica")

                    codigo_nuevo_terr = "codigo_nuevo_terreno"
                    codigo_nuevo_unidad = "codigo_nuevo_unidad"
                    
                    expresion_ter = "!Numero_Predial![0:22]+'0000'+!Numero_Predial![26:30] + '_' + !Unidad! + '_' + !TIPO_CONSTRUCCION!"
                    expresion_gra = (
                        "!CODIGO![0:22] + '0000' + !CODIGO![26:30] + '_' + "
                        "!IDENTIFICADOR! + '_' + ('NO_CONVENCIONAL' if !TIPO_CONSTRUCCION! == 'NO CONVENCIONAL' else !TIPO_CONSTRUCCION!)"
                    )

                    arcpy.CalculateField_management("vista_unidad", codigo_nuevo_terr, expresion_ter, "PYTHON3")
                    arcpy.CalculateField_management("vista_geografica", codigo_nuevo_unidad, expresion_gra, "PYTHON3")

                    arcpy.management.AddJoin(
                        in_layer_or_view="vista_unidad",
                        in_field=codigo_nuevo_terr,
                        join_table="vista_geografica",
                        join_field=codigo_nuevo_unidad,
                        join_type="KEEP_ALL"
                    )

                    where_clause = "Estado = 'ACTIVO' AND codigo_nuevo_unidad IS NULL AND SUBSTRING(Numero_Predial,22,1) IN ('0','2','5','8')"
                    if subzona == 'Urbana':
                        where_clause += " AND SUBSTRING(Numero_Predial,6,2) <>'00'"
                    elif subzona == 'Rural':
                        where_clause += " AND SUBSTRING(Numero_Predial,6,2) ='00'"

                    arcpy.management.SelectLayerByAttribute(
                        in_layer_or_view="vista_unidad",
                        selection_type="NEW_SELECTION",
                        where_clause=where_clause
                    )

                    df = self.process_table_to_dataframe("vista_unidad")
                    df.to_excel(writer, sheet_name=f'Omision_{subzona}', index=False)
                    
                    count = len(df)
                    self.log_message(f"Cantidad de registros de omisión {subzona}: {count}")

                except Exception as e:
                    self.log_error(f"Error procesando {subzona}: {str(e)}")
                finally:
                    arcpy.DeleteField_management("vista_unidad", codigo_nuevo_terr)
                    arcpy.DeleteField_management("vista_geografica", codigo_nuevo_unidad)
                    self.clean_temp_data()

            writer.close()
            self.log_message("Análisis de omisión de unidades completado")

        except Exception as e:
            self.log_error(f"Error en omisión de unidades: {str(e)}")
            raise

    def comision_unidades(self, zona):
        """Analiza la comisión de unidades"""
        try:
            self.log_message("Analizando comisión de unidades...")
            
            apex_unidad = os.path.join(self.paths['gdb'], "R1_UNIDAD")
            capa_geografica = os.path.join(self.paths['gdb'], "UNIDAD_TOTAL")
            output_excel = os.path.join(self.paths['output'], f"4_Comision_Unidades_Construccion_{zona}.xlsx")

            subzonas = ["Urbana", "Rural"] if zona == "Urbana-Rural" else [zona]
            writer = pd.ExcelWriter(output_excel, engine='xlsxwriter')

            for subzona in subzonas:
                try:
                    arcpy.MakeTableView_management(apex_unidad, "vista_unidad")
                    arcpy.management.MakeFeatureLayer(capa_geografica, "vista_geografica")

                    codigo_nuevo_terr = "codigo_nuevo_terreno"
                    codigo_nuevo_unidad = "codigo_nuevo_unidad"
                    
                    expresion_ter = "!Numero_Predial![0:22]+'0000'+!Numero_Predial![26:30] + '_' + !Unidad! + '_' + !TIPO_CONSTRUCCION!"
                    expresion_gra = (
                        "!CODIGO![0:22] + '0000' + !CODIGO![26:30] + '_' + "
                        "!IDENTIFICADOR! + '_' + ('NO_CONVENCIONAL' if !TIPO_CONSTRUCCION! == 'NO CONVENCIONAL' else !TIPO_CONSTRUCCION!)"
                    )

                    arcpy.CalculateField_management("vista_unidad", codigo_nuevo_terr, expresion_ter, "PYTHON3")
                    arcpy.CalculateField_management("vista_geografica", codigo_nuevo_unidad, expresion_gra, "PYTHON3")

                    arcpy.management.AddJoin(
                        in_layer_or_view="vista_geografica",
                        in_field=codigo_nuevo_unidad,
                        join_table="vista_unidad",
                        join_field=codigo_nuevo_terr,
                        join_type="KEEP_ALL"
                    )

                    where_clause = "Numero_Predial IS NULL AND SUBSTRING(CODIGO,22,1) IN ('0','2','5','8')"
                    if subzona == 'Urbana':
                        where_clause += " AND SUBSTRING(CODIGO,6,2) <>'00'"
                    elif subzona == 'Rural':
                        where_clause += " AND SUBSTRING(CODIGO,6,2) ='00'"

                    arcpy.management.SelectLayerByAttribute(
                        in_layer_or_view="vista_geografica",
                        selection_type="NEW_SELECTION",
                        where_clause=where_clause
                    )

                    output_subfolder = os.path.join(self.paths['output'], f"4_Shp_Comision_Unidades_{zona}")
                    if not os.path.exists(output_subfolder):
                        os.makedirs(output_subfolder)

                    output_shp = os.path.join(output_subfolder, f"4_Comision_Unidades_{subzona}.shp")
                    arcpy.management.CopyFeatures("vista_geografica", output_shp)

                    df = self.process_table_to_dataframe("vista_geografica")
                    df.to_excel(writer, sheet_name=f'Comision_{subzona}', index=False)
                    
                    count = len(df)
                    self.log_message(f"Cantidad de registros de comisión {subzona}: {count}")

                except Exception as e:
                    self.log_error(f"Error procesando {subzona}: {str(e)}")
                finally:
                    arcpy.DeleteField_management("vista_unidad", codigo_nuevo_terr)
                    arcpy.DeleteField_management("vista_geografica", codigo_nuevo_unidad)
                    self.clean_temp_data()

            writer.close()
            self.log_message("Análisis de comisión de unidades completado")

        except Exception as e:
            self.log_error(f"Error en comisión de unidades: {str(e)}")
            raise
    
    def load_r1_data(self):
        """Carga los datos de R1 (terrenos y unidades)"""
        try:
            # Silenciar los mensajes de Pandas
            import warnings
            warnings.filterwarnings('ignore')
            
            # Obtener rutas de las tablas
            fgdb_table_r1 = os.path.join(self.paths['gdb'], 'R1_TERRENO')
            fgdb_table_r2 = os.path.join(self.paths['gdb'], 'R1_UNIDAD')
            chunksize = 500000

            def get_column_name(df, possible_names):
                """Busca una columna que coincida con cualquiera de los nombres posibles"""
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            # Limpiar tablas existentes
            arcpy.TruncateTable_management(fgdb_table_r1)
            arcpy.TruncateTable_management(fgdb_table_r2)

            self.log_message("Iniciando carga de datos...")
            
            # Cargar datos de terrenos (R1)
            try:
                arcpy.SetProgressor("step", "Cargando datos de terrenos...", 0, 100, 1)
                
                # Definir tipos de columnas y sus posibles nombres
                column_types = {
                    'Numero_Predial': str, 
                    'Numero Predial': str,
                    'Etapa': str, 
                    'Destino': str, 
                    'Estado': str
                }
                
                # Definir variantes de nombres
                column_variants = {
                    'Numero_Predial': ['Numero_Predial', 'Numero Predial'],
                    'Etapa': ['Etapa'],
                    'Destino': ['Destino'],
                    'Estado': ['Estado']
                }
                
                # Leer primera fila para detectar nombres
                df_sample = pd.read_csv(self.paths['terreno_csv'], nrows=1, encoding="ISO-8859-1")
                
                # Crear mapping basado en los nombres encontrados
                column_mapping = {}
                field_mapping = {}
                
                for target_field, variants in column_variants.items():
                    found_column = get_column_name(df_sample, variants)
                    if found_column:
                        if found_column != target_field:
                            column_mapping[found_column] = target_field
                        field_mapping[target_field] = target_field
                    else:
                        raise ValueError(f"No se encontró la columna {target_field}")

                for chunk in pd.read_csv(self.paths['terreno_csv'], dtype=column_types, chunksize=chunksize, sep=',', encoding="ISO-8859-1"):
                    if column_mapping:
                        chunk = chunk.rename(columns=column_mapping)
                    
                    keys = list(field_mapping.keys())
                    with arcpy.da.InsertCursor(fgdb_table_r1, keys) as insert_cur:
                        for row in chunk.itertuples(index=False):
                            try:
                                new_row = [getattr(row, field_mapping[key]) for key in keys]
                                insert_cur.insertRow(new_row)
                            except Exception as e:
                                pass  # Silenciar errores individuales
                                
                count = int(arcpy.GetCount_management(fgdb_table_r1).getOutput(0))
                self.log_message(f"Total registros en R1: {count}")
                    
            except Exception as e:
                self.log_error(f"Error en carga de terrenos: {str(e)}")
                raise

            # Cargar datos de unidades (R2)
            try:
                arcpy.SetProgressor("step", "Cargando datos de unidades...", 0, 100, 1)
                
                # Definir tipos de columnas y sus posibles nombres
                column_types = {
                    'Municipio_Codigo': str,
                    'Municipio Codigo': str,
                    'Numero_Predial': str,
                    'Numero Predial': str,
                    'Estado': str,
                    'Tipo_Construccion': str,
                    'Tipo Construccion': str,
                    'Unidad': str,
                    'Etapa': str
                }
                
                # Definir variantes de nombres
                column_variants = {
                    'Municipio_Codigo': ['Municipio_Codigo', 'Municipio Codigo'],
                    'Numero_Predial': ['Numero_Predial', 'Numero Predial'],
                    'Estado': ['Estado'],
                    'Tipo_Construccion': ['Tipo_Construccion', 'Tipo Construccion'],
                    'Unidad': ['Unidad'],
                    'Etapa': ['Etapa']
                }
                
                # Leer primera fila para detectar nombres
                df_sample = pd.read_csv(self.paths['unidad_csv'], nrows=1, encoding="ISO-8859-1")
                
                # Crear mapping basado en los nombres encontrados
                column_mapping = {}
                field_mapping = {}
                
                for target_field, variants in column_variants.items():
                    found_column = get_column_name(df_sample, variants)
                    if found_column:
                        if found_column != target_field:
                            column_mapping[found_column] = target_field
                        field_mapping[target_field] = target_field
                    else:
                        raise ValueError(f"No se encontró la columna {target_field}")

                for chunk in pd.read_csv(self.paths['unidad_csv'], dtype=column_types, chunksize=chunksize, sep=',', encoding="ISO-8859-1"):
                    if column_mapping:
                        chunk = chunk.rename(columns=column_mapping)
                    
                    keys = list(field_mapping.keys())
                    with arcpy.da.InsertCursor(fgdb_table_r2, keys) as insert_cur:
                        for row in chunk.itertuples(index=False):
                            try:
                                new_row = [getattr(row, field_mapping[key]) for key in keys]
                                insert_cur.insertRow(new_row)
                            except Exception as e:
                                pass  # Silenciar errores individuales
                                
                count = int(arcpy.GetCount_management(fgdb_table_r2).getOutput(0))
                self.log_message(f"Total registros en R2: {count}")
                    
            except Exception as e:
                self.log_error(f"Error en carga de unidades: {str(e)}")
                raise

        except Exception as e:
            self.log_error(f"Error en la carga de datos: {str(e)}")
            raise
    def comision_terrenos_por_dataset(self, subzona, capa_geografica, writer, output_shp_folder):
        """Procesa la comisión de terrenos para un dataset específico"""
        try:
            self.log_message(f"Procesando comisión de terrenos para {subzona}")
            
            arcpy.MakeTableView_management(os.path.join(self.paths['gdb'], "R1_TERRENO"), "vista_terreno")
            arcpy.management.MakeFeatureLayer(capa_geografica, "temp_vista_geografica")

            arcpy.management.AddJoin(
                in_layer_or_view="temp_vista_geografica",
                in_field="CODIGO",
                join_table="vista_terreno",
                join_field="Numero_Predial",
                join_type="KEEP_ALL"
            )

            where_clause = "Numero_Predial IS NULL AND SUBSTRING(CODIGO,22,1) IN ('0','8','2')"
            if subzona == 'Urbana':
                where_clause += " AND SUBSTRING(CODIGO,6,2) <>'00'"
            else:  # Rural
                where_clause += " AND SUBSTRING(CODIGO,6,2) ='00'"

            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view="temp_vista_geografica",
                selection_type="NEW_SELECTION",
                where_clause=where_clause
            )

            # Exportar shapefile
            output_shp = os.path.join(output_shp_folder, f"2_Comision_Terrenos_{subzona}.shp")
            arcpy.management.CopyFeatures("temp_vista_geografica", output_shp)

            # Exportar a Excel
            df = self.process_table_to_dataframe("temp_vista_geografica")
            df.to_excel(writer, sheet_name=f'Comision_{subzona}', index=False)

            count = len(df)
            self.log_message(f"Registros de comisión {subzona}: {count}")

        except Exception as e:
            self.log_error(f"Error en comisión de terrenos para {subzona}: {str(e)}")
        finally:
            self.clean_temp_data()
    def determine_datasets(self):
        """Determina los datasets a procesar basado en el archivo de configuración"""
        try:
            active_datasets = []
            dataset_mappings = {}
            
            with open(self.paths['config_file'], 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        dataset_name = line.strip('",')
                        if dataset_name:
                            # Identificar el tipo de dataset y la zona
                            if 'URBANO' in dataset_name:
                                zona = 'URBANO'
                            elif 'RURAL' in dataset_name:
                                zona = 'RURAL'
                            else:
                                continue
                                
                            # Crear la ruta completa del dataset
                            if '_CTM12' in dataset_name:
                                base_path = os.path.join(self.paths['gdb'], f"{zona}_CTM12")
                            else:
                                base_path = os.path.join(self.paths['gdb'], zona)
                                
                            active_datasets.append(dataset_name)
                            dataset_mappings[dataset_name] = base_path
            
            return active_datasets, dataset_mappings
        except Exception as e:
            self.log_error(f"Error determinando datasets: {str(e)}")
            raise

    def process_terrenos(self, zona):
        """Procesa los datos de terrenos (omisión y comisión)"""
        try:
            self.log_message("Procesando datos de terrenos...")
            
            # Obtener datasets activos y sus rutas
            active_datasets, dataset_mappings = self.determine_datasets()
            
            # Diccionario para almacenar las capas de terrenos por zona
            terreno_layers = {
                'URBANO': [],
                'RURAL': []
            }
            
            # Procesar cada dataset activo
            for dataset in active_datasets:
                base_path = dataset_mappings[dataset]
                if 'URBANO' in dataset:
                    if '_CTM12' in dataset:
                        terreno_layers['URBANO'].extend([
                            os.path.join(base_path, "U_TERRENO_CTM12"),
                            os.path.join(base_path, "U_TERRENO_INFORMAL")
                        ])
                    else:
                        terreno_layers['URBANO'].append(os.path.join(base_path, "U_TERRENO"))
                elif 'RURAL' in dataset:
                    if '_CTM12' in dataset:
                        terreno_layers['RURAL'].extend([
                            os.path.join(base_path, "R_TERRENO_CTM12"),
                            os.path.join(base_path, "R_TERRENO_INFORMAL")
                        ])
                    else:
                        terreno_layers['RURAL'].append(os.path.join(base_path, "R_TERRENO"))
            
            # Crear capas unificadas por tipo
            output_layers = {}
            
            if terreno_layers['URBANO']:
                output_terreno_urbano = os.path.join(self.paths['gdb'], "TERRENO_URBANO")
                if arcpy.Exists(output_terreno_urbano):
                    arcpy.Delete_management(output_terreno_urbano)
                arcpy.Merge_management([layer for layer in terreno_layers['URBANO'] if arcpy.Exists(layer)], output_terreno_urbano)
                output_layers['URBANO'] = output_terreno_urbano
                
            if terreno_layers['RURAL']:
                output_terreno_rural = os.path.join(self.paths['gdb'], "TERRENO_RURAL")
                if arcpy.Exists(output_terreno_rural):
                    arcpy.Delete_management(output_terreno_rural)
                arcpy.Merge_management([layer for layer in terreno_layers['RURAL'] if arcpy.Exists(layer)], output_terreno_rural)
                output_layers['RURAL'] = output_terreno_rural

            # Crear capa unificada total
            output_terreno = os.path.join(self.paths['gdb'], "TERRENO_TOTAL")
            if arcpy.Exists(output_terreno):
                arcpy.Delete_management(output_terreno)
            
            layers_to_merge = []
            if 'URBANO' in output_layers and zona in ['Urbana', 'Urbana-Rural']:
                layers_to_merge.append(output_layers['URBANO'])
            if 'RURAL' in output_layers and zona in ['Rural', 'Urbana-Rural']:
                layers_to_merge.append(output_layers['RURAL'])
                
            if layers_to_merge:
                arcpy.Merge_management(layers_to_merge, output_terreno)

            # Procesar omisión
            self.omision_terrenos(zona)
            
            # Procesar comisión según zona
            writer = pd.ExcelWriter(os.path.join(self.paths['output'], f"2_Comision_Terrenos_{zona}.xlsx"), engine='xlsxwriter')
            output_shp_folder = os.path.join(self.paths['output'], f"2_Shp_Comision_Terrenos_{zona}")
            
            if not os.path.exists(output_shp_folder):
                os.makedirs(output_shp_folder)

            if 'URBANO' in output_layers and zona in ['Urbana', 'Urbana-Rural']:
                self.comision_terrenos_por_dataset('Urbana', output_layers['URBANO'], writer, output_shp_folder)
                
            if 'RURAL' in output_layers and zona in ['Rural', 'Urbana-Rural']:
                self.comision_terrenos_por_dataset('Rural', output_layers['RURAL'], writer, output_shp_folder)
                
            writer.close()
            
        except Exception as e:
            self.log_error(f"Error procesando terrenos: {str(e)}")
            raise

    def process_unidades(self, zona):
        """Procesa los datos de unidades (omisión y comisión)"""
        try:
            self.log_message("Procesando datos de unidades...")
            
            # Obtener datasets activos y sus rutas
            active_datasets, dataset_mappings = self.determine_datasets()
            
            # Diccionario para almacenar las capas de unidades por zona
            unidad_layers = {
                'URBANO': [],
                'RURAL': []
            }
            
            # Procesar cada dataset activo
            for dataset in active_datasets:
                base_path = dataset_mappings[dataset]
                if 'URBANO' in dataset:
                    if '_CTM12' in dataset:
                        unidad_layers['URBANO'].extend([
                            os.path.join(base_path, "U_UNIDAD_CTM12"),
                            os.path.join(base_path, "U_UNIDAD_INFORMAL")
                        ])
                    else:
                        unidad_layers['URBANO'].append(os.path.join(base_path, "U_UNIDAD"))
                elif 'RURAL' in dataset:
                    if '_CTM12' in dataset:
                        unidad_layers['RURAL'].extend([
                            os.path.join(base_path, "R_UNIDAD_CTM12"),
                            os.path.join(base_path, "R_UNIDAD_INFORMAL")
                        ])
                    else:
                        unidad_layers['RURAL'].append(os.path.join(base_path, "R_UNIDAD"))
            
            # Crear capas unificadas por tipo
            output_layers = {}
            
            if unidad_layers['URBANO']:
                output_unidad_urbana = os.path.join(self.paths['gdb'], "UNIDAD_URBANA")
                if arcpy.Exists(output_unidad_urbana):
                    arcpy.Delete_management(output_unidad_urbana)
                arcpy.Merge_management([layer for layer in unidad_layers['URBANO'] if arcpy.Exists(layer)], output_unidad_urbana)
                output_layers['URBANO'] = output_unidad_urbana
                
            if unidad_layers['RURAL']:
                output_unidad_rural = os.path.join(self.paths['gdb'], "UNIDAD_RURAL")
                if arcpy.Exists(output_unidad_rural):
                    arcpy.Delete_management(output_unidad_rural)
                arcpy.Merge_management([layer for layer in unidad_layers['RURAL'] if arcpy.Exists(layer)], output_unidad_rural)
                output_layers['RURAL'] = output_unidad_rural

            # Crear capa unificada total
            output_unidad = os.path.join(self.paths['gdb'], "UNIDAD_TOTAL")
            if arcpy.Exists(output_unidad):
                arcpy.Delete_management(output_unidad)
            
            layers_to_merge = []
            if 'URBANO' in output_layers and zona in ['Urbana', 'Urbana-Rural']:
                layers_to_merge.append(output_layers['URBANO'])
            if 'RURAL' in output_layers and zona in ['Rural', 'Urbana-Rural']:
                layers_to_merge.append(output_layers['RURAL'])
                
            if layers_to_merge:
                arcpy.Merge_management(layers_to_merge, output_unidad)

            # Procesar omisión
            self.omision_unidades(zona)
            
            # Procesar comisión según zona
            writer = pd.ExcelWriter(os.path.join(self.paths['output'], f"4_Comision_Unidades_Construccion_{zona}.xlsx"), engine='xlsxwriter')
            output_shp_folder = os.path.join(self.paths['output'], f"4_Shp_Comision_Unidades_{zona}")
            
            if not os.path.exists(output_shp_folder):
                os.makedirs(output_shp_folder)

            if 'URBANO' in output_layers and zona in ['Urbana', 'Urbana-Rural']:
                self.comision_unidades_por_dataset('Urbana', output_layers['URBANO'], writer, output_shp_folder)
                
            if 'RURAL' in output_layers and zona in ['Rural', 'Urbana-Rural']:
                self.comision_unidades_por_dataset('Rural', output_layers['RURAL'], writer, output_shp_folder)
                
            writer.close()
            
        except Exception as e:
            self.log_error(f"Error procesando unidades: {str(e)}")
            raise
    def comision_unidades_por_dataset(self, subzona, capa_geografica, writer, output_shp_folder):
        """Procesa la comisión de unidades para un dataset específico"""
        try:
            self.log_message(f"Procesando comisión de unidades para {subzona}")
            
            arcpy.MakeTableView_management(os.path.join(self.paths['gdb'], "R1_UNIDAD"), "vista_unidad")
            arcpy.management.MakeFeatureLayer(capa_geografica, "vista_geografica")

            codigo_nuevo_terr = "codigo_nuevo_terreno"
            codigo_nuevo_unidad = "codigo_nuevo_unidad"
            
            expresion_ter = "!Numero_Predial![0:22]+'0000'+!Numero_Predial![26:30] + '_' + !Unidad! + '_' + !TIPO_CONSTRUCCION!"
            expresion_gra = (
                "!CODIGO![0:22] + '0000' + !CODIGO![26:30] + '_' + "
                "!IDENTIFICADOR! + '_' + ('NO_CONVENCIONAL' if !TIPO_CONSTRUCCION! == 'NO CONVENCIONAL' else !TIPO_CONSTRUCCION!)"
            )

            arcpy.CalculateField_management("vista_unidad", codigo_nuevo_terr, expresion_ter, "PYTHON3")
            arcpy.CalculateField_management("vista_geografica", codigo_nuevo_unidad, expresion_gra, "PYTHON3")

            arcpy.management.AddJoin(
                in_layer_or_view="vista_geografica",
                in_field=codigo_nuevo_unidad,
                join_table="vista_unidad",
                join_field=codigo_nuevo_terr,
                join_type="KEEP_ALL"
            )

            where_clause = "Numero_Predial IS NULL AND SUBSTRING(CODIGO,22,1) IN ('0','2','5','8')"
            if subzona == 'Urbana':
                where_clause += " AND SUBSTRING(CODIGO,6,2) <>'00'"
            else:  # Rural
                where_clause += " AND SUBSTRING(CODIGO,6,2) ='00'"

            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view="vista_geografica",
                selection_type="NEW_SELECTION",
                where_clause=where_clause
            )

            # Exportar shapefile
            output_shp = os.path.join(output_shp_folder, f"4_Comision_Unidades_{subzona}.shp")
            arcpy.management.CopyFeatures("vista_geografica", output_shp)

            # Exportar a Excel
            df = self.process_table_to_dataframe("vista_geografica")
            df.to_excel(writer, sheet_name=f'Comision_{subzona}', index=False)

            count = len(df)
            self.log_message(f"Registros de comisión {subzona}: {count}")

        except Exception as e:
            self.log_error(f"Error en comisión de unidades para {subzona}: {str(e)}")
        finally:
            arcpy.DeleteField_management("vista_unidad", codigo_nuevo_terr)
            arcpy.DeleteField_management("vista_geografica", codigo_nuevo_unidad)
            self.clean_temp_data()    
        


    def process_mejoras(self, zona):
        """Procesa los datos de mejoras (omisión y comisión)"""
        try:
            self.log_message("Procesando datos de mejoras...")
            
            # Obtener datasets activos y sus rutas
            active_datasets, dataset_mappings = self.determine_datasets()
            
            # Diccionario para almacenar las capas de mejoras por zona
            mejoras_layers = {
                'URBANO': [],
                'RURAL': []
            }
            
            # Procesar cada dataset activo
            for dataset in active_datasets:
                base_path = dataset_mappings[dataset]
                if 'URBANO' in dataset:
                    if '_CTM12' in dataset:
                        mejoras_layers['URBANO'].extend([
                            os.path.join(base_path, "U_CONSTRUCCION_CTM12"),
                            os.path.join(base_path, "U_CONSTRUCCION_INFORMAL")
                        ])
                    else:
                        mejoras_layers['URBANO'].append(os.path.join(base_path, "U_CONSTRUCCION"))
                elif 'RURAL' in dataset:
                    if '_CTM12' in dataset:
                        mejoras_layers['RURAL'].extend([
                            os.path.join(base_path, "R_CONSTRUCCION_CTM12"),
                            os.path.join(base_path, "R_CONSTRUCCION_INFORMAL")
                        ])
                    else:
                        mejoras_layers['RURAL'].append(os.path.join(base_path, "R_CONSTRUCCION"))
            
            # Crear capas unificadas por tipo
            output_layers = {}
            
            if mejoras_layers['URBANO']:
                output_mejoras_urbana = os.path.join(self.paths['gdb'], "MEJORAS_URBANA")
                if arcpy.Exists(output_mejoras_urbana):
                    arcpy.Delete_management(output_mejoras_urbana)
                arcpy.Merge_management([layer for layer in mejoras_layers['URBANO'] if arcpy.Exists(layer)], output_mejoras_urbana)
                output_layers['URBANO'] = output_mejoras_urbana
                
            if mejoras_layers['RURAL']:
                output_mejoras_rural = os.path.join(self.paths['gdb'], "MEJORAS_RURAL")
                if arcpy.Exists(output_mejoras_rural):
                    arcpy.Delete_management(output_mejoras_rural)
                arcpy.Merge_management([layer for layer in mejoras_layers['RURAL'] if arcpy.Exists(layer)], output_mejoras_rural)
                output_layers['RURAL'] = output_mejoras_rural

            # Crear capa unificada total
            output_mejoras = os.path.join(self.paths['gdb'], "MEJORAS_TOTAL")
            if arcpy.Exists(output_mejoras):
                arcpy.Delete_management(output_mejoras)
            
            layers_to_merge = []
            if 'URBANO' in output_layers and zona in ['Urbana', 'Urbana-Rural']:
                layers_to_merge.append(output_layers['URBANO'])
            if 'RURAL' in output_layers and zona in ['Rural', 'Urbana-Rural']:
                layers_to_merge.append(output_layers['RURAL'])
                
            if layers_to_merge:
                arcpy.Merge_management(layers_to_merge, output_mejoras)

            # Procesar omisión y comisión según zona
            self.omision_mejoras(zona)
            self.comision_mejoras(zona)
            
        except Exception as e:
            self.log_error(f"Error procesando mejoras: {str(e)}")
            raise

    def omision_mejoras(self, zona):
        """Analiza la omisión de mejoras"""
        try:
            self.log_message("Analizando omisión de mejoras...")
            
            apex_terreno = os.path.join(self.paths['gdb'], "R1_TERRENO")
            capa_geografica = os.path.join(self.paths['gdb'], "MEJORAS_TOTAL")
            output_excel = os.path.join(self.paths['output'], f"5_Omision_Mejoras_{zona}.xlsx")

            subzonas = ["Urbana", "Rural"] if zona == "Urbana-Rural" else [zona]
            writer = pd.ExcelWriter(output_excel, engine='xlsxwriter')

            for subzona in subzonas:
                try:
                    arcpy.MakeTableView_management(apex_terreno, "vista_terreno")
                    arcpy.MakeTableView_management(capa_geografica, "vista_geografica")

                    arcpy.management.AddJoin(
                        in_layer_or_view="vista_terreno",
                        in_field="Numero_Predial",
                        join_table="vista_geografica",
                        join_field="CODIGO",
                        join_type="KEEP_ALL"
                    )

                    where_clause = "Estado = 'ACTIVO' AND CODIGO IS NULL AND SUBSTRING(Numero_Predial,22,1) IN ('5')"
                    if subzona == 'Urbana':
                        where_clause += " AND SUBSTRING(Numero_Predial,6,2) <>'00'"
                    elif subzona == 'Rural':
                        where_clause += " AND SUBSTRING(Numero_Predial,6,2) ='00'"

                    arcpy.management.SelectLayerByAttribute(
                        in_layer_or_view="vista_terreno",
                        selection_type="NEW_SELECTION",
                        where_clause=where_clause
                    )

                    df = self.process_table_to_dataframe("vista_terreno")
                    df.to_excel(writer, sheet_name=f'Omision_{subzona}', index=False)
                    
                    count = len(df)
                    self.log_message(f"Cantidad de registros de omisión mejoras {subzona}: {count}")

                except Exception as e:
                    self.log_error(f"Error procesando {subzona}: {str(e)}")
                finally:
                    self.clean_temp_data()

            writer.close()
            self.log_message("Análisis de omisión de mejoras completado")

        except Exception as e:
            self.log_error(f"Error en omisión de mejoras: {str(e)}")
            raise

    def comision_mejoras(self, zona):
        """Analiza la comisión de mejoras"""
        try:
            self.log_message("Analizando comisión de mejoras...")
            
            apex_terreno = os.path.join(self.paths['gdb'], "R1_TERRENO")
            capa_geografica = os.path.join(self.paths['gdb'], "MEJORAS_TOTAL")
            output_excel = os.path.join(self.paths['output'], f"6_Comision_Mejoras_{zona}.xlsx")

            subzonas = ["Urbana", "Rural"] if zona == "Urbana-Rural" else [zona]
            writer = pd.ExcelWriter(output_excel, engine='xlsxwriter')

            for subzona in subzonas:
                try:
                    arcpy.MakeTableView_management(apex_terreno, "vista_terreno")
                    arcpy.management.MakeFeatureLayer(capa_geografica, "vista_geografica")

                    arcpy.management.AddJoin(
                        in_layer_or_view="vista_geografica",
                        in_field="CODIGO",
                        join_table="vista_terreno",
                        join_field="Numero_Predial",
                        join_type="KEEP_ALL"
                    )

                    where_clause = "Numero_Predial IS NULL AND SUBSTRING(CODIGO,22,1) IN ('5')"
                    if subzona == 'Urbana':
                        where_clause += " AND SUBSTRING(CODIGO,6,2) <>'00'"
                    elif subzona == 'Rural':
                        where_clause += " AND SUBSTRING(CODIGO,6,2) ='00'"

                    arcpy.management.SelectLayerByAttribute(
                        in_layer_or_view="vista_geografica",
                        selection_type="NEW_SELECTION",
                        where_clause=where_clause
                    )

                    output_subfolder = os.path.join(self.paths['output'], f"6_Shp_Comision_Mejoras_{zona}")
                    if not os.path.exists(output_subfolder):
                        os.makedirs(output_subfolder)

                    output_shp = os.path.join(output_subfolder, f"6_Comision_Mejoras_{subzona}.shp")
                    arcpy.management.CopyFeatures("vista_geografica", output_shp)

                    df = self.process_table_to_dataframe("vista_geografica")
                    df.to_excel(writer, sheet_name=f'Comision_{subzona}', index=False)
                    
                    count = len(df)
                    self.log_message(f"Cantidad de registros de comisión mejoras {subzona}: {count}")

                except Exception as e:
                    self.log_error(f"Error procesando {subzona}: {str(e)}")
                finally:
                    self.clean_temp_data()

            writer.close()
            self.log_message("Análisis de comisión de mejoras completado")

        except Exception as e:
            self.log_error(f"Error en comisión de mejoras: {str(e)}")
            raise

    def clean_output_files(self, zona):
        """Limpia archivos de salida anteriores"""
        try:
            self.log_message("Limpiando archivos de salida anteriores...")
            
            # Eliminar archivos Excel
            excel_patterns = [
                f"1_Omision_Terrenos_{zona}.xlsx",
                f"2_Comision_Terrenos_{zona}.xlsx",
                f"3_Omision_Unidades_{zona}.xlsx",
                f"4_Comision_Unidades_Construccion_{zona}.xlsx",
                f"5_Omision_Mejoras_{zona}.xlsx",
                f"6_Comision_Mejoras_{zona}.xlsx"
            ]
            
            for pattern in excel_patterns:
                file_path = os.path.join(self.paths['output'], pattern)
                if os.path.exists(file_path):
                    os.remove(file_path)
            
            # Eliminar carpetas de shapefiles
            shp_folders = [
                f"2_Shp_Comision_Terrenos_{zona}",
                f"4_Shp_Comision_Unidades_{zona}",
                f"6_Shp_Comision_Mejoras_{zona}"
            ]
            
            for folder in shp_folders:
                folder_path = os.path.join(self.paths['output'], folder)
                if os.path.exists(folder_path):
                    shutil.rmtree(folder_path)
            
            self.log_message("Limpieza de archivos completada")
        except Exception as e:
            self.logger.warning(f"Advertencia durante limpieza de archivos: {str(e)}")

    def execute_process(self):
        """Ejecuta el proceso completo de análisis de omisión y comisión"""
        try:
            self.log_message("****************************************************************")
            self.log_message("Iniciando proceso de análisis de omisión y comisión")
            self.log_message("****************************************************************")
            
            # Mostrar configuración
            msg = f'''Configuración:
            Carpeta de salida: {self.paths['output']}
            GDB de entrada: {self.paths['gdb']}
            CSV Terreno: {self.paths['terreno_csv']}
            CSV Unidad: {self.paths['unidad_csv']}
            '''
            self.log_message(msg)
            
            # Limpiar datos temporales al inicio
            self.clean_temp_data()
            
            # Determinar zona a procesar
            zona = self.determine_zone()
            self.log_message(f"Zona a procesar: {zona}")
            
            # Limpiar archivos de salida anteriores
            self.clean_output_files(zona)
            
            # Crear y cargar tablas R1
            self.log_message("Creando y cargando tablas R1...")
            if not self.create_r1_tables():
                raise Exception("Error en la creación de tablas R1")
            
            # Cargar datos
            self.log_message("Cargando datos desde archivos CSV...")
            self.load_r1_data()
            
            # Procesar terrenos
            self.log_message("Iniciando procesamiento de terrenos...")
            self.process_terrenos(zona)
            
            # Procesar unidades
            self.log_message("Iniciando procesamiento de unidades...")
            self.process_unidades(zona)
            
            # Procesar mejoras
            self.log_message("Iniciando procesamiento de mejoras...")
            self.process_mejoras(zona)
            
            # Limpieza final
            self.clean_temp_data()
            
            # Mostrar tiempo de ejecución
            self.stop = datetime.now()
            self.log_message("****************************************************************")
            self.log_message(f"Tiempo de inicio: {self.start.isoformat()}")
            self.log_message(f"Tiempo de finalización: {self.stop.isoformat()}")
            self.log_message(f"Tiempo total de ejecución: {str(self.stop - self.start)}")
            self.log_message("****************************************************************")
            self.log_message("Proceso completado exitosamente")
            self.log_message("****************************************************************")
            
        except Exception as e:
            self.log_error("****************************************************************")
            self.log_error(f"Error en la ejecución: {str(e)}")
            self.log_error("****************************************************************")
            raise
    
# Script principal
if __name__ == "__main__":
    try:
        # Crear instancia del procesador
        processor = OmisionComisionProcessor()
        
        # Ejecutar proceso
        processor.execute_process()
        
    except Exception as e:
        print(f"Error en la ejecución principal: {str(e)}")
        logging.error(f"Error en la ejecución principal: {str(e)}")
        raise
    finally:
        print("Proceso finalizado")