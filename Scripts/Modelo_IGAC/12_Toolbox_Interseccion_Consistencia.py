import arcpy
import os
import openpyxl
import warnings
import tempfile
from datetime import datetime

import sys
sys.stdout.reconfigure(encoding='utf-8')  # Para Python 3.7 ( una version anterior podria traer problemas)

class IntersectValidator:
    def __init__(self, proyecto_dir):
        self.proyecto_dir = proyecto_dir
        self.setup_paths()
        self.load_config()
        self.possible_datasets = [
            "URBANO", 
            "URBANO_CTM12", 
            "RURAL", 
            "RURAL_CTM12"
        ]

    def setup_paths(self):
        """Configura las rutas necesarias para el proceso"""
        temp_dir = os.path.join(self.proyecto_dir,  "Files", "Temporary_Files","MODELO_IGAC")
        gdbs = [f for f in os.listdir(temp_dir) if f.endswith('.gdb')]
        if not gdbs:
            raise FileNotFoundError("No se encontró ninguna geodatabase en el directorio temporal")
        
        self.input_gdb = os.path.join(temp_dir, gdbs[0])
        self.output_folder = os.path.join(temp_dir, 'consistencia_geoespacial_temp')
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Crear nombres de GDBs de salida
        in_gdb_name = os.path.splitext(os.path.basename(self.input_gdb))[0]
        self.temp_gdb = os.path.join(self.output_folder, f"{in_gdb_name}_temp.gdb")
        self.output_gdb = os.path.join(self.output_folder, f"{in_gdb_name}_intersect_validacion.gdb")
        self.output_excel = os.path.join(self.output_folder, f"{in_gdb_name}_intersect_validacion.xlsx")

    def load_config(self):
        """Carga la configuración de datasets desde el archivo txt"""
        try:
            config_path = os.path.join(self.proyecto_dir, "Files", "Temporary_Files","array_config.txt")
            
            # Leer el archivo y filtrar los datasets activos
            active_datasets = []
            with open(config_path, 'r') as f:
                content = f.readlines()
                # Unir todas las líneas y evaluar como una lista de Python
                content_str = ''.join(line for line in content)
                try:
                    # Intentar evaluar el contenido como una lista de Python
                    datasets_list = eval(content_str)
                    # Filtrar solo las líneas no comentadas
                    active_datasets = [ds.strip() for ds in datasets_list if isinstance(ds, str) and not ds.startswith('#')]
                except:
                    # Si falla la evaluación, procesar línea por línea
                    for line in content:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            # Limpiar la línea de caracteres especiales
                            dataset_name = line.strip('[]"\', ')
                            if dataset_name:
                                active_datasets.append(dataset_name)

            if not active_datasets:
                raise ValueError("No se encontraron datasets activos en el archivo de configuración")

            self.datasets_to_process = active_datasets

            print("\nConfiguración de datasets cargada:")
            print("--------------------------------")
            print("Datasets a procesar:")
            for ds in self.datasets_to_process:
                print(f"  - {ds}")
            print("--------------------------------\n")

        except Exception as e:
            print(f"Error al cargar configuración: {str(e)}")
            # Configuración por defecto en caso de error
            self.datasets_to_process = ["URBANO_CTM12", "RURAL_CTM12"]
            print("\nUsando configuración por defecto:")
            print("--------------------------------")
            print("Datasets a procesar:")
            for ds in self.datasets_to_process:
                print(f"  - {ds}")
            print("--------------------------------\n")
    def truncate_sheet_name(self, name):
        """Trunca el nombre de la hoja a 31 caracteres y lo hace único"""
        return name[:31]

    def is_empty_or_invalid(self, value):
        """Verifica si un valor es considerado vacío o inválido"""
        try:
            if value is None:  # NULL
                return True
            if isinstance(value, str):
                value = value.strip()
                if value == '':  # String vacío o solo espacios
                    return True
                if value == '0' * 30:  # 30 ceros
                    return True
                if len(value.replace('0', '')) == 0:  # Solo ceros (cualquier longitud)
                    return True
            return False
        except Exception as e:
            print(f"Error al validar valor: {str(e)}")
            return True
        
    def process_intersections(self, intersections, in_gdb, temp_gdb):
        """Procesa las intersecciones básicas"""
        for i, (dataset, fc1, fc2, sql, error_desc) in enumerate(intersections, 1):
            print(f"Procesando intersección {i}: {fc1} con {fc2}")

            if not arcpy.Exists(os.path.join(in_gdb, dataset, fc1)) or not arcpy.Exists(os.path.join(in_gdb, dataset, fc2)):
                print(f"Uno o ambos feature classes no existen en {dataset}. Saltando esta intersección.")
                continue

            intersect_output = os.path.join(temp_gdb, f"INTERSECT_{fc1}_{fc2}")
            
            # Verificar si el feature class existe y eliminarlo si es necesario
            if arcpy.Exists(intersect_output):
                print(f"Eliminando feature class existente: {intersect_output}")
                arcpy.management.Delete(intersect_output)

            try:
                arcpy.analysis.Intersect([os.path.join(in_gdb, dataset, fc1), 
                                        os.path.join(in_gdb, dataset, fc2)], 
                                    intersect_output)
                print(f"[OK] Intersección creada: {intersect_output}")

                # Agregar campos solo si la intersección fue exitosa
                arcpy.management.AddField(intersect_output, "Error_Descripcion", "TEXT", field_length=255)
                arcpy.management.AddField(intersect_output, "Excepcion_Descripcion", "TEXT", field_length=500)
                
            except Exception as e:
                print(f"Error al procesar la intersección: {str(e)}")
                continue

    def process_special_intersections(self, special_intersections, in_gdb, temp_gdb):
        """Procesa las intersecciones especiales"""
        for i, (dataset, fc1, fc2, sql, error_desc, new_field) in enumerate(special_intersections, 1):
            print(f"Procesando intersección especial {i}: {fc1} con {fc2}")

            if not arcpy.Exists(os.path.join(in_gdb, dataset, fc1)) or not arcpy.Exists(os.path.join(in_gdb, dataset, fc2)):
                print(f"Uno o ambos feature classes no existen en {dataset}. Saltando esta intersección.")
                continue

            intersect_output = os.path.join(temp_gdb, f"INTERSECT_{fc1}_{fc2}")
            arcpy.analysis.Intersect([os.path.join(in_gdb, dataset, fc1), 
                                    os.path.join(in_gdb, dataset, fc2)], 
                                   intersect_output)

            arcpy.management.AddField(intersect_output, "Error_Descripcion", "TEXT", field_length=255)
            arcpy.management.AddField(intersect_output, "Excepcion_Descripcion", "TEXT", field_length=500)
            
            print(f"Creando y poblando el campo {new_field} para {intersect_output}")
            arcpy.management.AddField(intersect_output, new_field, "TEXT", field_length=17)
            with arcpy.da.UpdateCursor(intersect_output, ["CODIGO", new_field]) as cursor:
                for row in cursor:
                    row[1] = row[0][:17]
                    cursor.updateRow(row)

    def apply_selection_and_export(self, all_intersections, temp_gdb, out_gdb):
        """Aplica la selección y exporta los resultados"""
        results = []
        for i, intersection in enumerate(all_intersections, 1):
            dataset, fc1, fc2, sql, error_desc = intersection[:5]
            temp_fc = os.path.join(temp_gdb, f"INTERSECT_{fc1}_{fc2}")
            out_fc = os.path.join(out_gdb, f"INTERSECT_{fc1}_{fc2}")

            if not arcpy.Exists(temp_fc):
                print(f"El feature class temporal {temp_fc} no existe. Saltando.")
                continue

            if not os.access(os.path.dirname(out_fc), os.W_OK):
                print(f"No hay permisos de escritura en: {os.path.dirname(out_fc)}")
                continue

            try:
                # Verificar y reparar geometrías antes de copiar
                arcpy.management.RepairGeometry(temp_fc, "DELETE_NULL")
                
                # Crear capa temporal solo una vez
                temp_layer = f"temp_layer_{i}"
                
                # Asegurarse de que no exista la capa temporal
                if arcpy.Exists(temp_layer):
                    arcpy.management.Delete(temp_layer)
                    
                arcpy.management.MakeFeatureLayer(temp_fc, temp_layer)
                arcpy.management.SelectLayerByAttribute(temp_layer, "NEW_SELECTION", sql)
                selected_count = int(arcpy.GetCount_management(temp_layer)[0])

                if selected_count > 0:
                    try:
                        arcpy.management.CopyFeatures(temp_layer, out_fc)
                        print(f"Exportando {selected_count} registros a: {out_fc}")
                    except Exception as e:
                        print(f"Error al copiar features: {str(e)}")
                        print(f"Intentando copiar registro por registro...")
                        arcpy.management.CreateFeatureclass(out_gdb, 
                                                        os.path.basename(out_fc),
                                                        template=temp_layer)
                        with arcpy.da.SearchCursor(temp_layer, "*") as search_cur:
                            with arcpy.da.InsertCursor(out_fc, "*") as insert_cur:
                                for row in search_cur:
                                    try:
                                        insert_cur.insertRow(row)
                                    except Exception as e2:
                                        print(f"Error al insertar registro: {str(e2)}")
                                        continue

                    with arcpy.da.UpdateCursor(out_fc, ["Error_Descripcion"]) as cursor:
                        for row in cursor:
                            row[0] = error_desc
                            cursor.updateRow(row)
                    results.append(out_fc)
                else:
                    print(f"No se encontraron registros que cumplan el criterio para {fc1} y {fc2}")
                    arcpy.management.CreateFeatureclass(out_gdb, f"INTERSECT_{fc1}_{fc2}", 
                                                    template=temp_fc, 
                                                    spatial_reference=arcpy.Describe(temp_fc).spatialReference)
                    results.append(out_fc)

            except Exception as e:
                print(f"Error procesando {fc1} y {fc2}: {str(e)}")
                continue
            finally:
                # Asegurarse de que la capa temporal se elimine incluso si hay errores
                if arcpy.Exists(temp_layer):
                    try:
                        arcpy.management.Delete(temp_layer)
                    except:
                        pass

        return results

    def process_nomenclatura(self, dataset, in_gdb, out_gdb):
        """Procesa la nomenclatura para un dataset específico"""
        # Inicializar todas las variables temporales como None
        temp_layer = None
        temp_layer1 = None
        temp_layer2 = None
        temp_result = None
        temp_result2 = None

        try:
            if dataset == "URBANO_CTM12":
                target_layer = "U_NOMEN_DOMICILIARIA_CTM12"
                join_layer_1 = "U_TERRENO_CTM12"
                join_layer_2 = "U_TERRENO_INFORMAL"
                output_name = "INTERSECT_U_NOMEN_DOMICILIARIA_CTM12_U_TERRENO_CTM12"
                error_msg = "Error: El campo 'TERRENO_CODIGO' de la clase U_NOMEN_DOMICILIARIA_CTM12, NO coincide con el campo 'CODIGO' de la clase U_TERRENO_CTM12, tampoco coincide con el campo 'CODIGO_1' de la clase U_TERRENO_INFORMAL"
            
            else:  # RURAL_CTM12
                target_layer = "R_NOMEN_DOMICILIARIA_CTM12"
                join_layer_1 = "R_TERRENO_CTM12"
                join_layer_2 = "R_TERRENO_INFORMAL"
                output_name = "INTERSECT_R_NOMEN_DOMICILIARIA_CTM12_R_TERRENO_CTM12"
                error_msg = "Error: El campo 'TERRENO_CODIGO' de la clase R_NOMEN_DOMICILIARIA_CTM12, NO coincide con el campo 'CODIGO' de la clase R_TERRENO_CTM12, tampoco coincide con el campo 'CODIGO_1' de la clase R_TERRENO_INFORMAL"
            

            
            # Crear capa temporal inicial
            temp_layer = "temp_layer"
            arcpy.management.MakeFeatureLayer(os.path.join(in_gdb, dataset, target_layer), temp_layer)
            print("[OK] Capa temporal creada")

            # Primer join espacial
            print("\nRealizando primer join espacial...")
            temp_result = "memory/temp_result"
            arcpy.analysis.SpatialJoin(
                target_features=temp_layer,
                join_features=os.path.join(in_gdb, dataset, join_layer_1),
                out_feature_class=temp_result,
                join_operation="JOIN_ONE_TO_ONE",
                join_type="KEEP_ALL",
                match_option="INTERSECT"
            )
            print("[OK] Primer join espacial completado")

            # Procesar primer resultado
            temp_layer1 = "temp_layer1"
            arcpy.management.MakeFeatureLayer(temp_result, temp_layer1)
            print("Procesando primera comparación...")
            self.delete_matching_records(temp_layer1, "TERRENO_CODIGO", "CODIGO")
            print("[OK] Primera comparación completada")

            # Segundo join espacial
            print("\nRealizando segundo join espacial...")
            temp_result2 = "memory/temp_result2"
            arcpy.analysis.SpatialJoin(
                target_features=temp_layer1,
                join_features=os.path.join(in_gdb, dataset, join_layer_2),
                out_feature_class=temp_result2,
                join_operation="JOIN_ONE_TO_ONE",
                join_type="KEEP_ALL",
                match_option="INTERSECT"
            )
            print("[OK] Segundo join espacial completado")

            # Procesar resultado final
            temp_layer2 = "temp_layer2"
            arcpy.management.MakeFeatureLayer(temp_result2, temp_layer2)
            print("Procesando segunda comparación...")
            self.delete_matching_records(temp_layer2, "TERRENO_CODIGO", "CODIGO_1")
            print("[OK] Segunda comparación completada")

            # Guardar resultado final en la geodatabase de salida
            output_fc = os.path.join(out_gdb, output_name)
            print(f"\nGuardando resultado final en: {output_fc}")
            
            if arcpy.Exists(output_fc):
                arcpy.management.Delete(output_fc)

            arcpy.management.CopyFeatures(temp_layer2, output_fc)
            
            # Agregar y poblar campos de error
            arcpy.management.AddField(output_fc, "Error_Descripcion", "TEXT", field_length=255)
            arcpy.management.AddField(output_fc, "Excepcion_Descripcion", "TEXT", field_length=500)
            
            with arcpy.da.UpdateCursor(output_fc, ["Error_Descripcion"]) as cursor:
                for row in cursor:
                    row[0] = error_msg
                    cursor.updateRow(row)

            print("[OK] Resultado final guardado")

        except Exception as e:
            print(f"Error en process_nomenclatura: {str(e)}")
            raise e
        finally:
            # Limpiar capas temporales solo si existen
            for temp in [temp_layer, temp_layer1, temp_layer2]:
                if temp and arcpy.Exists(temp):
                    arcpy.management.Delete(temp)
            # Limpiar capas en memoria
            for temp in [temp_result, temp_result2]:
                if temp and arcpy.Exists(temp):
                    arcpy.management.Delete(temp)

    def delete_matching_records(self, layer, field1, field2):
        """Elimina registros donde los campos coinciden"""
        try:
            oids_to_delete = []
            preserved_count = 0
            null_count = 0
            empty_count = 0
            zeros_count = 0
            
            print("Analizando registros...")
            with arcpy.da.SearchCursor(layer, ["OID@", field1, field2]) as cursor:
                for row in cursor:
                    oid, val1, val2 = row
                    
                    if self.is_empty_or_invalid(val1) or self.is_empty_or_invalid(val2):
                        preserved_count += 1
                        if val1 is None or val2 is None:
                            null_count += 1
                        elif val1.strip() == '' or val2.strip() == '':
                            empty_count += 1
                        else:
                            zeros_count += 1
                        continue
                    
                    if val1.strip() == val2.strip():
                        oids_to_delete.append(oid)

            print(f"\nEstadísticas de registros:")
            print(f"- Registros preservados (total): {preserved_count}")
            print(f"  * Con valores NULL: {null_count}")
            print(f"  * Con strings vacíos: {empty_count}")
            print(f"  * Con solo ceros: {zeros_count}")
            print(f"- Registros a eliminar: {len(oids_to_delete)}")
            
            if oids_to_delete:
                where_clause = f"OBJECTID IN ({','.join(map(str, oids_to_delete))})"
                arcpy.SelectLayerByAttribute_management(layer, "NEW_SELECTION", where_clause)
                result = int(arcpy.GetCount_management(layer)[0])
                print(f"\nConfirmación de registros seleccionados para eliminar: {result}")
                arcpy.DeleteFeatures_management(layer)
                print("[OK] Registros coincidentes eliminados")  # Cambiado el símbolo ✓ por [OK]
            else:
                print("\nNo se encontraron registros válidos para eliminar")
                
        except Exception as e:
            print(f"Error en delete_matching_records: {str(e)}")
            raise e
        
    def define_intersections(self):
        """Define las intersecciones básicas por dataset"""
        return {
            "RURAL_CTM12": [
                ("RURAL_CTM12", "R_VEREDA_CTM12", "R_SECTOR_CTM12", "SECTOR_CODIGO <> CODIGO_1", "Error: El campo 'SECTOR_CODIGO' de la clase R_VEREDA_CTM12, NO coincide con el campo 'CODIGO_1' de la clase R_SECTOR_CTM12"),
                ("RURAL_CTM12", "R_TERRENO_CTM12", "R_VEREDA_CTM12", "VEREDA_CODIGO <> CODIGO_1", "Error: El campo 'VEREDA_CODIGO' de la clase R_TERRENO_CTM12, NO coincide con el campo 'CODIGO_1' de la clase R_VEREDA_CTM12"),

                ("RURAL_CTM12", "R_CONSTRUCCION_CTM12", "R_TERRENO_CTM12", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase R_CONSTRUCCION_CTM12, NO coincide con el campo 'CODIGO_1' de la clase R_TERRENO_CTM12"),
                ("RURAL_CTM12", "R_UNIDAD_CTM12", "R_CONSTRUCCION_CTM12", "CONSTRUCCION_CODIGO <> CODIGO_1", "Error: El campo 'CONSTRUCCION_CODIGO' de la clase R_UNIDAD_CTM12, NO coincide con el campo 'CODIGO_1' de la clase R_CONSTRUCCION_CTM12"),
                ("RURAL_CTM12", "R_UNIDAD_CTM12", "R_TERRENO_CTM12", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase R_UNIDAD_CTM12, NO coincide con el campo 'CODIGO_1' de la clase R_TERRENO_CTM12"),
                ("RURAL_CTM12", "R_CONSTRUCCION_INFORMAL", "R_TERRENO_INFORMAL", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase R_CONSTRUCCION_INFORMAL, NO coincide con el campo 'CODIGO_1' de la clase R_TERRENO_INFORMAL"),
                ("RURAL_CTM12", "R_UNIDAD_INFORMAL", "R_CONSTRUCCION_INFORMAL", "CONSTRUCCION_CODIGO <> CODIGO_1", "Error: El campo 'CONSTRUCCION_CODIGO' de la clase R_UNIDAD_INFORMAL, NO coincide con el campo 'CODIGO_1' de la clase R_CONSTRUCCION_INFORMAL")
                ],
            "URBANO_CTM12": [
                ("URBANO_CTM12", "U_MANZANA_CTM12", "U_SECTOR_CTM12", "(SUBSTRING (CODIGO,1,9)) <> CODIGO_1", "Error: El campo 'CODIGO' (posicion 1 al 9) de la clase U_MANZANA_CTM12, NO coincide con el campo 'CODIGO_1' de la clase U_SECTOR_CTM12"),
                ("URBANO_CTM12", "U_TERRENO_CTM12", "U_MANZANA_CTM12", "MANZANA_CODIGO <> CODIGO_1", "Error: El campo 'MANZANA_CODIGO' de la clase U_TERRENO_CTM12, NO coincide con el campo 'CODIGO_1' de la clase U_MANZANA_CTM12"),
                ("URBANO_CTM12", "U_CONSTRUCCION_CTM12", "U_TERRENO_CTM12", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase U_CONSTRUCCION_CTM12, NO coincide con el campo 'CODIGO_1' de la clase U_TERRENO_CTM12"),
                ("URBANO_CTM12", "U_UNIDAD_CTM12", "U_CONSTRUCCION_CTM12", "CONSTRUCCION_CODIGO <> CODIGO_1", "Error: El campo 'CONSTRUCCION_CODIGO' de la clase U_UNIDAD_CTM12, NO coincide con el campo 'CODIGO_1' de la clase U_CONSTRUCCION_CTM12"),
                ("URBANO_CTM12", "U_UNIDAD_CTM12", "U_TERRENO_CTM12", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase U_UNIDAD_CTM12, NO coincide con el campo 'CODIGO_1' de la clase U_TERRENO_CTM12"),
                ("URBANO_CTM12", "U_CONSTRUCCION_INFORMAL", "U_TERRENO_INFORMAL", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase U_CONSTRUCCION_INFORMAL, NO coincide con el campo 'CODIGO_1' de la clase U_TERRENO_INFORMAL"),
                ("URBANO_CTM12", "U_UNIDAD_INFORMAL", "U_CONSTRUCCION_INFORMAL", "CONSTRUCCION_CODIGO <> CODIGO_1", "Error: El campo 'CONSTRUCCION_CODIGO' de la clase U_UNIDAD_INFORMAL, NO coincide con el campo 'CODIGO_1' de la clase U_CONSTRUCCION_INFORMAL")
                ],
            "URBANO": [
                ("URBANO", "U_MANZANA", "U_SECTOR", "(SUBSTRING (CODIGO,1,9)) <> CODIGO_1", "Error: El campo 'CODIGO' (posicion 1 al 9) de la clase U_MANZANA, NO coincide con el campo 'CODIGO_1' de la clase U_SECTOR"),
                ("URBANO", "U_TERRENO", "U_MANZANA", "MANZANA_CODIGO <> CODIGO_1", "Error: El campo 'MANZANA_CODIGO ' de la clase U_TERRENO, NO coincide con el campo 'CODIGO_1' de la clase U_MANZANA"),
                ("URBANO", "U_CONSTRUCCION", "U_TERRENO", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase U_CONSTRUCCION, NO coincide con el campo 'CODIGO_1' de la clase U_TERRENO"),
                ("URBANO", "U_NOMENCLATURA_DOMICILIARIA", "U_TERRENO", "TERRENO_CODIGO <> CODIGO", "Error: El campo 'TERRENO_CODIGO' de la clase U_NOMENCLATURA_DOMICILIARIA, NO coincide con el campo 'CODIGO' de la clase U_TERRENO"),
                ("URBANO", "U_UNIDAD", "U_CONSTRUCCION", "CONSTRUCCION_CODIGO <> CODIGO_1", "Error: El campo 'CONSTRUCCION_CODIGO' de la clase U_UNIDAD, NO coincide con el campo 'CODIGO_1' de la clase U_CONSTRUCCION"),
                ("URBANO", "U_UNIDAD", "U_TERRENO", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase U_UNIDAD, NO coincide con el campo 'CODIGO_1' de la clase U_TERRENO")
                
                
                ],
            "RURAL": [
                ("RURAL", "R_VEREDA", "R_SECTOR", "SECTOR_CODIGO <> CODIGO_1", "Error: El campo 'SECTOR_CODIGO' de la clase R_VEREDA, NO coincide con el campo 'CODIGO_1' de la clase R_SECTOR"),
                ("RURAL", "R_TERRENO", "R_VEREDA", "VEREDA_CODIGO <> CODIGO_1", "Error: El campo 'VEREDA_CODIGO' de la clase R_TERRENO, NO coincide con el campo 'CODIGO_1' de la clase R_VEREDA"),
                ("RURAL", "R_CONSTRUCCION", "R_TERRENO", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGOCODIGO' de la clase R_CONSTRUCCION, NO coincide con el campo 'CODIGO_1' de la clase R_TERRENO"),
                ("RURAL", "R_NOMENCLATURA_DOMICILIARIA", "R_TERRENO", "TERRENO_CODIGO <> CODIGO", "Error: El campo 'TERRENO_CODIGO' de la clase R_NOMENCLATURA_DOMICILIARIA, NO coincide con el campo 'CODIGO' de la clase R_TERRENO"),
                ("RURAL", "R_UNIDAD", "R_CONSTRUCCION", "CONSTRUCCION_CODIGO <> CODIGO_1", "Error: El campo 'CONSTRUCCION_CODIGO' de la clase R_UNIDAD, NO coincide con el campo 'CODIGO_1' de la clase R_CONSTRUCCION"),
                ("RURAL", "R_UNIDAD", "R_TERRENO", "TERRENO_CODIGO <> CODIGO_1", "Error: El campo 'TERRENO_CODIGO' de la clase R_UNIDAD, NO coincide con el campo 'CODIGO_1' de la clase R_TERRENO")                      
                
                ]
        }

    def define_special_intersections(self):
        """Define las intersecciones especiales por dataset"""
        return {
            "RURAL_CTM12": [
                ("RURAL_CTM12", "R_TERRENO_INFORMAL", "R_VEREDA_CTM12", "VEREDA_CODIGO <> CODIGO_1", 
                 "Error: El campo 'CODIGO' de la clase R_TERRENO_INFORMAL, NO coincide con el campo 'CODIGO_1' de la clase R_VEREDA_CTM12", "VEREDA_CODIGO")
            ],
            "URBANO_CTM12": [
                ("URBANO_CTM12", "U_TERRENO_INFORMAL", "U_MANZANA_CTM12", "MANZANA_CODIGO <> CODIGO_1", 
                 "Error: El campo 'CODIGO' de la clase U_TERRENO_INFORMAL, NO coincide con el campo 'CODIGO_1' de la clase U_MANZANA_CTM12", "MANZANA_CODIGO")
            ]
        }
    
    def create_excel(self, results):
        """Crea el archivo Excel con los resultados"""
        if results:
            print(f"Creando archivo Excel: {self.output_excel}")
            wb = openpyxl.Workbook()
            wb.remove(wb.active)  # Eliminar la hoja por defecto

            for fc in results:
                try:
                    if not arcpy.Exists(fc):
                        print(f"Advertencia: El feature class {fc} no existe. Saltando...")
                        continue
                    
                    sheet_name = self.truncate_sheet_name(os.path.basename(fc))
                    ws = wb.create_sheet(sheet_name)
                    print(f"Creando hoja: {sheet_name}")

                    # Verificar si el feature class está vacío
                    count = int(arcpy.GetCount_management(fc)[0])
                    if count == 0:
                        print(f"El feature class {fc} está vacío. Creando hoja con solo encabezados.")
                        fields = [f.name for f in arcpy.ListFields(fc) if f.type not in ['Geometry', 'OID']]
                        ws.append(fields)
                        continue

                    fields = [f.name for f in arcpy.ListFields(fc) if f.type not in ['Geometry', 'OID']]
                    ws.append(fields)

                    with arcpy.da.SearchCursor(fc, fields) as cursor:
                        for row in cursor:
                            ws.append(row)

                except Exception as e:
                    print(f"Error al procesar {fc}: {str(e)}")
                    continue  # Continuar con el siguiente feature class en lugar de fallar completamente

            try:
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    wb.save(self.output_excel)
                    if len(w) > 0:
                        print("Se generaron advertencias al guardar el archivo Excel:")
                        for warning in w:
                            print(str(warning.message))
                    else:
                        print("Archivo Excel creado exitosamente sin advertencias.")
            except Exception as e:
                print(f"Error al guardar el archivo Excel: {str(e)}")
                # Intentar guardar en una ubicación alternativa
                alt_path = os.path.join(os.path.dirname(self.output_excel), "resultados_backup.xlsx")
                print(f"Intentando guardar en ubicación alternativa: {alt_path}")
                wb.save(alt_path)
        else:
            print("No se creó el archivo Excel porque no se encontraron resultados.")
    def run_validation(self):
        """Ejecuta todo el proceso de validación"""
        try:
            if not self.datasets_to_process:
                raise ValueError("No hay datasets para procesar. Verifique el archivo de configuración.")

            # Eliminar y recrear la geodatabase temporal si existe
            if arcpy.Exists(self.temp_gdb):
                print(f"Eliminando geodatabase temporal existente: {self.temp_gdb}")
                arcpy.management.Delete(self.temp_gdb)
            
            # Crear geodatabases
            for gdb in [self.temp_gdb, self.output_gdb]:
                if not arcpy.Exists(gdb):
                    arcpy.management.CreateFileGDB(os.path.dirname(gdb), os.path.basename(gdb))
                    print(f"Geodatabase creada: {gdb}")

            # Obtener las intersecciones solo para los datasets seleccionados
            all_intersections = self.define_intersections()
            special_intersections = self.define_special_intersections()
            
            intersections = []
            special_ints = []
            
            # Solo procesar los datasets que vienen del archivo de configuración
            for dataset in self.datasets_to_process:
                if dataset in all_intersections:
                    print(f"\nProcesando intersecciones para dataset: {dataset}")
                    intersections.extend(all_intersections[dataset])
                if dataset in special_intersections:
                    print(f"Procesando intersecciones especiales para dataset: {dataset}")
                    special_ints.extend(special_intersections[dataset])

            if not intersections and not special_ints:
                print("No se encontraron intersecciones para procesar en los datasets seleccionados.")
                return

            # Proceso 1: Crear intersecciones en la geodatabase temporal
            self.process_intersections(intersections, self.input_gdb, self.temp_gdb)
            self.process_special_intersections(special_ints, self.input_gdb, self.temp_gdb)

            # Proceso 2: Aplicar selección y exportar a la geodatabase de salida
            results = self.apply_selection_and_export(intersections + special_ints, self.temp_gdb, self.output_gdb)

            # Proceso 3: Procesar nomenclatura solo para los datasets seleccionados que lo requieran
            for dataset in self.datasets_to_process:
                if dataset in ["RURAL_CTM12", "URBANO_CTM12"]:
                    print(f"\nProcesando nomenclatura para dataset: {dataset}")
                    self.process_nomenclatura(dataset, self.input_gdb, self.output_gdb)
                    if dataset == "RURAL_CTM12":
                        results.append(os.path.join(self.output_gdb, "INTERSECT_R_NOMEN_DOMICILIARIA_CTM12_R_TERRENO_CTM12"))
                    else:
                        results.append(os.path.join(self.output_gdb, "INTERSECT_U_NOMEN_DOMICILIARIA_CTM12_U_TERRENO_CTM12"))

            # Proceso 4: Crear archivo Excel con todos los resultados
            if results:
                self.create_excel(results)
            else:
                print("No se encontraron resultados para exportar a Excel.")

            # Eliminar la geodatabase temporal
            if arcpy.Exists(self.temp_gdb):
                arcpy.management.Delete(self.temp_gdb)
                print("Geodatabase temporal eliminada.")

            print("Procesamiento completado exitosamente.")

        except Exception as e:
            print("Error durante la ejecución:")
            print(str(e))
            import traceback
            print(traceback.format_exc())
            raise

def main():
    """Función principal de ejecución"""
    try:
        # Definir el directorio del proyecto
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
        
        # Crear y ejecutar el validador
        validator = IntersectValidator(proyecto_dir)
        validator.run_validation()
        
    except Exception as e:
        print(f"Error fatal en la ejecución: {str(e)}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
    