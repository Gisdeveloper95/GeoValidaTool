# gdb_validator.py
import arcpy
import os
import pandas as pd
from datetime import datetime
import json
import uuid
import logging
from collections import defaultdict, Counter

 
class DetectorDuplicadosUnidades:
    def __init__(self, input_gdb, output_gdb):
        self.input_gdb = input_gdb
        self.output_gdb = output_gdb
        self.compare_fields = ['CODIGO', 'TIPO_CONSTRUCCION', 'IDENTIFICADOR', 'PLANTA']
        self.area_tolerance = 0.01  # 1% tolerancia para áreas
        self.overlap_threshold = 0.05  # 5% mínimo de superposición
        self.batch_size = 50

    def crear_indice_espacial(self, fc_path):
        try:
            indices_existentes = arcpy.ListIndexes(fc_path)
            tiene_indice = any(idx.name == "SHAPE_IDX" for idx in indices_existentes)
            if not tiene_indice:
                arcpy.AddSpatialIndex_management(fc_path)
        except Exception as e:
            print(f"Error creando índice espacial: {str(e)}")
            raise
            
    def son_areas_similares(self, area1, area2):
        if area1 == 0 or area2 == 0:
            return False
        diferencia = abs(area1 - area2) / max(area1, area2)
        return diferencia <= self.area_tolerance
        
    def crear_fc_temporal(self, template_fc, features):
        temp_name = f"TEMP_{str(uuid.uuid4()).replace('-', '')[:8]}"
        temp_fc = os.path.join("in_memory", temp_name)
        
        try:
            arcpy.CreateFeatureclass_management(
                "in_memory", temp_name, "POLYGON", 
                template=template_fc,
                spatial_reference=arcpy.Describe(template_fc).spatialReference
            )
            
            arcpy.AddField_management(temp_fc, "GROUP_ID", "TEXT", field_length=50)
            arcpy.AddField_management(temp_fc, "ORIG_OID", "LONG")
            arcpy.AddField_management(temp_fc, "ORIG_AREA", "DOUBLE")
            
            with arcpy.da.InsertCursor(temp_fc, ["SHAPE@", "GROUP_ID", "ORIG_OID", "ORIG_AREA"]) as cursor:
                for feat in features:
                    cursor.insertRow([
                        feat["geometry"],
                        feat["group_id"],
                        feat["oid"],
                        feat["area"]
                    ])
                    
            return temp_fc
            
        except Exception as e:
            if arcpy.Exists(temp_fc):
                arcpy.Delete_management(temp_fc)
            raise e
            
    def verificar_superposicion(self, temp_fc):
        overlap_name = f"OVERLAP_{str(uuid.uuid4()).replace('-', '')[:8]}"
        overlap_fc = os.path.join("in_memory", overlap_name)
        
        duplicados = []
        
        try:
            arcpy.Intersect_analysis([temp_fc], overlap_fc, "ALL")
            
            campos = ["GROUP_ID", "ORIG_OID", "ORIG_AREA", "SHAPE@AREA"]
            with arcpy.da.SearchCursor(overlap_fc, campos) as cursor:
                grupos_procesados = set()
                for row in cursor:
                    group_id = row[0]
                    if group_id in grupos_procesados:
                        continue
                        
                    area_overlap = row[3]
                    area_original = row[2]
                    
                    if area_overlap > (area_original * self.overlap_threshold):
                        duplicados.append({
                            'group_id': group_id,
                            'oid': row[1]
                        })
                        grupos_procesados.add(group_id)
                        
            return duplicados
            
        finally:
            if arcpy.Exists(overlap_fc):
                arcpy.Delete_management(overlap_fc)
                
    def procesar_duplicados_fc(self, input_gdb, output_gdb, dataset_name, fc):
        try:
            input_fc = os.path.join(input_gdb, dataset_name, fc)
            output_fc = os.path.join(output_gdb, dataset_name, fc)
            
            fields = [f.name for f in arcpy.ListFields(input_fc)]
            if 'CODIGO' not in fields:
                print(f"{fc} no tiene campo CODIGO, no se procesará")
                return

            compare_fields = ['CODIGO']
            records = {}  # Mover esta definición aquí arriba
            duplicate_groups = {}  # Y esta también
            
            # Proceso especial para unidades con análisis espacial
            if "UNIDAD" in fc.upper():
                unit_fields = ['TIPO_CONSTRUCCION', 'IDENTIFICADOR', 'PLANTA']
                if not all(field in fields for field in unit_fields):
                    print(f"Faltan campos requeridos para unidades en {fc}")
                    return
                compare_fields.extend(unit_fields)
                
                print(f"\nProcesando duplicados en: {fc}")

                # Crear índice espacial si no existe
                indices = arcpy.ListIndexes(input_fc)
                if not any(idx.name == "SHAPE_IDX" for idx in indices):
                    arcpy.AddSpatialIndex_management(input_fc)

                # Verificación de secuencia de pisos (independiente de duplicados)
                print(f"\nVerificando secuencia de pisos en {fc}...")
                if not arcpy.Exists(output_fc):
                    arcpy.CreateFeatureclass_management(
                        os.path.dirname(output_fc),
                        os.path.basename(output_fc),
                        template=input_fc,
                        spatial_reference=arcpy.Describe(input_fc).spatialReference
                    )
                    self.agregar_campos_descripcion(output_fc)
                errores = self.verificar_secuencia_pisos(input_fc, output_fc)
                if errores > 0:
                    print(f"Se encontraron {errores} errores en la secuencia de pisos")

                records = {}
                with arcpy.da.SearchCursor(input_fc, compare_fields + ["SHAPE@AREA", "OID@", "SHAPE@"]) as cursor:
                    for row in cursor:
                        area = round(row[-3], 2)  # Redondear a 2 decimales
                        key = tuple(str(val) if val is not None else 'None' for val in row[:-3]) + (area,)
                        
                        if key in records:
                            records[key].append((row[-2], row[-1]))
                        else:
                            records[key] = [(row[-2], row[-1])]

                duplicate_groups = {k: v for k, v in records.items() if len(v) > 1}
                grupos_encontrados = len(duplicate_groups)
                
                if not duplicate_groups:
                    return

                mem_fc = "in_memory\\temp_duplicates"
                if arcpy.Exists(mem_fc):
                    arcpy.Delete_management(mem_fc)

                arcpy.CreateFeatureclass_management("in_memory", "temp_duplicates", "POLYGON",
                    spatial_reference=arcpy.Describe(input_fc).spatialReference)
                arcpy.AddField_management(mem_fc, "GROUP_ID", "TEXT")
                arcpy.AddField_management(mem_fc, "ORIG_OID", "LONG")

                with arcpy.da.InsertCursor(mem_fc, ["SHAPE@", "GROUP_ID", "ORIG_OID"]) as cursor:
                    for group_id, features in duplicate_groups.items():
                        for oid, geom in features:
                            cursor.insertRow([geom, str(group_id), oid])

                overlap_fc = "in_memory\\overlap"
                if arcpy.Exists(overlap_fc):
                    arcpy.Delete_management(overlap_fc)
                arcpy.Intersect_analysis([mem_fc], overlap_fc)

                duplicados_confirmados = set()
                with arcpy.da.SearchCursor(overlap_fc, ["GROUP_ID", "ORIG_OID", "SHAPE@AREA"]) as cursor:
                    for row in cursor:
                        group_id = eval(row[0])
                        oid = row[1]
                        overlap_area = row[2]
                        
                        for features in records[group_id]:
                            if features[0] == oid:
                                if overlap_area > (features[1].area * 0.05):
                                    duplicados_confirmados.add(oid)
                                break

                if duplicados_confirmados:
                    if not arcpy.Exists(output_fc):
                        arcpy.CreateFeatureclass_management(
                            os.path.dirname(output_fc),
                            os.path.basename(output_fc),
                            template=input_fc,
                            spatial_reference=arcpy.Describe(input_fc).spatialReference
                        )
                        self.agregar_campos_descripcion(output_fc)

                    fields = [f.name for f in arcpy.ListFields(input_fc) 
                            if f.type not in ['OID', 'Geometry'] 
                            and f.name.upper() not in ['SHAPE_LENGTH', 'SHAPE_AREA']]
                    fields.append('SHAPE@')

                    with arcpy.da.InsertCursor(output_fc, fields + ['Error_Descripcion']) as insert_cursor:
                        for oid in duplicados_confirmados:
                            where_clause = f"OBJECTID = {oid}"
                            with arcpy.da.SearchCursor(input_fc, fields, where_clause) as search_cursor:
                                for row in search_cursor:
                                    new_row = list(row) + ["Error: Registro Duplicado"]
                                    insert_cursor.insertRow(new_row)

                    print(f"Se encontraron {len(duplicados_confirmados)} duplicados en {grupos_encontrados} grupos")
                return

            # Proceso normal para otras capas (solo atributos + área)
            if "CONSTRUCCION" in fc.upper():
                construction_fields = ['NUMERO_PISOS', 'IDENTIFICADOR', 'TIPO_CONSTRUCCION']
                if not all(field in fields for field in construction_fields):
                    print(f"Faltan campos requeridos para construcciones en {fc}")
                    return
                compare_fields.extend(construction_fields)

            print(f"\nProcesando duplicados en: {fc}")
                
            # Proceso simple por atributos y área para otras capas
            records = {}
            with arcpy.da.SearchCursor(input_fc, compare_fields + ["SHAPE@AREA", "OID@", "SHAPE@"]) as cursor:
                for row in cursor:
                    area = round(row[-3], 2)
                    key = tuple(str(val) if val is not None else 'None' for val in row[:-3]) + (area,)
                    
                    if key in records:
                        records[key].append((row[-2], row[-1]))
                    else:
                        records[key] = [(row[-2], row[-1])]

            duplicados_encontrados = 0
            grupos_encontrados = 0

            for key, values in records.items():
                if len(values) > 1:
                    grupos_encontrados += 1
                    
                    if not arcpy.Exists(output_fc):
                        arcpy.CreateFeatureclass_management(
                            os.path.dirname(output_fc),
                            os.path.basename(output_fc),
                            template=input_fc,
                            spatial_reference=arcpy.Describe(input_fc).spatialReference
                        )
                        self.agregar_campos_descripcion(output_fc)

                    fields = [f.name for f in arcpy.ListFields(input_fc) 
                            if f.type not in ['OID', 'Geometry'] 
                            and f.name.upper() not in ['SHAPE_LENGTH', 'SHAPE_AREA']]
                    fields.append('SHAPE@')

                    for oid, geom in values:
                        where_clause = f"OBJECTID = {oid}"
                        with arcpy.da.SearchCursor(input_fc, fields, where_clause) as search_cursor:
                            with arcpy.da.InsertCursor(output_fc, fields + ['Error_Descripcion']) as insert_cursor:
                                for row in search_cursor:
                                    new_row = list(row) + ["Error: Registro Duplicado"]
                                    insert_cursor.insertRow(new_row)
                                    duplicados_encontrados += 1

            if duplicados_encontrados > 0:
                print(f"Se encontraron {duplicados_encontrados} duplicados en {grupos_encontrados} grupos")

        except Exception as e:
            print(f"Error procesando {fc}: {str(e)}")
            import traceback
            print(traceback.format_exc())
        finally:
            try:
                arcpy.Delete_management("in_memory")
            except:
                pass           
    def agregar_campos_descripcion(self, fc):
        existing_fields = [f.name for f in arcpy.ListFields(fc)]
        if "Error_Descripcion" not in existing_fields:
            arcpy.AddField_management(fc, "Error_Descripcion", "TEXT", field_length=1000)
        if "Excepcion_Descripcion" not in existing_fields:
            arcpy.AddField_management(fc, "Excepcion_Descripcion", "TEXT", field_length=1000)   
                                   
class GDBValidator:
    def __init__(self, proyecto_dir):
        self.proyecto_dir = proyecto_dir
        self.setup_paths()
        self.load_config()
        
    def crear_nueva_geodatabase(self):
        """Crea una nueva geodatabase para los resultados si no existe"""
        if not arcpy.Exists(self.output_gdb):
            arcpy.CreateFileGDB_management(os.path.dirname(self.output_gdb), 
                                         os.path.basename(self.output_gdb))
            print(f"Geodatabase de validación creada: {self.output_gdb}")

            # Obtener la referencia espacial de la geodatabase original
            arcpy.env.workspace = self.input_gdb
            existing_datasets = arcpy.ListDatasets()

            # Crear los datasets que no existan
            for dataset in self.definir_datasets().keys():
                if dataset in self.datasets_to_process:
                    output_dataset = os.path.join(self.output_gdb, dataset)
                    if not arcpy.Exists(output_dataset):
                        try:
                            # Si existe en la GDB original, usar su referencia espacial
                            if dataset in existing_datasets:
                                spatial_ref = arcpy.Describe(os.path.join(self.input_gdb, dataset)).spatialReference
                            else:
                                # Si no existe, usar la referencia espacial del primer dataset que exista
                                spatial_ref = arcpy.Describe(os.path.join(self.input_gdb, existing_datasets[0])).spatialReference

                            arcpy.CreateFeatureDataset_management(
                                self.output_gdb, 
                                dataset, 
                                spatial_reference=spatial_ref
                            )
                            print(f"Dataset creado: {dataset}")
                        except Exception as e:
                            print(f"No se pudo crear el dataset {dataset}: {str(e)}")
        else:
            print(f"Usando geodatabase de validación existente: {self.output_gdb}")

    def analizar_duplicados_todos_datasets(self, datasets_to_validate, selected_datasets=None):
        """Procesa los datasets seleccionados para encontrar duplicados"""
        print("\nAnalizando duplicados en los datasets...")
        
        if selected_datasets is None:
            selected_datasets = self.datasets_to_process
        
        # Limpiar los nombres de datasets - CORREGIDO
        selected_datasets = [ds.strip('[]" \',').strip() for ds in selected_datasets if ds.strip('[]" \',').strip()]
        
        # Definir los datasets que pueden tener duplicados
        DATASETS_PERMITIDOS_DUPLICADOS = {
            "URBANO",
            "URBANO_CTM12",
            "RURAL",
            "RURAL_CTM12"
        }
        
        datasets_a_procesar = [dataset for dataset in selected_datasets 
                            if dataset in DATASETS_PERMITIDOS_DUPLICADOS]
        
        if not datasets_a_procesar:
            print("No se encontraron datasets aplicables para el análisis de duplicados.")
            return
            
        print("\nAnalizando duplicados en los siguientes datasets:")
        print(", ".join(datasets_a_procesar))
        
        # Guardar el workspace original
        original_workspace = arcpy.env.workspace
        
        try:
            for dataset_name in datasets_a_procesar:
                input_dataset_path = os.path.join(self.input_gdb, dataset_name)
                if not arcpy.Exists(input_dataset_path):
                    print(f"El dataset {dataset_name} no existe en la geodatabase de entrada")
                    continue
                
                arcpy.env.workspace = input_dataset_path
                print(f"\nProcesando dataset: {dataset_name}")
                
                feature_classes = arcpy.ListFeatureClasses()
                
                for fc in feature_classes:
                    # No procesar capas de PERIMETRO ni nomenclatura
                    if "PERIMETRO" in fc.upper() or "NOMEN" in fc.upper():
                        continue
                        
                    output_dataset = os.path.join(self.output_gdb, dataset_name)
                    output_fc = os.path.join(output_dataset, fc)
                    
                    if not arcpy.Exists(output_fc):
                        try:
                            input_fc = os.path.join(input_dataset_path, fc)
                            arcpy.CreateFeatureclass_management(
                                output_dataset,
                                fc,
                                template=input_fc,
                                spatial_reference=arcpy.Describe(input_fc).spatialReference
                            )
                            self.agregar_campos_descripcion(output_fc)
                        except Exception as e:
                            print(f"Error creando feature class {fc}: {str(e)}")
                            continue
                    
                    print(f"\nProcesando duplicados en: {fc}")
                    # CORREGIDO: Pasar input_fc y output_fc directamente
                    input_fc_path = os.path.join(input_dataset_path, fc)
                    self.procesar_duplicados_fc(self.input_gdb, self.output_gdb, dataset_name, fc)
                    
        except Exception as e:
            print(f"Error en el análisis de duplicados: {str(e)}")
            import traceback
            print(traceback.format_exc())
        finally:
            arcpy.env.workspace = original_workspace
            self.limpiar_temp()
        
    def setup_paths(self):
        """Configura las rutas necesarias para el proceso"""
        # Buscar la geodatabase en el directorio temporal
        temp_dir = os.path.join(self.proyecto_dir,  "Files", "Temporary_Files","MODELO_IGAC")
        gdbs = [f for f in os.listdir(temp_dir) if f.endswith('.gdb')]
        if not gdbs:
            raise FileNotFoundError("No se encontró ninguna geodatabase en el directorio temporal")
        
        self.input_gdb = os.path.join(temp_dir, gdbs[0])
        self.output_folder = os.path.join(temp_dir, 'consistencia_formato_temp')
        os.makedirs(self.output_folder, exist_ok=True)
        self.output_gdb = os.path.join(self.output_folder, 
                                     f"{os.path.splitext(os.path.basename(self.input_gdb))[0]}_validacion.gdb")

   
    def obtener_campo_id(self, fc):
        """Obtiene el nombre del campo ID"""
        desc = arcpy.Describe(fc)
        return desc.OIDFieldName

    def agregar_campos_descripcion(self, fc):
        """Agrega los campos de descripción de error y excepción si no existen"""
        existing_fields = [f.name for f in arcpy.ListFields(fc)]
        if "Error_Descripcion" not in existing_fields:
            arcpy.AddField_management(fc, "Error_Descripcion", "TEXT", field_length=1000)
        if "Excepcion_Descripcion" not in existing_fields:
            arcpy.AddField_management(fc, "Excepcion_Descripcion", "TEXT", field_length=1000)

    def actualizar_descripcion_error(self, fc):
        """Actualiza el campo Error_Descripcion con el mensaje de error estándar"""
        with arcpy.da.UpdateCursor(fc, ["Error_Descripcion"]) as cursor:
            for row in cursor:
                if not row[0]:  # Solo actualiza si está vacío
                    row[0] = "Error: Poligono Duplicado"
                    cursor.updateRow(row)

    def eliminar_columnas_duplicadas(self):
        """Elimina las columnas que terminan en _1 de todos los feature classes"""
        print("\nEliminando columnas duplicadas...")
        original_workspace = arcpy.env.workspace
        
        try:
            arcpy.env.workspace = self.output_gdb
            datasets = arcpy.ListDatasets(feature_type='feature')
            
            for dataset in datasets:
                arcpy.env.workspace = os.path.join(self.output_gdb, dataset)
                feature_classes = arcpy.ListFeatureClasses()
                
                for fc in feature_classes:
                    try:
                        campos_a_eliminar = []
                        fields = arcpy.ListFields(fc)
                        
                        for field in fields:
                            if field.name.endswith('_1') or field.name == 'FREQUENCY' or field.name =='POLY_AREA':
                                campos_a_eliminar.append(field.name)
                        
                        if campos_a_eliminar:
                            print(f"Eliminando campos duplicados de {fc}: {', '.join(campos_a_eliminar)}")
                            arcpy.DeleteField_management(fc, campos_a_eliminar)
                    
                    except Exception as e:
                        print(f"Error al procesar campos en {fc}: {str(e)}")
                        continue
        
        except Exception as e:
            print(f"Error al eliminar columnas duplicadas: {str(e)}")
        
        finally:
            arcpy.env.workspace = original_workspace

    def exportar_a_excel(self, excel_output):
        """Exporta los resultados a un archivo Excel con el mismo nombre de la GDB"""
        
        # Obtener el nombre base de la GDB para el archivo Excel
        gdb_name = os.path.splitext(os.path.basename(self.output_gdb))[0]
        excel_output = os.path.join(os.path.dirname(excel_output), f"{gdb_name}.xlsx")
        
        print(f"Exportando resultados a Excel: {excel_output}")
        
        try:
            # Verificar que el directorio de salida existe
            output_dir = os.path.dirname(excel_output)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                
            with pd.ExcelWriter(excel_output, engine='openpyxl') as writer:
                sheets_created = 0
                
                # Verificar que la geodatabase existe
                if not arcpy.Exists(self.output_gdb):
                    print(f"Error: La geodatabase {self.output_gdb} no existe")
                    return
                    
                for dataset in self.datasets_to_process:
                    dataset_path = os.path.join(self.output_gdb, dataset)
                    
                    if not arcpy.Exists(dataset_path):
                        print(f"El dataset {dataset} no existe en la geodatabase")
                        continue
                        
                    arcpy.env.workspace = dataset_path
                    fcs = arcpy.ListFeatureClasses()
                    
                    if not fcs:
                        print(f"No se encontraron feature classes en {dataset}")
                        continue
                    
                    for fc in fcs:
                        try:
                            # Verificar si hay registros
                            count = int(arcpy.GetCount_management(fc)[0])
                            if count == 0:
                                print(f"Omitiendo {fc} (sin datos)")
                                continue
                                
                            print(f"Exportando {fc} a Excel...")
                            
                            # Obtener campos válidos
                            fields = [f.name for f in arcpy.ListFields(fc) 
                                    if f.type not in ['Geometry', 'OID'] 
                                    and f.name.upper() not in ['SHAPE_LENGTH', 'SHAPE_AREA']]
                            
                            if not fields:
                                print(f"No hay campos válidos para exportar en {fc}")
                                continue
                            
                            # Leer datos en lotes
                            data_list = []
                            with arcpy.da.SearchCursor(fc, fields) as cursor:
                                batch = []
                                for i, row in enumerate(cursor, 1):
                                    batch.append(row)
                                    if len(batch) >= 100:  # Procesar en lotes de 100
                                        df_batch = pd.DataFrame(batch, columns=fields)
                                        data_list.append(df_batch)
                                        batch = []
                                        if i % 1000 == 0:
                                            print(f"  Procesados {i} registros...")
                                
                                if batch:  # Procesar el último lote
                                    df_batch = pd.DataFrame(batch, columns=fields)
                                    data_list.append(df_batch)
                            
                            if not data_list:
                                print(f"No se encontraron datos válidos en {fc}")
                                continue
                            
                            # Concatenar todos los lotes
                            df = pd.concat(data_list, ignore_index=True)
                            
                            # Convertir a string y manejar valores nulos
                            for col in df.columns:
                                df[col] = df[col].fillna('')
                                df[col] = df[col].astype(str)
                                df[col] = df[col].apply(lambda x: x[:32767] if len(x) > 32767 else x)
                            
                            # Usar nombre válido para la hoja
                            sheet_name = fc[:31].replace('/', '_').replace('\\', '_')
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            sheets_created += 1
                            
                            print(f"  Exportados {len(df)} registros de {fc}")
                            
                        except Exception as e:
                            print(f"Error al exportar {fc}: {str(e)}")
                            print(f"Tipo de error: {type(e).__name__}")
                            import traceback
                            print(traceback.format_exc())
                            continue
                
                if sheets_created > 0:
                    print(f"\nResultados exportados exitosamente a: {excel_output}")
                    print(f"Total de feature classes exportados: {sheets_created}")
                else:
                    print("\nNo se encontraron datos para exportar a Excel")
                    
        except Exception as e:
            print(f"Error durante la exportación a Excel: {str(e)}")
            print(f"Tipo de error: {type(e).__name__}")
            import traceback
            print(traceback.format_exc())
        finally:
            try:
                arcpy.env.workspace = None
            except:
                pass

    def copiar_registros_duplicados(self, input_fc, output_fc, where_clause):
        """Copia los registros duplicados al feature class de salida"""
        if not arcpy.Exists(output_fc):
            arcpy.CreateFeatureclass_management(
                os.path.dirname(output_fc), 
                os.path.basename(output_fc),
                template=input_fc,
                spatial_reference=arcpy.Describe(input_fc).spatialReference)
            self.agregar_campos_descripcion(output_fc)
        
        temp_layer = arcpy.MakeFeatureLayer_management(input_fc, "temp_layer", where_clause)
        
        fields = [f.name for f in arcpy.ListFields(input_fc) 
                if f.type not in ['OID', 'Geometry'] and f.name.upper() not in ['SHAPE_LENGTH', 'SHAPE_AREA']]
        fields.append('SHAPE@')
        
        with arcpy.da.SearchCursor(temp_layer, fields) as search_cursor:
            with arcpy.da.InsertCursor(output_fc, fields) as insert_cursor:
                for row in search_cursor:
                    insert_cursor.insertRow(row)
        
        self.actualizar_descripcion_error(output_fc)
        
        count = int(arcpy.GetCount_management(temp_layer)[0])
        print(f"- Se copiaron {count} registros duplicados")

    def copiar_registro_duplicado(self, input_fc, output_fc, oid):
        """Copia un registro específico al feature class de salida"""
        if not arcpy.Exists(output_fc):
            arcpy.CreateFeatureclass_management(
                os.path.dirname(output_fc), 
                os.path.basename(output_fc),
                template=input_fc,
                spatial_reference=arcpy.Describe(input_fc).spatialReference)
            self.agregar_campos_descripcion(output_fc)
        
        id_field = self.obtener_campo_id(input_fc)
        where_clause = f"{id_field} = {oid}"
        
        fields = [f.name for f in arcpy.ListFields(input_fc) 
                if f.type not in ['OID', 'Geometry'] and f.name.upper() not in ['SHAPE_LENGTH', 'SHAPE_AREA']]
        fields.append('SHAPE@')
        
        with arcpy.da.SearchCursor(input_fc, fields, where_clause) as search_cursor:
            with arcpy.da.InsertCursor(output_fc, fields) as insert_cursor:
                for row in search_cursor:
                    insert_cursor.insertRow(row)
        
        self.actualizar_descripcion_error(output_fc)
              
    def analizar_duplicados_atributos_area(self, input_fc, output_fc, grupos_duplicados, es_unidad, campos_comparacion):
        """Analiza duplicados por atributos y área, sin considerar ubicación espacial"""
        temp_layer = "in_memory\\temp_duplicados"
        
        try:
            if arcpy.Exists(temp_layer):
                arcpy.Delete_management(temp_layer)
                
            print("Analizando duplicados por atributos y área...")
            
            # Construir where_clause para los grupos
            where_clauses = []
            for grupo in grupos_duplicados:
                conditions = []
                for campo, valor in zip(campos_comparacion, grupo):
                    valor_escaped = str(valor).replace("'", "''") if valor is not None else None
                    if valor_escaped is None:
                        conditions.append(f"{campo} IS NULL")
                    else:
                        conditions.append(f"{campo} = '{valor_escaped}'")
                where_clauses.append(f"({' AND '.join(conditions)})")
            
            where_clause = " OR ".join(where_clauses)
            
            # Crear capa temporal
            temp_layer_all = "in_memory\\temp_all"
            arcpy.MakeFeatureLayer_management(input_fc, temp_layer_all, where_clause)
            
            # Agrupar por atributos y área
            grupos_duplicados = {}
            with arcpy.da.SearchCursor(temp_layer_all, ["OID@", "SHAPE@AREA"] + list(campos_comparacion)) as cursor:
                for row in cursor:
                    oid = row[0]
                    area = round(row[1], 2)  # Redondear a 2 decimales para evitar diferencias por precisión
                    attrs = tuple(row[2:])  
                    
                    # Incluir el área en la clave del grupo
                    grupo_key = attrs + (area,)
                    
                    if grupo_key not in grupos_duplicados:
                        grupos_duplicados[grupo_key] = []
                    grupos_duplicados[grupo_key].append(oid)
            
            # Copiar duplicados al feature class de salida
            total_grupos = len(grupos_duplicados)
            grupos_con_duplicados = 0
            total_duplicados = 0
            
            print(f"\nProcesando {total_grupos} grupos de atributos y áreas únicas...")
            
            for grupo_key, oids in grupos_duplicados.items():
                if len(oids) > 1:  # Si hay más de un registro con los mismos atributos y área
                    grupos_con_duplicados += 1
                    total_duplicados += len(oids) -1
                    
                    for oid in oids:
                        self.copiar_registro_duplicado(temp_layer_all, output_fc, oid)
                    
                    # Mostrar información del grupo duplicado
                    attrs = grupo_key[:-1]  
                    area = grupo_key[-1]   
                    atributos_str = ", ".join([f"{campo}={valor}" for campo, valor in zip(campos_comparacion, attrs)])
                    print(f"Grupo duplicado encontrado: {atributos_str}, Area={area}")
                    print(f"  OIDs: {', '.join(map(str, oids))}")
            
            print(f"\nResumen del análisis:")
            print(f"- Total de grupos analizados: {total_grupos}")
            print(f"- Grupos con duplicados: {grupos_con_duplicados}")
            print(f"- Total de registros duplicados: {total_duplicados}")
            
        except Exception as e:
            print(f"Error en el análisis: {str(e)}")
            raise
        finally:
            print("Limpiando datos temporales...")
            if arcpy.Exists(temp_layer_all):
                arcpy.Delete_management(temp_layer_all)
            self.limpiar_temp()

    def analizar_superposicion(self, input_fc, output_fc, grupos_duplicados, es_unidad, campos_comparacion):
        """Analiza duplicados solo por atributos"""
        try:
            print("Analizando duplicados solo por atributos...")
            
            # Crear feature class de salida si no existe
            if not arcpy.Exists(output_fc):
                arcpy.CreateFeatureclass_management(
                    os.path.dirname(output_fc),
                    os.path.basename(output_fc),
                    template=input_fc,
                    spatial_reference=arcpy.Describe(input_fc).spatialReference)
                self.agregar_campos_descripcion(output_fc)
            
            # Diccionario para almacenar grupos
            grupos = {}
            
            # Leer registros y agrupar por atributos
            campos_cursor = ["OID@", "SHAPE@"] + list(campos_comparacion)
            with arcpy.da.SearchCursor(input_fc, campos_cursor) as cursor:
                for row in cursor:
                    oid = row[0]
                    shape = row[1]
                    attrs = tuple(row[2:])  # Solo los atributos
                    
                    if attrs not in grupos:
                        grupos[attrs] = []
                    grupos[attrs].append((oid, shape))
            
            # Copiar duplicados al feature class de salida
            campos_salida = [f.name for f in arcpy.ListFields(input_fc) 
                            if f.type not in ['OID', 'Geometry']]
            campos_salida.append('SHAPE@')
            
            with arcpy.da.InsertCursor(output_fc, campos_salida) as insert_cursor:
                for attrs, registros in grupos.items():
                    if len(registros) > 1:  # Si hay duplicados
                        # Obtener los datos originales para cada registro duplicado
                        for oid, shape in registros:
                            where_clause = f"OBJECTID = {oid}"
                            with arcpy.da.SearchCursor(input_fc, campos_salida, where_clause) as search_cursor:
                                for original_row in search_cursor:
                                    insert_cursor.insertRow(original_row)
            
            # Actualizar campo de descripción
            with arcpy.da.UpdateCursor(output_fc, ["Error_Descripcion"]) as cursor:
                for row in cursor:
                    if not row[0]:
                        row[0] = "Error: Registro Duplicado"
                        cursor.updateRow(row)
                        
            count = int(arcpy.GetCount_management(output_fc)[0])
            print(f"Se encontraron {count} registros duplicados")
            
        except Exception as e:
            print(f"Error en el análisis: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise

    def procesar_dataset(self, input_gdb, dataset, fc_list, output_gdb, queries):
        """Procesa un dataset específico ejecutando las queries correspondientes"""
        print(f"Procesando dataset: {dataset}")
        
        # Asegurarnos de que estamos trabajando con el path completo
        input_dataset_path = os.path.join(input_gdb, dataset)
        
        # Verificar si el dataset tiene queries definidas
        if dataset not in queries:
            print(f"No hay queries definidas para el dataset {dataset}")
            return
                
        for fc in fc_list:
            # Obtener path completo del feature class
            input_fc_path = os.path.join(input_dataset_path, fc)
            
            # Verificar si el feature class existe y tiene registros
            if arcpy.Exists(input_fc_path):
                try:
                    # Verificar que realmente tengamos acceso al feature class
                    count = int(arcpy.GetCount_management(input_fc_path)[0])
                    print(f"\nFeature class {fc} tiene {count} registros")
                    
                    # Verificar si hay queries para este feature class
                    if fc in queries[dataset]:
                        print(f"\nEjecutando queries para: {fc}")
                        self.procesar_feature_class(fc, dataset, queries[dataset][fc], input_gdb, output_gdb)
                    else:
                        print(f"No hay queries definidas para {fc}")
                except Exception as e:
                    print(f"Error al procesar feature class {fc}: {str(e)}")
                    import traceback
                    print(traceback.format_exc())
                    continue
            else:
                print(f"El feature class {fc} no existe en {dataset}")

    def procesar_feature_class(self, fc, dataset, queries, input_gdb, output_gdb):
        """Procesa los duplicados para un feature class específico"""
        try:
            input_fc = os.path.join(input_gdb, dataset, fc)
            output_fc = os.path.join(output_gdb, dataset, fc)
            
            print(f"\nProcesando queries para: {fc}")
            
            # Crear el feature class de salida SI NO EXISTE
            if not arcpy.Exists(output_fc):
                try:
                    # Asegurarse que el dataset existe
                    if not arcpy.Exists(os.path.dirname(output_fc)):
                        print(f"Creando dataset: {dataset}")
                        spatial_reference = arcpy.Describe(input_fc).spatialReference
                        arcpy.CreateFeatureDataset_management(output_gdb, dataset, spatial_reference)
                    
                    print(f"Creando feature class: {fc}")
                    arcpy.CreateFeatureclass_management(
                        os.path.dirname(output_fc),
                        os.path.basename(output_fc),
                        template=input_fc,
                        spatial_reference=arcpy.Describe(input_fc).spatialReference
                    )
                    
                    # Agregar campos de descripción
                    self.agregar_campos_descripcion(output_fc)
                    
                except Exception as e:
                    print(f"Error creando feature class de salida: {str(e)}")
                    raise
            
            # Obtener lista de campos excluyendo los de sistema
            all_fields = [f.name for f in arcpy.ListFields(input_fc) 
                        if f.type not in ['OID', 'GlobalID'] and 
                        f.name.upper() not in ['SHAPE', 'SHAPE.AREA', 'SHAPE.LEN', 'SHAPE_LENGTH', 'SHAPE_AREA']]
            
            # Campos para cursor de búsqueda
            search_fields = ['OID@'] + all_fields + ['SHAPE@']
            
            # Diccionario para almacenar registros con errores
            records_to_copy = {}
            
            # Procesar cada query
            for query_index, (query, error_desc) in enumerate(queries):
                try:
                    temp_layer_name = f"temp_layer_{fc}_{query_index}_{uuid.uuid4().hex[:8]}"
                    temp_layer = arcpy.MakeFeatureLayer_management(input_fc, temp_layer_name)
                    
                    arcpy.SelectLayerByAttribute_management(temp_layer, "NEW_SELECTION", query)
                    count = int(arcpy.GetCount_management(temp_layer)[0])
                    
                    if count > 0:
                        print(f"  {error_desc}")
                        print(f"  - Registros encontrados: {count}")
                        
                        with arcpy.da.SearchCursor(temp_layer, search_fields) as cursor:
                            for row in cursor:
                                oid = row[0]
                                geometry = row[-1]
                                attributes = list(row[1:-1])
                                
                                if oid in records_to_copy:
                                    if error_desc not in records_to_copy[oid]['errors']:
                                        records_to_copy[oid]['errors'].append(error_desc)
                                else:
                                    records_to_copy[oid] = {
                                        'attributes': attributes,
                                        'geometry': geometry,
                                        'errors': [error_desc]
                                    }
                    
                    arcpy.Delete_management(temp_layer)
                    
                except Exception as e:
                    print(f"  Error en query: {str(e)}")
                    continue
            
            # Verificar que tenemos registros para copiar
            if records_to_copy:
                # Obtener campos de salida
                insert_fields = all_fields + ['Error_Descripcion', 'SHAPE@']
                
                # Verificar que el feature class de salida existe y tiene los campos correctos
                if not arcpy.Exists(output_fc):
                    raise Exception(f"Feature class de salida no existe: {output_fc}")
                
                output_fields = [f.name for f in arcpy.ListFields(output_fc)]
                if 'Error_Descripcion' not in output_fields:
                    arcpy.AddField_management(output_fc, "Error_Descripcion", "TEXT", field_length=1000)
                
                registros_insertados = 0
                
                try:
                    with arcpy.da.InsertCursor(output_fc, insert_fields) as insert_cursor:
                        for oid, record_info in records_to_copy.items():
                            try:
                                if record_info['geometry'] is None or record_info['geometry'].area == 0:
                                    continue
                                    
                                error_desc = "; ".join(record_info['errors'])
                                new_row = record_info['attributes'] + [error_desc, record_info['geometry']]
                                insert_cursor.insertRow(new_row)
                                registros_insertados += 1
                                
                            except Exception as e:
                                print(f"  Error insertando registro: {str(e)}")
                                continue
                    
                    print(f"\nResumen de {fc}:")
                    print(f"  Registros con errores: {len(records_to_copy)}")
                    print(f"  Registros procesados: {registros_insertados}")
                    
                except Exception as e:
                    print(f"Error en inserción de registros: {str(e)}")
                    raise
                
        except Exception as e:
            print(f"\nError procesando {fc}: {str(e)}")
            import traceback
            print(traceback.format_exc())
        finally:
            arcpy.Delete_management("in_memory")
                
    def eliminar_columnas_auxiliares(self):
        """Elimina las columnas auxiliares de todos los feature classes"""
        print("\nEliminando columnas auxiliares...")
        original_workspace = arcpy.env.workspace
        
        try:
            arcpy.env.workspace = self.output_gdb
            datasets = arcpy.ListDatasets(feature_type='feature')
            
            for dataset in datasets:
                arcpy.env.workspace = os.path.join(self.output_gdb, dataset)
                feature_classes = arcpy.ListFeatureClasses()
                
                for fc in feature_classes:
                    try:
                        campos_a_eliminar = []
                        fields = arcpy.ListFields(fc)
                        
                        for field in fields:
                            # Lista de campos auxiliares a eliminar
                            if (field.name.endswith('_1') or 
                                field.name == 'FREQUENCY' or 
                                field.name == 'POLY_AREA' or
                                field.name == 'TEMP_ID'):  # Agregamos TEMP_ID a la lista
                                campos_a_eliminar.append(field.name)
                        
                        if campos_a_eliminar:
                            print(f"Eliminando campos auxiliares de {fc}: {', '.join(campos_a_eliminar)}")
                            arcpy.DeleteField_management(fc, campos_a_eliminar)
                    
                    except Exception as e:
                        print(f"Error al procesar campos en {fc}: {str(e)}")
                        continue
        
        except Exception as e:
            print(f"Error al eliminar columnas auxiliares: {str(e)}")
        
        finally:
            arcpy.env.workspace = original_workspace
    
    def eliminar_temp_id_features(self):
        """Elimina la columna TEMP_ID de todos los feature classes"""
        original_workspace = arcpy.env.workspace
        
        try:
            for dataset in self.datasets_to_process:
                dataset_path = os.path.join(self.output_gdb, dataset)
                if not arcpy.Exists(dataset_path):
                    continue
                
                print(f"\nProcesando dataset: {dataset}")
                arcpy.env.workspace = dataset_path
                feature_classes = arcpy.ListFeatureClasses()
                
                for fc in feature_classes:
                    try:
                        fc_path = os.path.join(dataset_path, fc)
                        # Verificar si existe la columna TEMP_ID
                        field_list = [field.name for field in arcpy.ListFields(fc_path)]
                        if 'TEMP_ID' in field_list:
                            print(f"- Eliminando TEMP_ID de {fc}")
                            # Asegurarse de que no haya bloqueos
                            arcpy.ClearWorkspaceCache_management()
                            # Intentar eliminar el campo
                            arcpy.DeleteField_management(fc_path, ['TEMP_ID'])
                            print(f"  ✓ TEMP_ID eliminado exitosamente")
                            
                    except Exception as e:
                        print(f"Error al eliminar TEMP_ID de {fc}: {str(e)}")
                        import traceback
                        print(traceback.format_exc())
                        continue
                        
        except Exception as e:
            print(f"Error en la eliminación de columnas TEMP_ID: {str(e)}")
            import traceback
            print(traceback.format_exc())
            
        finally:
            arcpy.env.workspace = original_workspace    

    def limpiar_temp(self):
        """Limpia workspace in_memory y capas temporales"""
        try:
            # Limpiar workspace in_memory
            arcpy.Delete_management("in_memory")
            
            # Listar y eliminar todas las capas temporales
            for item in arcpy.ListWorkspaces("*", "in_memory"):
                arcpy.Delete_management(item)
                
            # Limpiar el workspace actual
            arcpy.ClearWorkspaceCache_management()
            
        except:
            pass

    def verificar_eliminacion_temp_id(self):
        """Verifica que la columna TEMP_ID haya sido eliminada de todos los feature classes"""
        for dataset in self.datasets_to_process:
            dataset_path = os.path.join(self.output_gdb, dataset)
            if not arcpy.Exists(dataset_path):
                continue
                
            arcpy.env.workspace = dataset_path
            feature_classes = arcpy.ListFeatureClasses()
            
            for fc in feature_classes:
                fc_path = os.path.join(dataset_path, fc)
                field_list = [field.name for field in arcpy.ListFields(fc_path)]
                if 'TEMP_ID' in field_list:
                    print(f"⚠️ ADVERTENCIA: TEMP_ID aún existe en {dataset}/{fc}")
                    try:
                        print(f"Intentando eliminar TEMP_ID de {fc}...")
                        arcpy.DeleteField_management(fc_path, ['TEMP_ID'])
                        print(f"TEMP_ID eliminado exitosamente de {fc}")
                    except Exception as e:
                        print(f"Error al eliminar TEMP_ID de {fc}: {str(e)}")
                               
    def run_validation(self):
        """Ejecuta el proceso completo de validación"""
        try:
            print("Iniciando proceso de validación...")
            print(f"Datasets seleccionados para procesar: {', '.join(self.datasets_to_process)}")
            
            # Guardar workspace original
            original_workspace = arcpy.env.workspace
            
            # Crear nueva geodatabase
            self.crear_nueva_geodatabase()
            
            # Filtrar datasets según la selección
            datasets_to_validate = {k: v for k, v in self.definir_datasets().items() 
                                if k in self.datasets_to_process}
            
            if not datasets_to_validate:
                print("No se encontraron datasets válidos para procesar según la selección.")
                return
            
            # Primera fase: Validación de formato (queries)
            queries = self.definir_queries()
            for dataset, fc_list in datasets_to_validate.items():
                print(f"\nProcesando queries para dataset: {dataset}")
                self.procesar_dataset(self.input_gdb, dataset, fc_list, self.output_gdb, queries)
            
            # Segunda fase: Análisis de duplicados
            print("\nIniciando análisis de duplicados...")
            self.analizar_duplicados_todos_datasets(datasets_to_validate)

            # Eliminar la columna TEMP_ID y otras columnas en un solo paso
            print("\nEliminando columnas temporales...")
            arcpy.env.workspace = self.output_gdb
            for dataset in arcpy.ListDatasets():
                for fc in arcpy.ListFeatureClasses(feature_dataset=dataset):
                    try:
                        fc_path = os.path.join(self.output_gdb, dataset, fc)
                        # Eliminar todas las columnas auxiliares de una vez
                        fields_to_delete = []
                        existing_fields = [f.name for f in arcpy.ListFields(fc_path)]
                        if 'TEMP_ID' in existing_fields:
                            fields_to_delete.append('TEMP_ID')
                        if 'FREQUENCY' in existing_fields:
                            fields_to_delete.append('FREQUENCY')
                        if 'POLY_AREA' in existing_fields:
                            fields_to_delete.append('POLY_AREA')
                        # Agregar campos que terminan en _1
                        fields_to_delete.extend([f for f in existing_fields if f.endswith('_1')])
                        
                        if fields_to_delete:
                            arcpy.DeleteField_management(fc_path, fields_to_delete)
                    except:
                        continue
            
            # Restaurar workspace original
            arcpy.env.workspace = original_workspace
            

            # Continuar con la exportación a Excel...
            # Exportar a Excel
            excel_output = os.path.join(self.output_folder, 
                                    f"{os.path.splitext(os.path.basename(self.input_gdb))[0]}_validacion.xlsx")
            self.exportar_a_excel(excel_output)
            
            print("Proceso de validación completado.")
            
        except Exception as e:
            print("Error durante el proceso de validación:")
            print(str(e))
            import traceback
            print(traceback.format_exc())
        finally:
            self.limpiar_temp()
    
    def load_config(self):
        """Carga la configuración de datasets desde el archivo txt"""
        try:
            config_path = os.path.join(self.proyecto_dir, "Files", "Temporary_Files", "array_config.txt")
            
            with open(config_path, 'r') as f:
                content = f.read().strip()
                
                # Intentar evaluar el contenido como una lista de Python
                try:
                    datasets_list = eval(content)
                    if isinstance(datasets_list, list):
                        active_datasets = [ds.strip() for ds in datasets_list if isinstance(ds, str) and not ds.startswith('#')]
                    else:
                        raise ValueError("El contenido no es una lista válida")
                except:
                    # Si falla la evaluación, procesar línea por línea
                    active_datasets = []
                    for line in content.split('\n'):
                        line = line.strip('[]"\', \n').strip()
                        if line and not line.startswith('#'):
                            active_datasets.append(line)

            if not active_datasets:
                raise ValueError("No se encontraron datasets activos en el archivo de configuración")

            # Eliminar duplicados y valores vacíos
            active_datasets = list(filter(None, dict.fromkeys(active_datasets)))
            
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

    def verificar_secuencia_pisos(self, input_fc, output_fc):
        """
        Verifica secuencia de pisos con validación adicional para casos especiales (ramadas)
        """
        import re
        try:
            print("Analizando secuencia de pisos...")
            
            # Primera fase: Detectar potenciales errores (código existente)
            errores_potenciales = []
            grupos_espaciales = defaultdict(list)
            
            # Recolectar unidades y hacer primera validación
            with arcpy.da.SearchCursor(input_fc, ['CODIGO', 'PLANTA', 'SHAPE@', 'OID@']) as cursor:
                for row in cursor:
                    try:
                        codigo_base = row[0][:24] if row[0] else ''
                        planta = str(row[1]) if row[1] else ''
                        
                        match = re.search(r'(\d+)', planta)
                        if match and codigo_base:
                            num_piso = int(match.group(1))
                            geometry = row[2]
                            
                            grupos_espaciales[codigo_base].append({
                                'oid': row[3],
                                'piso': num_piso,
                                'geometry': geometry,
                                'area': geometry.area,
                                'planta': planta,
                                'codigo': row[0]
                            })
                    except Exception as e:
                        print(f"Error procesando registro: {str(e)}")
                        continue

            # Primera validación (superposición 20%)
            for codigo_base, unidades in grupos_espaciales.items():
                if len(unidades) >= 1:
                    subgrupos = []
                    unidades_procesadas = set()
                    
                    for i, unidad in enumerate(unidades):
                        if unidad['oid'] in unidades_procesadas:
                            continue
                            
                        subgrupo_actual = [unidad]
                        unidades_procesadas.add(unidad['oid'])
                        
                        for otra_unidad in unidades[i+1:]:
                            if otra_unidad['oid'] not in unidades_procesadas:
                                try:
                                    interseccion = unidad['geometry'].intersect(otra_unidad['geometry'], 4)
                                    area_interseccion = interseccion.area
                                    area_menor = min(unidad['area'], otra_unidad['area'])
                                    porcentaje_superposicion = (area_interseccion / area_menor) * 100
                                    
                                    if porcentaje_superposicion > 20:
                                        subgrupo_actual.append(otra_unidad)
                                        unidades_procesadas.add(otra_unidad['oid'])
                                except:
                                    continue
                        
                        if subgrupo_actual:
                            subgrupos.append(subgrupo_actual)
                    
                    for subgrupo in subgrupos:
                        pisos_disponibles = set(u['piso'] for u in subgrupo)
                        if pisos_disponibles:
                            piso_max = max(pisos_disponibles)
                            if piso_max > 1:
                                pisos_requeridos = set(range(1, piso_max + 1))
                                pisos_faltantes = pisos_requeridos - pisos_disponibles
                                
                                if pisos_faltantes:
                                    for unidad in subgrupo:
                                        if unidad['piso'] > 1:
                                            errores_potenciales.append({
                                                'oid': unidad['oid'],
                                                'piso': unidad['piso'],
                                                'geometry': unidad['geometry'],
                                                'codigo_base': codigo_base,
                                                'pisos_faltantes': sorted(pisos_faltantes)
                                            })

            # Segunda fase: Validar casos especiales (ramadas)
            errores_confirmados = []
            
            # Crear capa temporal para búsqueda espacial con nombre único
            temp_layer = f"in_memory\\temp_layer_{str(uuid.uuid4()).replace('-','')}"
            if arcpy.Exists(temp_layer):
                arcpy.Delete_management(temp_layer)
            arcpy.MakeFeatureLayer_management(input_fc, temp_layer)
            
            for error in errores_potenciales:
                # Crear buffer de 1 metro para el polígono
                buffer_geom = error['geometry'].buffer(1)
                
                # Buscar polígonos que tocan el buffer
                pisos_vecinos = set()
                try:
                    # Seleccionar por localización
                    arcpy.SelectLayerByLocation_management(
                        temp_layer,
                        "INTERSECT",
                        buffer_geom,
                        "#",
                        "NEW_SELECTION"
                    )
                    
                    # Filtrar por código base
                    where_clause = f"SUBSTRING(CODIGO, 1, 24) = '{error['codigo_base']}' AND OBJECTID <> {error['oid']}"
                    arcpy.SelectLayerByAttribute_management(
                        temp_layer,
                        "SUBSET_SELECTION",
                        where_clause
                    )
                    
                    # Recolectar pisos de vecinos
                    with arcpy.da.SearchCursor(temp_layer, ['PLANTA']) as cursor:
                        for row in cursor:
                            planta = str(row[0]) if row[0] else ''
                            match = re.search(r'(\d+)', planta)
                            if match:
                                pisos_vecinos.add(int(match.group(1)))
                    
                    # Verificar si los vecinos tienen la secuencia completa
                    if pisos_vecinos:
                        pisos_requeridos = set(range(1, error['piso'] + 1))
                        todos_los_pisos = pisos_vecinos | set([error['piso']])
                        
                        # Si entre este polígono y sus vecinos tienen todos los pisos necesarios,
                        # entonces NO es un error (es parte de un conjunto válido)
                        if pisos_requeridos.issubset(todos_los_pisos):
                            continue
                            
                        # Si faltan pisos incluso considerando los vecinos
                        pisos_faltantes = pisos_requeridos - todos_los_pisos
                        if pisos_faltantes:
                            # Verificar si hay suficiente contacto con los vecinos
                            try:
                                interseccion = error['geometry'].buffer(1).intersect(buffer_geom, 4)
                                longitud_contacto = interseccion.length
                                longitud_minima = min(error['geometry'].length, buffer_geom.length)
                                
                                # Si hay suficiente contacto con los vecinos (>20% del perímetro), no es error
                                if longitud_contacto > (longitud_minima * 0.2):
                                    continue
                                    
                                errores_confirmados.append({
                                    'oid': error['oid'],
                                    'pisos_faltantes': sorted(pisos_faltantes)
                                })
                            except Exception as e:
                                print(f"Error calculando contacto: {str(e)}")
                                continue
                    else:
                        # Si no hay vecinos con el mismo código base, mantener como error
                        errores_confirmados.append({
                            'oid': error['oid'],
                            'pisos_faltantes': error['pisos_faltantes']
                        })
                        
                except Exception as e:
                    print(f"Error en validación de vecinos: {str(e)}")
                    continue

            # Exportar errores confirmados
            if errores_confirmados:
                print(f"\nSe encontraron {len(errores_confirmados)} errores confirmados en la secuencia de pisos")
                
                if not arcpy.Exists(output_fc):
                    arcpy.CreateFeatureclass_management(
                        os.path.dirname(output_fc),
                        os.path.basename(output_fc),
                        template=input_fc,
                        spatial_reference=arcpy.Describe(input_fc).spatialReference
                    )
                    self.agregar_campos_descripcion(output_fc)

                campos = [f.name for f in arcpy.ListFields(input_fc) 
                        if f.type not in ['OID', 'Geometry'] 
                        and f.name not in ['SHAPE_Length', 'SHAPE_Area']]
                campos.append('SHAPE@')
                
                errores_procesados = set()
                with arcpy.da.InsertCursor(output_fc, campos + ['Error_Descripcion']) as insert_cursor:
                    for error in errores_confirmados:
                        if error['oid'] not in errores_procesados:
                            where_clause = f"OBJECTID = {error['oid']}"
                            with arcpy.da.SearchCursor(input_fc, campos, where_clause) as search_cursor:
                                for row in search_cursor:
                                    pisos_faltantes = sorted(error['pisos_faltantes'])  # Asegurar orden ascendente
                                    pisos_str = ','.join(map(str, pisos_faltantes))
                                    error_msg = f"Error: Faltan Capas de Unidad con los pisos {pisos_str}"
                                    new_row = list(row) + [error_msg]
                                    insert_cursor.insertRow(new_row)
                                    errores_procesados.add(error['oid'])

                return len(errores_confirmados)
            
            return 0

        except Exception as e:
            print(f"Error validando secuencia de pisos: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return 0
        finally:
            # Limpiar recursos
            try:
                # Limpiar todo el workspace in_memory
                arcpy.Delete_management("in_memory")
                
                # Limpiar cualquier selección activa
                arcpy.ClearSelectionTool_management()
                
                # Asegurarse que todas las capas temporales se eliminan
                for desc in arcpy.Describe("in_memory").children:
                    try:
                        arcpy.Delete_management(os.path.join("in_memory", desc.name))
                    except:
                        continue
                        
            except:
                pass
                
            # Restaurar el entorno de trabajo
            try:
                arcpy.ClearWorkspaceCache_management()
            except:
                pass     
                          
    def procesar_duplicados_construcciones(self, input_fc, output_fc, dataset_name):
        """
        Procesa duplicados para construcciones.
        Solo marca como duplicados los que realmente se superponen dentro de cada grupo.
        """
        try:
            print(f"\nProcesando duplicados en construcción: {os.path.basename(input_fc)}")
            
            # Paso 1: Agrupar por atributos y área similar (esto ya lo teníamos bien)
            grupos_potenciales = {}
            campos_comparacion = ['CODIGO', 'NUMERO_PISOS', 'IDENTIFICADOR', 'TIPO_CONSTRUCCION']
            
            with arcpy.da.SearchCursor(input_fc, campos_comparacion + ["SHAPE@", "SHAPE@AREA", "OID@"]) as cursor:
                for row in cursor:
                    atributos = tuple(str(val) if val is not None else 'None' for val in row[:-3])
                    area = round(row[-2], 2)
                    clave_grupo = atributos + (area,)
                    
                    if clave_grupo not in grupos_potenciales:
                        grupos_potenciales[clave_grupo] = []
                    grupos_potenciales[clave_grupo].append({
                        'oid': row[-1],
                        'geom': row[-3]
                    })
            
            # Filtrar grupos con múltiples registros
            grupos_potenciales = {k: v for k, v in grupos_potenciales.items() if len(v) > 1}
            print(f"Se encontraron {len(grupos_potenciales)} grupos potenciales de duplicados")
            
            # Paso 2: Verificar superposición real SOLO dentro de cada grupo
            duplicados_confirmados = set()
            
            for grupo in grupos_potenciales.values():
                for i, feat1 in enumerate(grupo):
                    for j, feat2 in enumerate(grupo[i+1:], i+1):
                        try:
                            intersection = feat1['geom'].intersect(feat2['geom'], 4)
                            min_area = min(feat1['geom'].area, feat2['geom'].area)
                            
                            if intersection.area > (min_area * 0.10):  # 10% de superposición
                                duplicados_confirmados.add(feat1['oid'])
                                duplicados_confirmados.add(feat2['oid'])
                        except:
                            continue
            
            # Paso 3: Exportar solo los duplicados reales (los que se superponen)
            if duplicados_confirmados:
                print(f"\nExportando {len(duplicados_confirmados)} duplicados confirmados...")
                
                if not arcpy.Exists(output_fc):
                    arcpy.CreateFeatureclass_management(
                        os.path.dirname(output_fc),
                        os.path.basename(output_fc),
                        template=input_fc,
                        spatial_reference=arcpy.Describe(input_fc).spatialReference
                    )
                    self.agregar_campos_descripcion(output_fc)
                
                fields = [f.name for f in arcpy.ListFields(input_fc) 
                        if f.type not in ['OID', 'Geometry'] 
                        and f.name.upper() not in ['SHAPE_LENGTH', 'SHAPE_AREA']]
                fields.append('SHAPE@')
                
                where_clause = f"OBJECTID IN ({','.join(map(str, duplicados_confirmados))})"
                with arcpy.da.SearchCursor(input_fc, fields, where_clause) as search_cursor:
                    with arcpy.da.InsertCursor(output_fc, fields + ['Error_Descripcion']) as insert_cursor:
                        for row in search_cursor:
                            new_row = list(row) + ["Error: Registro Duplicado"]
                            insert_cursor.insertRow(new_row)
                
                print("Exportación completada")
            else:
                print("\nNo se encontraron duplicados confirmados")
                
        except Exception as e:
            print(f"Error procesando duplicados de construcciones: {str(e)}")
            import traceback
            print(traceback.format_exc())
        finally:
            try:
                arcpy.Delete_management("in_memory")
            except:
                pass

    def procesar_duplicados_fc(self, input_gdb, output_gdb, dataset_name, fc):
        """Procesa los duplicados para un feature class específico"""
        try:
            input_fc = os.path.join(input_gdb, dataset_name, fc)
            output_fc = os.path.join(output_gdb, dataset_name, fc)
            
            fields = [f.name for f in arcpy.ListFields(input_fc)]
            if 'CODIGO' not in fields:
                print(f"{fc} no tiene campo CODIGO, no se procesará")
                return

            # Definir campos base de comparación
            compare_fields = ['CODIGO']

            # Proceso especial para unidades con análisis espacial
            if "UNIDAD" in fc.upper():
                unit_fields = ['TIPO_CONSTRUCCION', 'IDENTIFICADOR', 'PLANTA']
                if not all(field in fields for field in unit_fields):
                    print(f"Faltan campos requeridos para unidades en {fc}")
                    return
                compare_fields.extend(unit_fields)
                
                print(f"\nProcesando duplicados en: {fc}")

                # Crear índice espacial si no existe
                indices = arcpy.ListIndexes(input_fc)
                if not any(idx.name == "SHAPE_IDX" for idx in indices):
                    arcpy.AddSpatialIndex_management(input_fc)

                # Verificación de secuencia de pisos (independiente de duplicados)
                print(f"\nVerificando secuencia de pisos en {fc}...")
                if not arcpy.Exists(output_fc):
                    arcpy.CreateFeatureclass_management(
                        os.path.dirname(output_fc),
                        os.path.basename(output_fc),
                        template=input_fc,
                        spatial_reference=arcpy.Describe(input_fc).spatialReference
                    )
                    self.agregar_campos_descripcion(output_fc)
                    
                errores = self.verificar_secuencia_pisos(input_fc, output_fc)
                if errores > 0:
                    print(f"Se encontraron {errores} errores en la secuencia de pisos")

                # Primera fase: Detectar grupos potenciales de duplicados
                grupos_potenciales = {}
                campos_busqueda = compare_fields + ['SHAPE@AREA', 'OID@', 'SHAPE@']

                with arcpy.da.SearchCursor(input_fc, campos_busqueda) as cursor:
                    for row in cursor:
                        atributos = tuple(str(val).strip() if val is not None else 'None' for val in row[:-3])
                        area = round(row[-3], 2)
                        clave_grupo = atributos + (area,)
                        
                        if clave_grupo not in grupos_potenciales:
                            grupos_potenciales[clave_grupo] = []
                        grupos_potenciales[clave_grupo].append({
                            'oid': row[-2],
                            'geometry': row[-1],
                            'attributes': atributos
                        })

                # Filtrar grupos con múltiples registros
                grupos_potenciales = {k: v for k, v in grupos_potenciales.items() if len(v) > 1}

                # Segunda fase: Verificación espacial y de atributos estricta
                duplicados_verificados = []
                
                for grupo in grupos_potenciales.values():
                    # Verificar cada par en el grupo
                    for i, feat1 in enumerate(grupo):
                        for feat2 in grupo[i+1:]:
                            # Verificar que los atributos sean exactamente iguales
                            if feat1['attributes'] != feat2['attributes']:
                                continue
                                
                            # Verificar superposición geométrica
                            try:
                                intersection = feat1['geometry'].intersect(feat2['geometry'], 4)
                                union = feat1['geometry'].union(feat2['geometry'])
                                
                                if union.area > 0:
                                    overlap_ratio = intersection.area / union.area
                                    if overlap_ratio > 0.95:  # 95% de superposición
                                        duplicados_verificados.append({
                                            'oid': feat2['oid'],
                                            'original_oid': feat1['oid'],
                                            'geometry': feat2['geometry']
                                        })
                            except Exception as e:
                                print(f"Error en verificación geométrica: {str(e)}")
                                continue

                # Tercera fase: Exportar duplicados verificados
                if duplicados_verificados:
                    if not arcpy.Exists(output_fc):
                        arcpy.CreateFeatureclass_management(
                            os.path.dirname(output_fc),
                            os.path.basename(output_fc),
                            template=input_fc,
                            spatial_reference=arcpy.Describe(input_fc).spatialReference
                        )
                        self.agregar_campos_descripcion(output_fc)

                    # Copiar duplicados verificados
                    campos = [f.name for f in arcpy.ListFields(input_fc) 
                            if f.type not in ['OID', 'Geometry'] 
                            and f.name.upper() not in ['SHAPE_LENGTH', 'SHAPE_AREA']]
                    campos.append('SHAPE@')

                    with arcpy.da.InsertCursor(output_fc, campos + ['Error_Descripcion']) as insert_cursor:
                        for duplicado in duplicados_verificados:
                            where_clause = f"OBJECTID = {duplicado['oid']}"
                            with arcpy.da.SearchCursor(input_fc, campos, where_clause) as search_cursor:
                                for row in search_cursor:
                                    new_row = list(row) + ["Error: Registro Duplicado"]
                                    insert_cursor.insertRow(new_row)

                    print(f"Se encontraron {len(duplicados_verificados)} duplicados verificados")
            
            if "CONSTRUCCION" in fc.upper():
                construction_fields = ['NUMERO_PISOS', 'IDENTIFICADOR', 'TIPO_CONSTRUCCION']
                if not all(field in fields for field in construction_fields):
                    print(f"Faltan campos requeridos para construcciones en {fc}")
                    return
                compare_fields.extend(construction_fields)

                print(f"\nProcesando duplicados en: {fc}")

                # Create spatial index if doesn't exist
                indices = arcpy.ListIndexes(input_fc)
                if not any(idx.name == "SHAPE_IDX" for idx in indices):
                    arcpy.AddSpatialIndex_management(input_fc)

                # Group features by attributes
                records = {}
                with arcpy.da.SearchCursor(input_fc, compare_fields + ["SHAPE@", "SHAPE@AREA", "OID@"]) as cursor:
                    for row in cursor:
                        attrs = tuple(str(val) if val is not None else 'None' for val in row[:-3])
                        area = round(row[-2], 2)
                        key = attrs + (area,)
                        
                        if key in records:
                            records[key].append((row[-1], row[-3]))  # Store OID and geometry
                        else:
                            records[key] = [(row[-1], row[-3])]

                duplicate_groups = {k: v for k, v in records.items() if len(v) > 1}
                duplicate_count = 0
                group_count = 0

                if duplicate_groups:
                    # Create output feature class if needed
                    if not arcpy.Exists(output_fc):
                        arcpy.CreateFeatureclass_management(
                            os.path.dirname(output_fc),
                            os.path.basename(output_fc),
                            template=input_fc,
                            spatial_reference=arcpy.Describe(input_fc).spatialReference
                        )
                        self.agregar_campos_descripcion(output_fc)

                    # Process each group
                    for attrs, features in duplicate_groups.items():
                        overlapping_features = set()
                        
                        # Check spatial overlap within group
                        for i, (oid1, geom1) in enumerate(features):
                            for oid2, geom2 in features[i+1:]:
                                try:
                                    intersection = geom1.intersect(geom2, 4)
                                    min_area = min(geom1.area, geom2.area)
                                    
                                    if intersection.area > (min_area * 0.05):  # 5% overlap threshold
                                        overlapping_features.add(oid1)
                                        overlapping_features.add(oid2)
                                except:
                                    continue

                        if overlapping_features:
                            group_count += 1
                            duplicate_count += len(overlapping_features)
                            
                            # Copy only confirmed duplicates
                            fields = [f.name for f in arcpy.ListFields(input_fc) 
                                    if f.type not in ['OID', 'Geometry'] 
                                    and f.name.upper() not in ['SHAPE_LENGTH', 'SHAPE_AREA']]
                            fields.append('SHAPE@')

                            for oid in overlapping_features:
                                where_clause = f"OBJECTID = {oid}"
                                with arcpy.da.SearchCursor(input_fc, fields, where_clause) as search_cursor:
                                    with arcpy.da.InsertCursor(output_fc, fields + ['Error_Descripcion']) as insert_cursor:
                                        for row in search_cursor:
                                            new_row = list(row) + ["Error: Registro Duplicado"]
                                            insert_cursor.insertRow(new_row)

                    if duplicate_count > 0:
                        print(f"Se encontraron {duplicate_count} duplicados en {group_count} grupos")
                        
                return
        except Exception as e:
            print(f"Error procesando {fc}: {str(e)}")
            import traceback
            print(traceback.format_exc())
        finally:
            try:
                arcpy.Delete_management("in_memory")
            except:
                pass     
                       
    def definir_datasets(self):
        return {
            "RURAL_CTM12": [
                "R_UNIDAD_INFORMAL", "R_TERRENO_INFORMAL", "R_UNIDAD_CTM12", "R_TERRENO_CTM12",
                "R_VEREDA_CTM12", "R_SECTOR_CTM12", "R_CONSTRUCCION_CTM12", "R_NOMENCLATURA_VIAL_CTM12",
                "R_CONSTRUCCION_INFORMAL", "R_NOMEN_DOMICILIARIA_CTM12"
            ],
            "URBANO_CTM12": [
                "U_SECTOR_CTM12", "U_TERRENO_CTM12", "U_CONSTRUCCION_CTM12", "U_UNIDAD_CTM12",
                "U_NOMENCLATURA_VIAL_CTM12", "U_UNIDAD_INFORMAL", "U_CONSTRUCCION_INFORMAL",
                "U_NOMEN_DOMICILIARIA_CTM12", "U_MANZANA_CTM12", "U_TERRENO_INFORMAL"
            ],
            "ZONA_HOMOGENEA_RURAL_CTM12": [
                "R_ZONA_HOMOGENEA_FISICA_CTM12", "R_ZONA_HOMO_GEOECONOMICA_CTM12"
            ],
            "ZONA_HOMOGENEA_URBANO_CTM12": [
                "U_ZONA_HOMOGENEA_FISICA_CTM12", "U_ZONA_HOMO_GEOECONOMICA_CTM12"
            ],
            "RURAL": [
                "R_UNIDAD", "R_TERRENO",
                "R_VEREDA", "R_SECTOR", "R_CONSTRUCCION", "R_NOMENCLATURA_VIAL",
                "R_NOMENCLATURA_DOMICILIARIA"
            ],
            "URBANO": [
                "U_SECTOR", "U_TERRENO", "U_CONSTRUCCION", "U_UNIDAD",
                "U_NOMENCLATURA_VIAL",
                "U_NOMENCLATURA_DOMICILIARIA", "U_MANZANA"
            ],
            "ZONA_HOMOGENEA_RURAL": [
                "R_ZONA_HOMOGENEA_FISICA", "R_ZONA_HOMOGENEA_GEOECONOMICA"
            ],
            "ZONA_HOMOGENEA_URBANO": [
                "U_ZONA_HOMOGENEA_FISICA", "U_ZONA_HOMOGENEA_GEOECONOMICA"
            ]
        }


    def definir_queries(self):
        return {
        "RURAL_CTM12": {
            
            "R_SECTOR_CTM12": [
                
                ("CODIGO LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' ","Error: Atributo contiene espacio en blanco al comienzo de su valor."),
                ("CHAR_LENGTH(CODIGO) <> 9 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO IS NULL OR CODIGO IS NULL OR CODIGO IS NULL OR CODIGO_MUNICIPIO IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null'."),
                ("CODIGO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO_MUNICIPIO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '' OR CODIGO_MUNICIPIO LIKE ''","Error: Atributos con valor en blanco o espacios vacíos."),
                ("CODIGO like 'Null' OR CODIGO_MUNICIPIO LIKE 'Null'","Error: Atributos con valor falso 'null'"),
                ("SUBSTRING ( CODIGO,6,2) <> '00'","Error: En código : posiciones 6 Y 7 - La Zona debe ser igual a 00"),
                ("SUBSTRING ( CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, sector  no coincide con código de municipio.")
            ],
            "R_VEREDA_CTM12": [
                ("SUBSTRING ( CODIGO,1,9) <> SECTOR_CODIGO","Error: No hay coincidencia entre código y código de sector"),
                ("SUBSTRING (CODIGO,1,5) <> SUBSTRING( SECTOR_CODIGO,1,5) OR SUBSTRING (CODIGO,1,5) <> SUBSTRING( CODIGO_ANTERIOR ,1,5) OR SUBSTRING (CODIGO,1,5) <> SUBSTRING( CODIGO_MUNICIPIO,1,5)","Error: En código, sector código o código anterior, no coinciden con código municipio."),
                ("(SUBSTRING (CODIGO,1,9) || SUBSTRING (CODIGO,14,4)) <> CODIGO_ANTERIOR","Error: NO hay coincidencia entre código y código anterior "),
                ("SUBSTRING(CODIGO,6,2) <> '00' OR SUBSTRING( SECTOR_CODIGO,6,2) <> '00' OR SUBSTRING( CODIGO_ANTERIOR,6,2) <> '00'","Error: En código, código de sector o código anterior (posiciones 6 Y 7 La Zona debe ser igual a 00)"),

                ("CODIGO LIKE '' or SECTOR_CODIGO LIKE '' or NOMBRE like '' or CODIGO_ANTERIOR like ''","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE '' or SECTOR_CODIGO LIKE '' or NOMBRE like '' or CODIGO_ANTERIOR like '' or CODIGO_MUNICIPIO like '' or CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR SECTOR_CODIGO LIKE '% %' OR SECTOR_CODIGO LIKE '%  %' OR SECTOR_CODIGO LIKE '%   %' OR SECTOR_CODIGO LIKE '%    %' OR SECTOR_CODIGO LIKE '%     %' OR SECTOR_CODIGO LIKE '%      %' OR SECTOR_CODIGO LIKE '%       %' OR SECTOR_CODIGO LIKE '%        %' OR SECTOR_CODIGO LIKE '%         %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR NOMBRE LIKE ' %' OR  NOMBRE LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%     %'","Error: Atributo contiene espacio en blanco o vacíos "),
                ("CODIGO is null or SECTOR_CODIGO is null or NOMBRE is null or CODIGO_ANTERIOR  is null or CODIGO_MUNICIPIO iS null","Error: Atributos con valor 'null' ó en Blanco"),
                ("CHAR_LENGTH (CODIGO) <> 17 or CHAR_LENGTH (SECTOR_CODIGO) <> 9 or CHAR_LENGTH (NOMBRE) < 2 or CHAR_LENGTH (CODIGO_ANTERIOR) <> 13 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta., valores que no cumplen longitud correcta."),
                ("CODIGO like 'Null'  or SECTOR_CODIGO like 'Null' or NOMBRE like 'Null' or CODIGO_ANTERIOR like 'Null' OR CODIGO_MUNICIPIO LIKE 'Null'","Error: Atributos con valor falso 'null'")

                
            ],
            "R_TERRENO_CTM12":[
                ("SUBSTRING(CODIGO, 1, 17) <> VEREDA_CODIGO", "Error: No hay coincidencia entre código y vereda código"),
                ("((SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '8' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '008') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '008'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '9' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '009') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '009'))))) OR (SUBSTRING(CODIGO, 22, 1) = '5')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '005') OR (SUBSTRING(CODIGO, 22, 1) = '2')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '3' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '003') OR   (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '003'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '4' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '004') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '004'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '7' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '007') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '007')))))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código y/o codigo anterior."),
                ("(SUBSTRING(CODIGO,6,2) <>  '00') OR (SUBSTRING( VEREDA_CODIGO,6,2) <>  '00') OR (SUBSTRING( CODIGO_ANTERIOR,6,2) <>  '00')","Error: En código, vereda código o código anterior ( posiciones 6 Y 7 - La Zona debe ser igual a 00)"),
                ("CODIGO IS NULL OR VEREDA_CODIGO IS NULL OR CODIGO_ANTERIOR IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO is null or VEREDA_CODIGO is null or CODIGO_ANTERIOR  is null or CODIGO_MUNICIPIO is null","Error: Atributos con valor falso 'null'"),
                ("CODIGO_ANTERIOR LIKE ' %' OR CODIGO_MUNICIPIO LIKE ' %' OR CODIGO LIKE ' %' OR VEREDA_CODIGO LIKE ' %' OR CODIGO_MUNICIPIO LIKE ' %'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("(CHAR_LENGTH (CODIGO) <> 30) OR (CHAR_LENGTH( VEREDA_CODIGO) <> 17)  OR (CHAR_LENGTH(CODIGO_ANTERIOR) <> 20 ) OR (CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5)","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO LIKE '' OR VEREDA_CODIGO LIKE '' OR CODIGO_ANTERIOR LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR VEREDA_CODIGO LIKE '% %' OR VEREDA_CODIGO LIKE '%  %' OR VEREDA_CODIGO LIKE '%   %' OR VEREDA_CODIGO LIKE '%    %' OR VEREDA_CODIGO LIKE '%     %' OR VEREDA_CODIGO LIKE '%      %' OR VEREDA_CODIGO LIKE '%       %' OR VEREDA_CODIGO LIKE '%        %' OR VEREDA_CODIGO LIKE '%         %' OR VEREDA_CODIGO LIKE '%          %' OR VEREDA_CODIGO LIKE '%           %' OR VEREDA_CODIGO LIKE '%            %' OR VEREDA_CODIGO LIKE '%             %' OR VEREDA_CODIGO LIKE '%              %' OR VEREDA_CODIGO LIKE '%               %' OR VEREDA_CODIGO LIKE '%                %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("SUBSTRING(CODIGO, 22, 1) IN ('1', '2', '5', '6')","Error: En condición de predio (código), debe ser diferente a 1, 2, 5 ó 6"),
                ("(SUBSTRING (CODIGO,18,4) = '0000') OR (SUBSTRING( CODIGO_ANTERIOR,14,4) = '0000')","Error: En código  ( Predio = 0000)"),
                ("(SUBSTRING(CODIGO,1,5) <> CODIGO_MUNICIPIO) OR (SUBSTRING( VEREDA_CODIGO,1,5) <> CODIGO_MUNICIPIO) OR (SUBSTRING( CODIGO_ANTERIOR,1,5) <> CODIGO_MUNICIPIO)","Error: En código, vereda código o código anterior, difieren a código municipio"),
                ("(SUBSTRING (CODIGO,18,4) = '0000') OR (SUBSTRING( CODIGO_ANTERIOR,14,4) = '0000')","Error: No hay coincidencia de código y código anterior")
            ],
            "R_CONSTRUCCION_CTM12":[
                ("((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '0') OR ((SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000'))))) OR ((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '8') OR ((SUBSTRING(CODIGO, 22, 1) = '8' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '008') OR  (SUBSTRING(CODIGO, 23, 4) <> '0000') OR  (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '008'))))) OR (((SUBSTRING(CODIGO,1,21) || SUBSTRING(CODIGO,25,6)) <> (SUBSTRING(TERRENO_CODIGO,1,21) || SUBSTRING(TERRENO_CODIGO,25,6))) AND SUBSTRING(Codigo, 22, 1) = '9')OR ((SUBSTRING(CODIGO, 22, 1) = '9' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '009') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '009'))))) OR (SUBSTRING(Codigo, 22, 1) = '2') OR (SUBSTRING(TERRENO_CODIGO, 22, 1) = '2') OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '002') OR   (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR = (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002'))))) OR ((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '3') OR ((SUBSTRING(CODIGO, 22, 1) = '3' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '003') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '003'))))) OR ((CODIGO = TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '5') OR ((SUBSTRING(CODIGO, 22, 1) = '5' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '000') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || SUBSTRING(CODIGO, 28, 3)))))) OR ((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '3')OR ((SUBSTRING(CODIGO, 22, 1) = '4' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '004') OR   (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '004'))))) OR ((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '7') OR ((SUBSTRING(CODIGO, 22, 1) = '7' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '007') OR  (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '007')))))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o codigo anterior."),
                ("SUBSTRING(codigo, 6, 2) <> '00' OR SUBSTRING(Terreno_Codigo, 6, 2) <> '00' OR SUBSTRING(Codigo_Anterior, 6, 2) <> '00'","Error: En código de terreno, código o código anterior (posiciones 6 Y 7 La Zona debe ser igual a 00)"),
                ("CODIGO IS null OR TERRENO_CODIGO is null OR TIPO_CONSTRUCCION is null  OR NUMERO_PISOS is null OR NUMERO_SOTANOS is null OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like '*null*' OR TERRENO_CODIGO like '*null*' OR TIPO_CONSTRUCCION like '*null*' OR ETIQUETA like '*null*' OR CODIGO_ANTERIOR like '*null*' OR CODIGO_MUNICIPIO LIKE '*null*' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null  OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CODIGO LIKE '*' or TERRENO_CODIGO LIKE '*' or TIPO_CONSTRUCCION LIKE '*' or ETIQUETA LIKE '*' or CODIGO_MUNICIPIO LIKE '*' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null ","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_length (CODIGO) <> 30 or CHAR_length (TERRENO_CODIGO) <> 30 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5 or CHAR_length (CODIGO_ANTERIOR) <> 20 OR NUMERO_PISOS <= 0 OR NUMERO_SOTANOS < 0 OR NUMERO_MEZANINES < 0 OR NUMERO_SEMISOTANOS < 0","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '' or TERRENO_CODIGO like '' or TIPO_CONSTRUCCION like ''  or CODIGO_ANTERIOR like '' OR CODIGO_MUNICIPIO like '' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("SUBSTRING(CODIGO, 22, 1) IN ('1', '2','6') OR SUBSTRING(TERRENO_CODIGO, 22, 1) IN ('1', '2', '5', '6')","Error: En condición de predio en código o código terreno, deben ser diferentes a 1, 2, 5 ó 6"),
                ("substring (CODIGO,18,4) = '0000' or substring (TERRENO_CODIGO,18,4) = '0000' or substring (CODIGO_ANTERIOR ,14,4) = '0000'","Error: En código de terreno o código ( Predio = 0000)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring (TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring (CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, código o código anterior, difieren a código municipio")                
            ],
            "R_UNIDAD_CTM12":[
                
                ("substring(CODIGO,6,2) <> '00' OR substring (TERRENO_CODIGO,6,2) <> '00' OR substring (CONSTRUCCION_CODIGO,6,2) <> '00'","Error: En código, código de terreno o código de construcción (La Zona debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(CONSTRUCCION_CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, código de terreno o código de construcción, difieren a código municipio"),
                ("substring(CODIGO, 22, 1) IN ('1', '2', '6') OR substring(TERRENO_CODIGO, 22, 1) IN ('1', '2', '5', '6') OR substring(CONSTRUCCION_CODIGO, 22, 1) IN ('1', '2', '6')","Error: En condición de predio no permitido. En código, código terreno o código construcción"),
                ("(SUBSTRING(CODIGO, 22, 1) = '0' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '8' AND (SUBSTRING(TERRENO_CODIGO, 23, 4) <> '0000' OR SUBSTRING(TERRENO_CODIGO, 27, 4) = '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 4) <> '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 27, 4) = '0000' OR SUBSTRING(CODIGO, 23, 4) <> '0000' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '9' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR      SUBSTRING(CONSTRUCCION_CODIGO, 25, 6) <> '000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 2) = '00' OR SUBSTRING(CODIGO, 23, 2) = '00' OR SUBSTRING(CODIGO, 25, 2) = '00' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO = TERRENO_CODIGO OR CODIGO = CONSTRUCCION_CODIGO OR (SUBSTRING(CODIGO,1,21) <> SUBSTRING(TERRENO_CODIGO,1,21) OR SUBSTRING(CODIGO,1,21) <> SUBSTRING(CONSTRUCCION_CODIGO,1,21)))) OR SUBSTRING(CODIGO,22,1) = '2' OR  SUBSTRING(TERRENO_CODIGO,22,1) = '2' OR SUBSTRING(CONSTRUCCION_CODIGO,22,1) = '2' OR (SUBSTRING(CODIGO, 22, 1) = '2' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR     SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR     SUBSTRING(CODIGO, 23, 8) <> '00000000' OR     CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '5' AND     (SUBSTRING(TERRENO_CODIGO, 22, 9) <> '000000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 4) <> '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 27, 4) = '0000' OR SUBSTRING(CODIGO, 23, 4) <> '0000' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO = TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO OR (SUBSTRING(CODIGO,1,21) <> SUBSTRING(TERRENO_CODIGO,1,21) OR SUBSTRING(CODIGO,1,21) <> SUBSTRING(CONSTRUCCION_CODIGO,1,21)))) OR (SUBSTRING(CODIGO, 22, 1) = '3' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '4' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR     SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '7' AND (SUBSTRING(TERRENO_CODIGO, 23, 4) <> '0000' OR SUBSTRING(TERRENO_CODIGO, 27, 4) = '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 4) <> '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 27, 4) = '0000' OR SUBSTRING(CODIGO, 23, 4) <> '0000' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO))","Error: en codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o codigo de construcción."),
               
                ("(substring(CODIGO,25,2)>'00' AND substring(CODIGO,22,1)<>'9') OR (substring(CODIGO,22,1)='9' AND ((substring(CODIGO,25,2)='01' AND (PLANTA<>'Piso 1' AND PLANTA<>'PS-01')) OR (substring(CODIGO,25,2)='02' AND (PLANTA<>'Piso 2' AND PLANTA<>'PS-02')) OR (substring(CODIGO,25,2)='03' AND (PLANTA<>'Piso 3' AND PLANTA<>'PS-03')) OR (substring(CODIGO,25,2)='04' AND (PLANTA<>'Piso 4' AND PLANTA<>'PS-04')) OR (substring(CODIGO,25,2)='05' AND (PLANTA<>'Piso 5' AND PLANTA<>'PS-05')) OR (substring(CODIGO,25,2)='06' AND (PLANTA<>'Piso 6' AND PLANTA<>'PS-06')) OR (substring(CODIGO,25,2)='07' AND (PLANTA<>'Piso 7' AND PLANTA<>'PS-07')) OR (substring(CODIGO,25,2)='08' AND (PLANTA<>'Piso 8' AND PLANTA<>'PS-08')) OR (substring(CODIGO,25,2)='09' AND (PLANTA<>'Piso 9' AND PLANTA<>'PS-09')) OR (substring(CODIGO,25,2)='10' AND (PLANTA<>'Piso 10' AND PLANTA<>'PS-10')) OR (substring(CODIGO,25,2)='11' AND (PLANTA<>'Piso 11' AND PLANTA<>'PS-11')) OR (substring(CODIGO,25,2)='12' AND (PLANTA<>'Piso 12' AND PLANTA<>'PS-12')) OR (substring(CODIGO,25,2)='13' AND (PLANTA<>'Piso 13' AND PLANTA<>'PS-13')) OR (substring(CODIGO,25,2)='14' AND (PLANTA<>'Piso 14' AND PLANTA<>'PS-14')) OR (substring(CODIGO,25,2)='15' AND (PLANTA<>'Piso 15' AND PLANTA<>'PS-15')) OR (substring(CODIGO,25,2)='16' AND (PLANTA<>'Piso 16' AND PLANTA<>'PS-16')) OR (substring(CODIGO,25,2)='17' AND (PLANTA<>'Piso 17' AND PLANTA<>'PS-17')) OR (substring(CODIGO,25,2)='18' AND (PLANTA<>'Piso 18' AND PLANTA<>'PS-18')) OR (substring(CODIGO,25,2)='19' AND (PLANTA<>'Piso 19' AND PLANTA<>'PS-19')) OR (substring(CODIGO,25,2)='20' AND (PLANTA<>'Piso 20' AND PLANTA<>'PS-20')) OR (substring(CODIGO,25,2)='21' AND (PLANTA<>'Piso 21' AND PLANTA<>'PS-21')) OR (substring(CODIGO,25,2)='22' AND (PLANTA<>'Piso 22' AND PLANTA<>'PS-22')) OR (substring(CODIGO,25,2)='23' AND (PLANTA<>'Piso 23' AND PLANTA<>'PS-23')) OR (substring(CODIGO,25,2)='24' AND (PLANTA<>'Piso 24' AND PLANTA<>'PS-24')) OR (substring(CODIGO,25,2)='25' AND (PLANTA<>'Piso 25' AND PLANTA<>'PS-25')) OR (substring(CODIGO,25,2)='26' AND (PLANTA<>'Piso 26' AND PLANTA<>'PS-26')) OR (substring(CODIGO,25,2)='27' AND (PLANTA<>'Piso 27' AND PLANTA<>'PS-27')) OR (substring(CODIGO,25,2)='28' AND (PLANTA<>'Piso 28' AND PLANTA<>'PS-28')) OR (substring(CODIGO,25,2)='29' AND (PLANTA<>'Piso 29' AND PLANTA<>'PS-29')) OR (substring(CODIGO,25,2)='30' AND (PLANTA<>'Piso 30' AND PLANTA<>'PS-30')) OR (substring(CODIGO,25,2)='31' AND (PLANTA<>'Piso 31' AND PLANTA<>'PS-31')) OR (substring(CODIGO,25,2)='32' AND (PLANTA<>'Piso 32' AND PLANTA<>'PS-32')) OR (substring(CODIGO,25,2)='33' AND (PLANTA<>'Piso 33' AND PLANTA<>'PS-33')) OR (substring(CODIGO,25,2)='34' AND (PLANTA<>'Piso 34' AND PLANTA<>'PS-34')) OR (substring(CODIGO,25,2)='35' AND (PLANTA<>'Piso 35' AND PLANTA<>'PS-35')) OR (substring(CODIGO,25,2)='36' AND (PLANTA<>'Piso 36' AND PLANTA<>'PS-36')) OR (substring(CODIGO,25,2)='37' AND (PLANTA<>'Piso 37' AND PLANTA<>'PS-37')) OR (substring(CODIGO,25,2)='38' AND (PLANTA<>'Piso 38' AND PLANTA<>'PS-38')) OR (substring(CODIGO,25,2)='39' AND (PLANTA<>'Piso 39' AND PLANTA<>'PS-39')) OR (substring(CODIGO,25,2)='40' AND (PLANTA<>'Piso 40' AND PLANTA<>'PS-40')) OR (substring(CODIGO,25,2)='41' AND (PLANTA<>'Piso 41' AND PLANTA<>'PS-41')) OR (substring(CODIGO,25,2)='42' AND (PLANTA<>'Piso 42' AND PLANTA<>'PS-42')) OR (substring(CODIGO,25,2)='43' AND (PLANTA<>'Piso 43' AND PLANTA<>'PS-43')) OR (substring(CODIGO,25,2)='44' AND (PLANTA<>'Piso 44' AND PLANTA<>'PS-44')) OR (substring(CODIGO,25,2)='45' AND (PLANTA<>'Piso 45' AND PLANTA<>'PS-45')) OR (substring(CODIGO,25,2)='46' AND (PLANTA<>'Piso 46' AND PLANTA<>'PS-46')) OR (substring(CODIGO,25,2)='47' AND (PLANTA<>'Piso 47' AND PLANTA<>'PS-47')) OR (substring(CODIGO,25,2)='48' AND (PLANTA<>'Piso 48' AND PLANTA<>'PS-48')) OR (substring(CODIGO,25,2)='49' AND (PLANTA<>'Piso 49' AND PLANTA<>'PS-49')) OR (substring(CODIGO,25,2)='50' AND (PLANTA<>'Piso 50' AND PLANTA<>'PS-50')) OR (substring(CODIGO,25,2)='51' AND (PLANTA<>'Piso 51' AND PLANTA<>'PS-51')) OR (substring(CODIGO,25,2)='52' AND (PLANTA<>'Piso 52' AND PLANTA<>'PS-52')) OR (substring(CODIGO,25,2)='53' AND (PLANTA<>'Piso 53' AND PLANTA<>'PS-53')) OR (substring(CODIGO,25,2)='54' AND (PLANTA<>'Piso 54' AND PLANTA<>'PS-54')) OR (substring(CODIGO,25,2)='55' AND (PLANTA<>'Piso 55' AND PLANTA<>'PS-55')) OR (substring(CODIGO,25,2)='56' AND (PLANTA<>'Piso 56' AND PLANTA<>'PS-56')) OR (substring(CODIGO,25,2)='57' AND (PLANTA<>'Piso 57' AND PLANTA<>'PS-57')) OR (substring(CODIGO,25,2)='58' AND (PLANTA<>'Piso 58' AND PLANTA<>'PS-58')) OR (substring(CODIGO,25,2)='59' AND (PLANTA<>'Piso 59' AND PLANTA<>'PS-59')) OR (substring(CODIGO,25,2)='60' AND (PLANTA<>'Piso 60' AND PLANTA<>'PS-60')) OR (substring(CODIGO,25,2)='61' AND (PLANTA<>'Piso 61' AND PLANTA<>'PS-61')) OR (substring(CODIGO,25,2)='62' AND (PLANTA<>'Piso 62' AND PLANTA<>'PS-62')) OR (substring(CODIGO,25,2)='63' AND (PLANTA<>'Piso 63' AND PLANTA<>'PS-63')) OR (substring(CODIGO,25,2)='64' AND (PLANTA<>'Piso 64' AND PLANTA<>'PS-64')) OR (substring(CODIGO,25,2)='65' AND (PLANTA<>'Piso 65' AND PLANTA<>'PS-65')) OR (substring(CODIGO,25,2)='66' AND (PLANTA<>'Piso 66' AND PLANTA<>'PS-66')) OR (substring(CODIGO,25,2)='67' AND (PLANTA<>'Piso 67' AND PLANTA<>'PS-67')) OR (substring(CODIGO,25,2)='68' AND (PLANTA<>'Piso 68' AND PLANTA<>'PS-68')) OR (substring(CODIGO,25,2)='69' AND (PLANTA<>'Piso 69' AND PLANTA<>'PS-69')) OR (substring(CODIGO,25,2)='70' AND (PLANTA<>'Piso 70' AND PLANTA<>'PS-70')) OR (substring(CODIGO,25,2)='71' AND (PLANTA<>'Piso 71' AND PLANTA<>'PS-71')) OR (substring(CODIGO,25,2)='72' AND (PLANTA<>'Piso 72' AND PLANTA<>'PS-72')) OR (substring(CODIGO,25,2)='73' AND (PLANTA<>'Piso 73' AND PLANTA<>'PS-73')) OR (substring(CODIGO,25,2)='74' AND (PLANTA<>'Piso 74' AND PLANTA<>'PS-74')) OR (substring(CODIGO,25,2)='75' AND (PLANTA<>'Piso 75' AND PLANTA<>'PS-75')) OR (substring(CODIGO,25,2)='76' AND (PLANTA<>'Piso 76' AND PLANTA<>'PS-76')) OR (substring(CODIGO,25,2)='77' AND (PLANTA<>'Piso 77' AND PLANTA<>'PS-77')) OR (substring(CODIGO,25,2)='78' AND (PLANTA<>'Piso 78' AND PLANTA<>'PS-78')) OR (substring(CODIGO,25,2)='79' AND (PLANTA<>'Piso 79' AND PLANTA<>'PS-79')) OR (substring(CODIGO,25,2)='80' AND (PLANTA<>'Piso 80' AND PLANTA<>'PS-80')) OR (substring(CODIGO,25,2)='99' AND PLANTA<>'ST-01') OR (substring(CODIGO,25,2)='98' AND PLANTA<>'ST-02') OR (substring(CODIGO,25,2)='97' AND PLANTA<>'ST-03') OR (substring(CODIGO,25,2)='96' AND PLANTA<>'ST-04') OR (substring(CODIGO,25,2)='95' AND PLANTA<>'ST-05') OR (substring(CODIGO,25,2)='94' AND PLANTA<>'ST-06') OR (substring(CODIGO,25,2)='93' AND PLANTA<>'ST-07') OR (substring(CODIGO,25,2)='92' AND PLANTA<>'ST-08') OR (substring(CODIGO,25,2)='91' AND PLANTA<>'ST-09') OR (substring(CODIGO,25,2)='90' AND PLANTA<>'ST-10') OR (substring(CODIGO,25,2)='89' AND PLANTA<>'ST-11') OR (substring(CODIGO,25,2)='88' AND PLANTA<>'ST-12') OR (substring(CODIGO,25,2)='87' AND PLANTA<>'ST-13') OR (substring(CODIGO,25,2)='86' AND PLANTA<>'ST-14') OR (substring(CODIGO,25,2)='85' AND PLANTA<>'ST-15') OR (substring(CODIGO,25,2)='84' AND PLANTA<>'ST-16') OR (substring(CODIGO,25,2)='83' AND PLANTA<>'ST-17') OR (substring(CODIGO,25,2)='82' AND PLANTA<>'ST-18') OR (substring(CODIGO,25,2)='81' AND PLANTA<>'ST-19')))","Error: En código (Posiciones 25 y 26 - Piso)"),
                ("substring (CODIGO,18,4) = '0000' or substring (TERRENO_CODIGO,18,4) = '0000'  or substring (CONSTRUCCION_CODIGO,18,4) = '0000'","Error: En código de terreno, código de construcción o código ( Predio = 0000)"),
                #("substring (CODIGO,22,1)='9' AND substring(CODIGO,19,3) < '900' OR substring (CODIGO,19,3) > '999' OR substring (CODIGO,22,1)='8' AND substring(CODIGO,19,3) < '800' OR substring (CODIGO,19,3) > '899'","Error: Código con condición  de PH o de Condominio"),
                
                ("CODIGO LIKE '' or TERRENO_CODIGO LIKE '' or CONSTRUCCION_CODIGO like '' or PLANTA like '' or TIPO_CONSTRUCCION like '' or (SUBSTRING(CODIGO, 22, 1) = '9' AND (ETIQUETA like ' ' OR  ETIQUETA like '' OR ETIQUETA IS NULL)) or IDENTIFICADOR like '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CONSTRUCCION_CODIGO LIKE '% %' OR CONSTRUCCION_CODIGO LIKE '%  %' OR CONSTRUCCION_CODIGO LIKE '%   %' OR CONSTRUCCION_CODIGO LIKE '%    %' OR CONSTRUCCION_CODIGO LIKE '%     %' OR CONSTRUCCION_CODIGO LIKE '%      %' OR CONSTRUCCION_CODIGO LIKE '%       %' OR CONSTRUCCION_CODIGO LIKE '%        %' OR CONSTRUCCION_CODIGO LIKE '%         %' OR CONSTRUCCION_CODIGO LIKE '%          %' OR CONSTRUCCION_CODIGO LIKE '%           %' OR CONSTRUCCION_CODIGO LIKE '%            %' OR CONSTRUCCION_CODIGO LIKE '%             %' OR CONSTRUCCION_CODIGO LIKE '%              %' OR CONSTRUCCION_CODIGO LIKE '%               %' OR CONSTRUCCION_CODIGO LIKE '%                %' OR CONSTRUCCION_CODIGO LIKE '%                 %' OR CONSTRUCCION_CODIGO LIKE '%                  %' OR CONSTRUCCION_CODIGO LIKE '%                   %' OR CONSTRUCCION_CODIGO LIKE '%                    %' OR CONSTRUCCION_CODIGO LIKE '%                     %' OR CONSTRUCCION_CODIGO LIKE '%                      %' OR CONSTRUCCION_CODIGO LIKE '%                       %' OR CONSTRUCCION_CODIGO LIKE '%                        %' OR CONSTRUCCION_CODIGO LIKE '%                         %' OR CONSTRUCCION_CODIGO LIKE '%                          %' OR CONSTRUCCION_CODIGO LIKE '%                           %' OR CONSTRUCCION_CODIGO LIKE '%                            %' OR CONSTRUCCION_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),

                ("CODIGO LIKE ' *' or TERRENO_CODIGO LIKE ' *' or CONSTRUCCION_CODIGO like ' *' or PLANTA like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *' or IDENTIFICADOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                
                ("CODIGO is null or TERRENO_CODIGO is null or CONSTRUCCION_CODIGO is null or PLANTA is null or TIPO_CONSTRUCCION is null or IDENTIFICADOR is null OR CODIGO_MUNICIPIO is null","Error: Atributos con valor 'null' ó en Blanco"),
                ("CHAR_length(CODIGO) <> 30 or CHAR_length (TERRENO_CODIGO) <> 30 or CHAR_length (CONSTRUCCION_CODIGO) <> 30 or IDENTIFICADOR  LIKE '*/*' or CHAR_length ( IDENTIFICADOR ) <> 1 OR CHAR_length(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '*null*' or TERRENO_CODIGO like '*null*' or CONSTRUCCION_CODIGO like '*null*' or PLANTA like '*null*' or TIPO_CONSTRUCCION like '*null*' or ETIQUETA like '*null*' or IDENTIFICADOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'") 
                
            ],
            "R_NOMEN_DOMICILIARIA_CTM12":[
                
                ("substring (TERRENO_CODIGO ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, difieren a código municipio"),
                ("TEXTO IS NULL or TERRENO_CODIGO IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("TEXTO LIKE'' OR TERRENO_CODIGO LIKE'' OR CODIGO_MUNICIPIO LIKE'' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("TEXTO LIKE '*_*' or CHAR_LENGTH( TERRENO_CODIGO ) <> 30 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: Formato de atributos, no cumplen longitud correcta"),
                ("TEXTO like '*null*'  or TERRENO_CODIGO like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("substring( TERRENO_CODIGO ,6, 2) <> '00'","Error: Código de terreno (zona debe ser 00)"),
                ("TEXTO LIKE ' *' or TERRENO_CODIGO LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *'","Error: Valor de atributo comienza con espacio en blanco"),                
            ],
            "R_NOMENCLATURA_VIAL_CTM12":[
                ("TEXTO like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("TEXTO IS null","Error: Atributos con valor 'null' ó en Blanco"),
                ("TEXTO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("TEXTO  like' '","Error: Atributos con valor en blanco"),
            ],
            
            ### SQL PARA INFORMALES RURAL_CTM12
            
            "R_TERRENO_INFORMAL":[
                ("(SUBSTRING(CODIGO, 22, 1) <> '2')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002')))))","Error: En condición del predio debe ser = 2 ó en codificación de las ultimas 8 o 4 posiciones, ó discrepancias entre código y/o código anterior"),
                
                ("(SUBSTRING(CODIGO,6,2) <>  '00') OR (SUBSTRING( CODIGO_ANTERIOR,6,2) <>  '00')","Error: En código, vereda código o código anterior ( posiciones 6 Y 7 - La Zona debe ser igual a 00)"),
                ("CODIGO IS NULL  OR CODIGO_ANTERIOR IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO is null or CODIGO_ANTERIOR  is null or CODIGO_MUNICIPIO is null","Error: Atributos con valor falso 'null'"),
                ("CODIGO_ANTERIOR LIKE ' %' OR CODIGO_MUNICIPIO LIKE ' %' OR CODIGO LIKE ' %' OR CODIGO_MUNICIPIO LIKE ' %'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("(CHAR_LENGTH (CODIGO) <> 30)   OR (CHAR_LENGTH ( CODIGO_ANTERIOR) <> 20 ) OR (CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5)","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO LIKE ''  OR CODIGO_ANTERIOR LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("substring(CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6',  '7', '8', '9')","Error: En condición de predio (código), solo se permite condicion ''2''"),
                ("(SUBSTRING (CODIGO,18,4) = '0000') OR (SUBSTRING( CODIGO_ANTERIOR,14,4) = '0000')","Error: En código  ( Predio = 0000)"),
                ("(SUBSTRING(CODIGO,1,5) <> CODIGO_MUNICIPIO)  OR (SUBSTRING( CODIGO_ANTERIOR,1,5) <> CODIGO_MUNICIPIO)","Error: En código, vereda código o código anterior, difieren a código municipio"),
                ("(SUBSTRING (CODIGO,18,4) = '0000') OR (SUBSTRING( CODIGO_ANTERIOR,14,4) = '0000')","Error: No hay coincidencia de código y código anterior")
                
                ],
            "R_CONSTRUCCION_INFORMAL":[
                ("((CODIGO <> TERRENO_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) <> '2')  OR (SUBSTRING(TERRENO_CODIGO, 22, 1) <> '2')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <>(SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002')))))","Error: En condición del predio debe ser = 2 ó en codificación de las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o código anterior."),               
                ("(SUBSTRING (CODIGO,27,4)  > '0000' AND SUBSTRING (CODIGO,22,1)  <> '8') OR (SUBSTRING (CODIGO,27,4) = '0000' AND SUBSTRING ( CODIGO ,22,1)  =  '8')","Error: En condición de propiedad de código anterior"),
                ("SUBSTRING(codigo, 6, 2) <> '00' OR SUBSTRING(Terreno_Codigo, 6, 2) <> '00' OR SUBSTRING(Codigo_Anterior, 6, 2) <> '00'","Error: En código de terreno, código o código anterior (posiciones 6 Y 7 La Zona debe ser igual a 00)"),
                ("CODIGO IS null OR TERRENO_CODIGO is null OR TIPO_CONSTRUCCION is null  OR NUMERO_PISOS is null OR NUMERO_SOTANOS is null OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like '*null*' OR TERRENO_CODIGO like '*null*' OR TIPO_CONSTRUCCION like '*null*' OR ETIQUETA like '*null*' OR CODIGO_ANTERIOR like '*null*' OR CODIGO_MUNICIPIO LIKE '*null*' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null  OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CODIGO LIKE '*'or TERRENO_CODIGO LIKE '*' or TIPO_CONSTRUCCION LIKE '*'or ETIQUETA LIKE '*' or CODIGO_MUNICIPIO LIKE '*' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null ","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_length (CODIGO) <> 30 or CHAR_length (TERRENO_CODIGO) <> 30 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5 or CHAR_length (CODIGO_ANTERIOR) <> 20 OR NUMERO_PISOS <= 0 OR NUMERO_SOTANOS < 0 OR NUMERO_MEZANINES < 0 OR NUMERO_SEMISOTANOS < 0","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '' or TERRENO_CODIGO like '' or TIPO_CONSTRUCCION like ''  or CODIGO_ANTERIOR like '' OR CODIGO_MUNICIPIO like '' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("substring(CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6', '7','8','9') OR SUBSTRING(TERRENO_CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6',  '7', '8', '9')","Error: En condición de predio, código o código terreno, solo se permite condicion ''2''"),
                ("substring (CODIGO,18,4) = '0000' or substring (TERRENO_CODIGO,18,4) = '0000' or substring (CODIGO_ANTERIOR ,14,4) = '0000'","Error: En código de terreno o código ( Predio = 0000)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring (TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring (CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, código o código anterior, difieren a código municipio")
                
            ],
            "R_UNIDAD_INFORMAL":[
                ("substring(CODIGO,6,2) <>  '00' OR substring (TERRENO_CODIGO,6,2) <>  '00' OR substring (CONSTRUCCION_CODIGO,6,2) <>  '00'","Error: En código, código de terreno o código de construcción (La Zona debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(CONSTRUCCION_CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, código de terreno o código de construcción difieren a código municipio)"),
                ("substring(CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6', '7', '8', '9') OR SUBSTRING(TERRENO_CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6', '7', '8', '9') OR substring(CONSTRUCCION_CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6', '7', '8', '9')","Error: En condición de predio no permitido. En código, código terreno o código construcción"),
                
                ("SUBSTRING(CODIGO,22,1) <> '2' OR  SUBSTRING(TERRENO_CODIGO,22,1) <> '2' OR SUBSTRING(CONSTRUCCION_CODIGO,22,1) <> '2' OR (SUBSTRING(CODIGO, 22, 1) = '2' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR           CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO))","Error: En condición del predio debe ser = 2 ó en codificación de las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o código construcción."),
                
                ("substring (CODIGO,18,4) = '0000' or substring (TERRENO_CODIGO,18,4) = '0000'  or substring (CONSTRUCCION_CODIGO,18,4) = '0000'","Error: En código de terreno, código de construcción o código ( Predio = 0000)"),
                ("CODIGO LIKE '' or TERRENO_CODIGO LIKE '' or CONSTRUCCION_CODIGO like '' or PLANTA like '' or TIPO_CONSTRUCCION like ''  or IDENTIFICADOR like '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CONSTRUCCION_CODIGO LIKE '% %' OR CONSTRUCCION_CODIGO LIKE '%  %' OR CONSTRUCCION_CODIGO LIKE '%   %' OR CONSTRUCCION_CODIGO LIKE '%    %' OR CONSTRUCCION_CODIGO LIKE '%     %' OR CONSTRUCCION_CODIGO LIKE '%      %' OR CONSTRUCCION_CODIGO LIKE '%       %' OR CONSTRUCCION_CODIGO LIKE '%        %' OR CONSTRUCCION_CODIGO LIKE '%         %' OR CONSTRUCCION_CODIGO LIKE '%          %' OR CONSTRUCCION_CODIGO LIKE '%           %' OR CONSTRUCCION_CODIGO LIKE '%            %' OR CONSTRUCCION_CODIGO LIKE '%             %' OR CONSTRUCCION_CODIGO LIKE '%              %' OR CONSTRUCCION_CODIGO LIKE '%               %' OR CONSTRUCCION_CODIGO LIKE '%                %' OR CONSTRUCCION_CODIGO LIKE '%                 %' OR CONSTRUCCION_CODIGO LIKE '%                  %' OR CONSTRUCCION_CODIGO LIKE '%                   %' OR CONSTRUCCION_CODIGO LIKE '%                    %' OR CONSTRUCCION_CODIGO LIKE '%                     %' OR CONSTRUCCION_CODIGO LIKE '%                      %' OR CONSTRUCCION_CODIGO LIKE '%                       %' OR CONSTRUCCION_CODIGO LIKE '%                        %' OR CONSTRUCCION_CODIGO LIKE '%                         %' OR CONSTRUCCION_CODIGO LIKE '%                          %' OR CONSTRUCCION_CODIGO LIKE '%                           %' OR CONSTRUCCION_CODIGO LIKE '%                            %' OR CONSTRUCCION_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                
                ("CODIGO LIKE ' *' or TERRENO_CODIGO LIKE ' *' or CONSTRUCCION_CODIGO like ' *' or PLANTA like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *' or IDENTIFICADOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                
                ("CODIGO is null or TERRENO_CODIGO is null or CONSTRUCCION_CODIGO is null or PLANTA is null or TIPO_CONSTRUCCION is null or IDENTIFICADOR is null OR CODIGO_MUNICIPIO is null","Error: Atributos con valor 'null' ó en Blanco"),
                ("CHAR_length(CODIGO) <> 30 or CHAR_length (TERRENO_CODIGO) <> 30 or CHAR_length (CONSTRUCCION_CODIGO) <> 30 or IDENTIFICADOR  LIKE '*/*' or CHAR_length ( IDENTIFICADOR ) <> 1 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '*null*' or TERRENO_CODIGO like '*null*' or CONSTRUCCION_CODIGO like '*null*' or PLANTA like '*null*' or TIPO_CONSTRUCCION like '*null*' or ETIQUETA like '*null*' or IDENTIFICADOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'")
                
                

            ]
            
            
        },
        "RURAL": {
                       
            "R_SECTOR": [
                
                ("CODIGO LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' ","Error: Atributo contiene espacio en blanco al comienzo de su valor."),
                ("CHAR_LENGTH(CODIGO) <> 9 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO IS NULL OR CODIGO IS NULL OR CODIGO IS NULL OR CODIGO_MUNICIPIO IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null'."),
                ("CODIGO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO_MUNICIPIO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '' OR CODIGO_MUNICIPIO LIKE ''","Error: Atributos con valor en blanco o espacios vacíos."),
                ("CODIGO like 'Null' OR CODIGO_MUNICIPIO LIKE 'Null'","Error: Atributos con valor falso 'null'"),
                ("SUBSTRING ( CODIGO,6,2) <> '00'","Error: En código : posiciones 6 Y 7 - La Zona debe ser igual a 00"),
                ("SUBSTRING ( CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, sector  no coincide con código de municipio.")
            ],
            "R_VEREDA": [
                ("SUBSTRING ( CODIGO,1,9) <> SECTOR_CODIGO","Error: No hay coincidencia entre código y código de sector"),
                ("SUBSTRING (CODIGO,1,5) <> SUBSTRING( SECTOR_CODIGO,1,5) OR SUBSTRING (CODIGO,1,5) <> SUBSTRING( CODIGO_ANTERIOR ,1,5) OR SUBSTRING (CODIGO,1,5) <> SUBSTRING( CODIGO_MUNICIPIO,1,5)","Error: En código, sector código o código anterior, no coinciden con código municipio."),
                ("(SUBSTRING (CODIGO,1,9) || SUBSTRING (CODIGO,14,4)) <> CODIGO_ANTERIOR","Error: NO hay coincidencia entre código y código anterior "),
                ("SUBSTRING(CODIGO,6,2) <> '00' OR SUBSTRING( SECTOR_CODIGO,6,2) <> '00' OR SUBSTRING( CODIGO_ANTERIOR,6,2) <> '00'","Error: En código, código de sector o código anterior (posiciones 6 Y 7 La Zona debe ser igual a 00)"),

                ("CODIGO LIKE '' or SECTOR_CODIGO LIKE '' or NOMBRE like '' or CODIGO_ANTERIOR like ''","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE '' or SECTOR_CODIGO LIKE '' or NOMBRE like '' or CODIGO_ANTERIOR like '' or CODIGO_MUNICIPIO like '' or CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR SECTOR_CODIGO LIKE '% %' OR SECTOR_CODIGO LIKE '%  %' OR SECTOR_CODIGO LIKE '%   %' OR SECTOR_CODIGO LIKE '%    %' OR SECTOR_CODIGO LIKE '%     %' OR SECTOR_CODIGO LIKE '%      %' OR SECTOR_CODIGO LIKE '%       %' OR SECTOR_CODIGO LIKE '%        %' OR SECTOR_CODIGO LIKE '%         %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR NOMBRE LIKE ' %' OR  NOMBRE LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%     %'","Error: Atributo contiene espacio en blanco o vacíos "),
                ("CODIGO is null or SECTOR_CODIGO is null or NOMBRE is null or CODIGO_ANTERIOR  is null or CODIGO_MUNICIPIO iS null","Error: Atributos con valor 'null' ó en Blanco"),
                ("CHAR_LENGTH (CODIGO) <> 17 or CHAR_LENGTH (SECTOR_CODIGO) <> 9 or CHAR_LENGTH (NOMBRE) < 2 or CHAR_LENGTH (CODIGO_ANTERIOR) <> 13 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta., valores que no cumplen longitud correcta."),
                ("CODIGO like 'Null'  or SECTOR_CODIGO like 'Null' or NOMBRE like 'Null' or CODIGO_ANTERIOR like 'Null' OR CODIGO_MUNICIPIO LIKE 'Null'","Error: Atributos con valor falso 'null'")

                
            ],
            "R_TERRENO":[
                ("SUBSTRING(CODIGO, 1, 17) <> VEREDA_CODIGO", "Error: No hay coincidencia entre código y vereda código"),
                ("((SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000')))) OR (SUBSTRING(CODIGO, 22, 1) = '8' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) < '800' OR SUBSTRING(CODIGO_ANTERIOR, 18, 3) >'899') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || SUBSTRING(CODIGO, 19, 3))))) OR (SUBSTRING(CODIGO, 22, 1) = '9' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) < '900' ) OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || SUBSTRING(CODIGO, 19, 3))))) OR (SUBSTRING(CODIGO, 22, 1) NOT IN ('0','8','9') AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000')))))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código y/o codigo anterior."),
                ("(SUBSTRING(CODIGO,6,2) <>  '00') OR (SUBSTRING( VEREDA_CODIGO,6,2) <>  '00') OR (SUBSTRING( CODIGO_ANTERIOR,6,2) <>  '00')","Error: En código, vereda código o código anterior ( posiciones 6 Y 7 - La Zona debe ser igual a 00)"),
                ("CODIGO IS NULL OR VEREDA_CODIGO IS NULL OR CODIGO_ANTERIOR IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO is null or VEREDA_CODIGO is null or CODIGO_ANTERIOR  is null or CODIGO_MUNICIPIO is null","Error: Atributos con valor falso 'null'"),
                ("CODIGO_ANTERIOR LIKE ' %' OR CODIGO_MUNICIPIO LIKE ' %' OR CODIGO LIKE ' %' OR VEREDA_CODIGO LIKE ' %' OR CODIGO_MUNICIPIO LIKE ' %'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("(CHAR_LENGTH (CODIGO) <> 30) OR (CHAR_LENGTH( VEREDA_CODIGO) <> 17)  OR (CHAR_LENGTH(CODIGO_ANTERIOR) <> 20 ) OR (CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5)","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO LIKE '' OR VEREDA_CODIGO LIKE '' OR CODIGO_ANTERIOR LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR VEREDA_CODIGO LIKE '% %' OR VEREDA_CODIGO LIKE '%  %' OR VEREDA_CODIGO LIKE '%   %' OR VEREDA_CODIGO LIKE '%    %' OR VEREDA_CODIGO LIKE '%     %' OR VEREDA_CODIGO LIKE '%      %' OR VEREDA_CODIGO LIKE '%       %' OR VEREDA_CODIGO LIKE '%        %' OR VEREDA_CODIGO LIKE '%         %' OR VEREDA_CODIGO LIKE '%          %' OR VEREDA_CODIGO LIKE '%           %' OR VEREDA_CODIGO LIKE '%            %' OR VEREDA_CODIGO LIKE '%             %' OR VEREDA_CODIGO LIKE '%              %' OR VEREDA_CODIGO LIKE '%               %' OR VEREDA_CODIGO LIKE '%                %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                #("SUBSTRING(CODIGO, 22, 1) IN ('1', '2',  '6')","Error: En condición de predio (código), debe ser diferente a 1, 2,  ó 6"),
                ("(SUBSTRING (CODIGO,18,4) = '0000') OR (SUBSTRING( CODIGO_ANTERIOR,14,4) = '0000')","Error: En código  ( Predio = 0000)"),
                ("(SUBSTRING(CODIGO,1,5) <> CODIGO_MUNICIPIO) OR (SUBSTRING( VEREDA_CODIGO,1,5) <> CODIGO_MUNICIPIO) OR (SUBSTRING( CODIGO_ANTERIOR,1,5) <> CODIGO_MUNICIPIO)","Error: En código, vereda código o código anterior, difieren a código municipio"),
                ("(SUBSTRING (CODIGO,18,4) = '0000') OR (SUBSTRING( CODIGO_ANTERIOR,14,4) = '0000')","Error: No hay coincidencia de código y código anterior")
            ],
            "R_CONSTRUCCION":[
                ("((CODIGO <> TERRENO_CODIGO AND SUBSTRING(Codigo, 22, 1) = '0') OR (SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000')))) OR (CODIGO <> TERRENO_CODIGO AND SUBSTRING(Codigo, 22, 1) = '8') OR (SUBSTRING(CODIGO, 22, 1) = '8' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) < '800' OR SUBSTRING(CODIGO_ANTERIOR, 18, 3) >'899') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || SUBSTRING(CODIGO, 19, 3))))) OR (((SUBSTRING(CODIGO,1,21) || SUBSTRING(CODIGO,25,6)) <> (SUBSTRING(TERRENO_CODIGO,1,21) || SUBSTRING(TERRENO_CODIGO,25,6))) AND SUBSTRING(Codigo, 22, 1) = '9') OR (SUBSTRING(CODIGO, 22, 1) = '9' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) < '900') OR (SUBSTRING(CODIGO, 25, 6) <> '000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || SUBSTRING(CODIGO, 19, 3))))) OR (CODIGO = TERRENO_CODIGO AND SUBSTRING(Codigo, 22, 1) = '5') OR (SUBSTRING(CODIGO, 22, 1) = '5' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '000') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || SUBSTRING(CODIGO, 28, 3))))) OR (SUBSTRING(CODIGO, 22, 1) NOT IN ('0','5','8','9') AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000')))))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o codigo anterior."),
                ("SUBSTRING(codigo, 6, 2) <> '00' OR SUBSTRING(Terreno_Codigo, 6, 2) <> '00' OR SUBSTRING(Codigo_Anterior, 6, 2) <> '00'","Error: En código de terreno, código o código anterior (posiciones 6 Y 7 La Zona debe ser igual a 00)"),
                ("CODIGO IS null OR TERRENO_CODIGO is null OR TIPO_CONSTRUCCION is null  OR NUMERO_PISOS is null OR NUMERO_SOTANOS is null OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like '*null*' OR TERRENO_CODIGO like '*null*' OR TIPO_CONSTRUCCION like '*null*' OR ETIQUETA like '*null*' OR CODIGO_ANTERIOR like '*null*' OR CODIGO_MUNICIPIO LIKE '*null*' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null  OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CODIGO LIKE '*' or TERRENO_CODIGO LIKE '*' or TIPO_CONSTRUCCION LIKE '*' or ETIQUETA LIKE '*' or CODIGO_MUNICIPIO LIKE '*' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null ","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_length (CODIGO) <> 30 or CHAR_length (TERRENO_CODIGO) <> 30 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5 or CHAR_length (CODIGO_ANTERIOR) <> 20 OR NUMERO_PISOS <= 0 OR NUMERO_SOTANOS < 0 OR NUMERO_MEZANINES < 0 OR NUMERO_SEMISOTANOS < 0","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '' or TERRENO_CODIGO like '' or TIPO_CONSTRUCCION like ''  or CODIGO_ANTERIOR like '' OR CODIGO_MUNICIPIO like '' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                #("SUBSTRING(CODIGO, 22, 1) IN ('1', '2','6') OR SUBSTRING(TERRENO_CODIGO, 22, 1) IN ('1', '2', '5', '6')","Error: En condición de predio en código o código terreno, deben ser diferentes a 1, 2, 5 ó 6"),
                ("substring (CODIGO,18,4) = '0000' or substring (TERRENO_CODIGO,18,4) = '0000' or substring (CODIGO_ANTERIOR ,14,4) = '0000'","Error: En código de terreno o código ( Predio = 0000)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring (TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring (CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, código o código anterior, difieren a código municipio")                
            ],
            "R_UNIDAD":[
                
                ("substring(CODIGO,6,2) <> '00' OR substring (TERRENO_CODIGO,6,2) <> '00' OR substring (CONSTRUCCION_CODIGO,6,2) <> '00'","Error: En código, código de terreno o código de construcción (La Zona debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(CONSTRUCCION_CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, código de terreno o código de construcción, difieren a código municipio"),
                #("substring(CODIGO, 22, 1) IN ('1', '2', '6') OR substring(TERRENO_CODIGO, 22, 1) IN ('1', '2', '5', '6') OR substring(CONSTRUCCION_CODIGO, 22, 1) IN ('1', '2', '6')","Error: En condición de predio no permitido. En código, código terreno o código construcción"),
                ("(SUBSTRING(CODIGO, 22, 1) NOT IN ('5', '8','9') AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '8' AND (SUBSTRING(TERRENO_CODIGO, 23, 4) <> '0000' OR SUBSTRING(TERRENO_CODIGO, 27, 4) = '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 4) <> '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 27, 4) = '0000' OR SUBSTRING(CODIGO, 23, 4) <> '0000' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '9' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 25, 6) <> '000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 2) = '00' OR SUBSTRING(CODIGO, 23, 2) = '00' OR SUBSTRING(CODIGO, 25, 2) = '00' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO = TERRENO_CODIGO OR CODIGO = CONSTRUCCION_CODIGO OR (SUBSTRING(CODIGO,1,21) <> SUBSTRING(TERRENO_CODIGO,1,21) OR SUBSTRING(CODIGO,1,21) <> SUBSTRING(CONSTRUCCION_CODIGO,1,21))))","Error: en codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o codigo de construcción."),
               
                ("(substring(CODIGO,25,2)>'00' AND substring(CODIGO,22,1)<>'9') OR (substring(CODIGO,22,1)='9' AND ((substring(CODIGO,25,2)='01' AND (PLANTA<>'Piso 1' AND PLANTA<>'PS-01')) OR (substring(CODIGO,25,2)='02' AND (PLANTA<>'Piso 2' AND PLANTA<>'PS-02')) OR (substring(CODIGO,25,2)='03' AND (PLANTA<>'Piso 3' AND PLANTA<>'PS-03')) OR (substring(CODIGO,25,2)='04' AND (PLANTA<>'Piso 4' AND PLANTA<>'PS-04')) OR (substring(CODIGO,25,2)='05' AND (PLANTA<>'Piso 5' AND PLANTA<>'PS-05')) OR (substring(CODIGO,25,2)='06' AND (PLANTA<>'Piso 6' AND PLANTA<>'PS-06')) OR (substring(CODIGO,25,2)='07' AND (PLANTA<>'Piso 7' AND PLANTA<>'PS-07')) OR (substring(CODIGO,25,2)='08' AND (PLANTA<>'Piso 8' AND PLANTA<>'PS-08')) OR (substring(CODIGO,25,2)='09' AND (PLANTA<>'Piso 9' AND PLANTA<>'PS-09')) OR (substring(CODIGO,25,2)='10' AND (PLANTA<>'Piso 10' AND PLANTA<>'PS-10')) OR (substring(CODIGO,25,2)='11' AND (PLANTA<>'Piso 11' AND PLANTA<>'PS-11')) OR (substring(CODIGO,25,2)='12' AND (PLANTA<>'Piso 12' AND PLANTA<>'PS-12')) OR (substring(CODIGO,25,2)='13' AND (PLANTA<>'Piso 13' AND PLANTA<>'PS-13')) OR (substring(CODIGO,25,2)='14' AND (PLANTA<>'Piso 14' AND PLANTA<>'PS-14')) OR (substring(CODIGO,25,2)='15' AND (PLANTA<>'Piso 15' AND PLANTA<>'PS-15')) OR (substring(CODIGO,25,2)='16' AND (PLANTA<>'Piso 16' AND PLANTA<>'PS-16')) OR (substring(CODIGO,25,2)='17' AND (PLANTA<>'Piso 17' AND PLANTA<>'PS-17')) OR (substring(CODIGO,25,2)='18' AND (PLANTA<>'Piso 18' AND PLANTA<>'PS-18')) OR (substring(CODIGO,25,2)='19' AND (PLANTA<>'Piso 19' AND PLANTA<>'PS-19')) OR (substring(CODIGO,25,2)='20' AND (PLANTA<>'Piso 20' AND PLANTA<>'PS-20')) OR (substring(CODIGO,25,2)='21' AND (PLANTA<>'Piso 21' AND PLANTA<>'PS-21')) OR (substring(CODIGO,25,2)='22' AND (PLANTA<>'Piso 22' AND PLANTA<>'PS-22')) OR (substring(CODIGO,25,2)='23' AND (PLANTA<>'Piso 23' AND PLANTA<>'PS-23')) OR (substring(CODIGO,25,2)='24' AND (PLANTA<>'Piso 24' AND PLANTA<>'PS-24')) OR (substring(CODIGO,25,2)='25' AND (PLANTA<>'Piso 25' AND PLANTA<>'PS-25')) OR (substring(CODIGO,25,2)='26' AND (PLANTA<>'Piso 26' AND PLANTA<>'PS-26')) OR (substring(CODIGO,25,2)='27' AND (PLANTA<>'Piso 27' AND PLANTA<>'PS-27')) OR (substring(CODIGO,25,2)='28' AND (PLANTA<>'Piso 28' AND PLANTA<>'PS-28')) OR (substring(CODIGO,25,2)='29' AND (PLANTA<>'Piso 29' AND PLANTA<>'PS-29')) OR (substring(CODIGO,25,2)='30' AND (PLANTA<>'Piso 30' AND PLANTA<>'PS-30')) OR (substring(CODIGO,25,2)='31' AND (PLANTA<>'Piso 31' AND PLANTA<>'PS-31')) OR (substring(CODIGO,25,2)='32' AND (PLANTA<>'Piso 32' AND PLANTA<>'PS-32')) OR (substring(CODIGO,25,2)='33' AND (PLANTA<>'Piso 33' AND PLANTA<>'PS-33')) OR (substring(CODIGO,25,2)='34' AND (PLANTA<>'Piso 34' AND PLANTA<>'PS-34')) OR (substring(CODIGO,25,2)='35' AND (PLANTA<>'Piso 35' AND PLANTA<>'PS-35')) OR (substring(CODIGO,25,2)='36' AND (PLANTA<>'Piso 36' AND PLANTA<>'PS-36')) OR (substring(CODIGO,25,2)='37' AND (PLANTA<>'Piso 37' AND PLANTA<>'PS-37')) OR (substring(CODIGO,25,2)='38' AND (PLANTA<>'Piso 38' AND PLANTA<>'PS-38')) OR (substring(CODIGO,25,2)='39' AND (PLANTA<>'Piso 39' AND PLANTA<>'PS-39')) OR (substring(CODIGO,25,2)='40' AND (PLANTA<>'Piso 40' AND PLANTA<>'PS-40')) OR (substring(CODIGO,25,2)='41' AND (PLANTA<>'Piso 41' AND PLANTA<>'PS-41')) OR (substring(CODIGO,25,2)='42' AND (PLANTA<>'Piso 42' AND PLANTA<>'PS-42')) OR (substring(CODIGO,25,2)='43' AND (PLANTA<>'Piso 43' AND PLANTA<>'PS-43')) OR (substring(CODIGO,25,2)='44' AND (PLANTA<>'Piso 44' AND PLANTA<>'PS-44')) OR (substring(CODIGO,25,2)='45' AND (PLANTA<>'Piso 45' AND PLANTA<>'PS-45')) OR (substring(CODIGO,25,2)='46' AND (PLANTA<>'Piso 46' AND PLANTA<>'PS-46')) OR (substring(CODIGO,25,2)='47' AND (PLANTA<>'Piso 47' AND PLANTA<>'PS-47')) OR (substring(CODIGO,25,2)='48' AND (PLANTA<>'Piso 48' AND PLANTA<>'PS-48')) OR (substring(CODIGO,25,2)='49' AND (PLANTA<>'Piso 49' AND PLANTA<>'PS-49')) OR (substring(CODIGO,25,2)='50' AND (PLANTA<>'Piso 50' AND PLANTA<>'PS-50')) OR (substring(CODIGO,25,2)='51' AND (PLANTA<>'Piso 51' AND PLANTA<>'PS-51')) OR (substring(CODIGO,25,2)='52' AND (PLANTA<>'Piso 52' AND PLANTA<>'PS-52')) OR (substring(CODIGO,25,2)='53' AND (PLANTA<>'Piso 53' AND PLANTA<>'PS-53')) OR (substring(CODIGO,25,2)='54' AND (PLANTA<>'Piso 54' AND PLANTA<>'PS-54')) OR (substring(CODIGO,25,2)='55' AND (PLANTA<>'Piso 55' AND PLANTA<>'PS-55')) OR (substring(CODIGO,25,2)='56' AND (PLANTA<>'Piso 56' AND PLANTA<>'PS-56')) OR (substring(CODIGO,25,2)='57' AND (PLANTA<>'Piso 57' AND PLANTA<>'PS-57')) OR (substring(CODIGO,25,2)='58' AND (PLANTA<>'Piso 58' AND PLANTA<>'PS-58')) OR (substring(CODIGO,25,2)='59' AND (PLANTA<>'Piso 59' AND PLANTA<>'PS-59')) OR (substring(CODIGO,25,2)='60' AND (PLANTA<>'Piso 60' AND PLANTA<>'PS-60')) OR (substring(CODIGO,25,2)='61' AND (PLANTA<>'Piso 61' AND PLANTA<>'PS-61')) OR (substring(CODIGO,25,2)='62' AND (PLANTA<>'Piso 62' AND PLANTA<>'PS-62')) OR (substring(CODIGO,25,2)='63' AND (PLANTA<>'Piso 63' AND PLANTA<>'PS-63')) OR (substring(CODIGO,25,2)='64' AND (PLANTA<>'Piso 64' AND PLANTA<>'PS-64')) OR (substring(CODIGO,25,2)='65' AND (PLANTA<>'Piso 65' AND PLANTA<>'PS-65')) OR (substring(CODIGO,25,2)='66' AND (PLANTA<>'Piso 66' AND PLANTA<>'PS-66')) OR (substring(CODIGO,25,2)='67' AND (PLANTA<>'Piso 67' AND PLANTA<>'PS-67')) OR (substring(CODIGO,25,2)='68' AND (PLANTA<>'Piso 68' AND PLANTA<>'PS-68')) OR (substring(CODIGO,25,2)='69' AND (PLANTA<>'Piso 69' AND PLANTA<>'PS-69')) OR (substring(CODIGO,25,2)='70' AND (PLANTA<>'Piso 70' AND PLANTA<>'PS-70')) OR (substring(CODIGO,25,2)='71' AND (PLANTA<>'Piso 71' AND PLANTA<>'PS-71')) OR (substring(CODIGO,25,2)='72' AND (PLANTA<>'Piso 72' AND PLANTA<>'PS-72')) OR (substring(CODIGO,25,2)='73' AND (PLANTA<>'Piso 73' AND PLANTA<>'PS-73')) OR (substring(CODIGO,25,2)='74' AND (PLANTA<>'Piso 74' AND PLANTA<>'PS-74')) OR (substring(CODIGO,25,2)='75' AND (PLANTA<>'Piso 75' AND PLANTA<>'PS-75')) OR (substring(CODIGO,25,2)='76' AND (PLANTA<>'Piso 76' AND PLANTA<>'PS-76')) OR (substring(CODIGO,25,2)='77' AND (PLANTA<>'Piso 77' AND PLANTA<>'PS-77')) OR (substring(CODIGO,25,2)='78' AND (PLANTA<>'Piso 78' AND PLANTA<>'PS-78')) OR (substring(CODIGO,25,2)='79' AND (PLANTA<>'Piso 79' AND PLANTA<>'PS-79')) OR (substring(CODIGO,25,2)='80' AND (PLANTA<>'Piso 80' AND PLANTA<>'PS-80')) OR (substring(CODIGO,25,2)='99' AND PLANTA<>'ST-01') OR (substring(CODIGO,25,2)='98' AND PLANTA<>'ST-02') OR (substring(CODIGO,25,2)='97' AND PLANTA<>'ST-03') OR (substring(CODIGO,25,2)='96' AND PLANTA<>'ST-04') OR (substring(CODIGO,25,2)='95' AND PLANTA<>'ST-05') OR (substring(CODIGO,25,2)='94' AND PLANTA<>'ST-06') OR (substring(CODIGO,25,2)='93' AND PLANTA<>'ST-07') OR (substring(CODIGO,25,2)='92' AND PLANTA<>'ST-08') OR (substring(CODIGO,25,2)='91' AND PLANTA<>'ST-09') OR (substring(CODIGO,25,2)='90' AND PLANTA<>'ST-10') OR (substring(CODIGO,25,2)='89' AND PLANTA<>'ST-11') OR (substring(CODIGO,25,2)='88' AND PLANTA<>'ST-12') OR (substring(CODIGO,25,2)='87' AND PLANTA<>'ST-13') OR (substring(CODIGO,25,2)='86' AND PLANTA<>'ST-14') OR (substring(CODIGO,25,2)='85' AND PLANTA<>'ST-15') OR (substring(CODIGO,25,2)='84' AND PLANTA<>'ST-16') OR (substring(CODIGO,25,2)='83' AND PLANTA<>'ST-17') OR (substring(CODIGO,25,2)='82' AND PLANTA<>'ST-18') OR (substring(CODIGO,25,2)='81' AND PLANTA<>'ST-19')))","Error: En código (Posiciones 25 y 26 - Piso)"),
                ("substring (CODIGO,18,4) = '0000' or substring (TERRENO_CODIGO,18,4) = '0000'  or substring (CONSTRUCCION_CODIGO,18,4) = '0000'","Error: En código de terreno, código de construcción o código ( Predio = 0000)"),
                #("substring (CODIGO,22,1)='9' AND substring(CODIGO,19,3) < '900' OR substring (CODIGO,19,3) > '999' OR substring (CODIGO,22,1)='8' AND substring(CODIGO,19,3) < '800' OR substring (CODIGO,19,3) > '899'","Error: Código con condición  de PH o de Condominio"),
                
                ("CODIGO LIKE '' or TERRENO_CODIGO LIKE '' or CONSTRUCCION_CODIGO like '' or PLANTA like '' or TIPO_CONSTRUCCION like '' or (SUBSTRING(CODIGO, 22, 1) = '9' AND (ETIQUETA like ' ' OR  ETIQUETA like '' OR ETIQUETA IS NULL)) or IDENTIFICADOR like '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CONSTRUCCION_CODIGO LIKE '% %' OR CONSTRUCCION_CODIGO LIKE '%  %' OR CONSTRUCCION_CODIGO LIKE '%   %' OR CONSTRUCCION_CODIGO LIKE '%    %' OR CONSTRUCCION_CODIGO LIKE '%     %' OR CONSTRUCCION_CODIGO LIKE '%      %' OR CONSTRUCCION_CODIGO LIKE '%       %' OR CONSTRUCCION_CODIGO LIKE '%        %' OR CONSTRUCCION_CODIGO LIKE '%         %' OR CONSTRUCCION_CODIGO LIKE '%          %' OR CONSTRUCCION_CODIGO LIKE '%           %' OR CONSTRUCCION_CODIGO LIKE '%            %' OR CONSTRUCCION_CODIGO LIKE '%             %' OR CONSTRUCCION_CODIGO LIKE '%              %' OR CONSTRUCCION_CODIGO LIKE '%               %' OR CONSTRUCCION_CODIGO LIKE '%                %' OR CONSTRUCCION_CODIGO LIKE '%                 %' OR CONSTRUCCION_CODIGO LIKE '%                  %' OR CONSTRUCCION_CODIGO LIKE '%                   %' OR CONSTRUCCION_CODIGO LIKE '%                    %' OR CONSTRUCCION_CODIGO LIKE '%                     %' OR CONSTRUCCION_CODIGO LIKE '%                      %' OR CONSTRUCCION_CODIGO LIKE '%                       %' OR CONSTRUCCION_CODIGO LIKE '%                        %' OR CONSTRUCCION_CODIGO LIKE '%                         %' OR CONSTRUCCION_CODIGO LIKE '%                          %' OR CONSTRUCCION_CODIGO LIKE '%                           %' OR CONSTRUCCION_CODIGO LIKE '%                            %' OR CONSTRUCCION_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),

                ("CODIGO LIKE ' *' or TERRENO_CODIGO LIKE ' *' or CONSTRUCCION_CODIGO like ' *' or PLANTA like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *' or IDENTIFICADOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                
                ("CODIGO is null or TERRENO_CODIGO is null or CONSTRUCCION_CODIGO is null or PLANTA is null or TIPO_CONSTRUCCION is null or IDENTIFICADOR is null OR CODIGO_MUNICIPIO is null","Error: Atributos con valor 'null' ó en Blanco"),
                ("CHAR_length(CODIGO) <> 30 or CHAR_length (TERRENO_CODIGO) <> 30 or CHAR_length (CONSTRUCCION_CODIGO) <> 30 or IDENTIFICADOR  LIKE '*/*' or CHAR_length ( IDENTIFICADOR ) <> 1 OR CHAR_length(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '*null*' or TERRENO_CODIGO like '*null*' or CONSTRUCCION_CODIGO like '*null*' or PLANTA like '*null*' or TIPO_CONSTRUCCION like '*null*' or ETIQUETA like '*null*' or IDENTIFICADOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'") 
                
            ],
            "R_NOMENCLATURA_DOMICILIARIA":[
                
                ("substring (TERRENO_CODIGO ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, difieren a código municipio"),
                ("TEXTO IS NULL or TERRENO_CODIGO IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("TEXTO LIKE'' OR TERRENO_CODIGO LIKE'' OR CODIGO_MUNICIPIO LIKE'' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("TEXTO LIKE '*_*' or CHAR_LENGTH( TERRENO_CODIGO ) <> 30 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: Formato de atributos, no cumplen longitud correcta"),
                ("TEXTO like '*null*'  or TERRENO_CODIGO like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("substring( TERRENO_CODIGO ,6, 2) <> '00'","Error: Código de terreno (zona debe ser 00)"),
                ("TEXTO LIKE ' *' or TERRENO_CODIGO LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *'","Error: Valor de atributo comienza con espacio en blanco"),                
            ],
            "R_NOMENCLATURA_VIAL":[
                ("TEXTO like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("TEXTO IS null","Error: Atributos con valor 'null' ó en Blanco"),
                ("TEXTO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("TEXTO  like' '","Error: Atributos con valor en blanco"),
            ]
                      
                  
        
        },
        "URBANO_CTM12": {
            "U_SECTOR_CTM12": [
                ("CODIGO LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' ","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_LENGTH(CODIGO) <> 9 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO IS NULL OR CODIGO = '' OR CODIGO = ' ' OR CODIGO_MUNICIPIO IS NULL OR CODIGO_MUNICIPIO = ' '","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO_MUNICIPIO LIKE '%     %' OR CODIGO LIKE '%      %' OR       CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '' OR CODIGO_MUNICIPIO LIKE ''","Error: Atributos con valor en blanco"),
                ("CODIGO like 'Null' OR CODIGO_MUNICIPIO LIKE 'Null'","Error: Atributos con valor falso 'null'"),
                ("substring( CODIGO ,6,2) =  '00'","Error: En código ( posiciones 6 Y 7 - La Zona NO debe ser igual a 00)"),
                ("substring( CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, sector código o código anterior, difieren a código municipio")
            ],
            "U_BARRIO_CTM12":[
                ("CODIGO LIKE ' *' OR SECTOR_CODIGO LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' ","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO IS NULL OR CODIGO = '' OR CODIGO = ' ' OR SECTOR_CODIGO IS NULL OR SECTOR_CODIGO = ' ' OR SECTOR_CODIGO = '' OR CODIGO_MUNICIPIO IS NULL OR CODIGO_MUNICIPIO = ' ' OR CODIGO_MUNICIPIO =  ''","Error: Atributos con valor 'null' ó en Blanco"),
                ("SECTOR_CODIGO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '% %' OR SECTOR_CODIGO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%  %' OR SECTOR_CODIGO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%   %' OR SECTOR_CODIGO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%    %' OR SECTOR_CODIGO LIKE '%     %' OR CODIGO_MUNICIPIO LIKE '%     %' OR SECTOR_CODIGO LIKE '%      %' OR       SECTOR_CODIGO LIKE '%       %' OR SECTOR_CODIGO LIKE '%        %' OR SECTOR_CODIGO LIKE '%         %' OR SECTOR_CODIGO LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %'","Error: Atributos con valor en blanco"),
                ("CODIGO like 'Null' OR CODIGO_MUNICIPIO LIKE 'Null' OR SECTOR_CODIGO like 'Null'","Error: Atributos con valor falso 'null'"),
                ("substring( CODIGO ,6,2) =  '00' OR substring( SECTOR_CODIGO ,6,2) =  '00'","Error: En código o Sector Codigo ( posiciones 6 Y 7 - La Zona NO debe ser igual a 00)"),
                ("substring( CODIGO ,1,9) <> SECTOR_CODIGO","Error: No hay coincidencia entre código y sector código"),
                ("substring( CODIGO ,1,5) <> CODIGO_MUNICIPIO OR substring( SECTOR_CODIGO ,1,5) <> CODIGO_MUNICIPIO ","Error: En código o sector código, difieren a código municipio")

            ],

            "U_MANZANA_CTM12": [
                ("substring( CODIGO ,1,13) <> BARRIO_CODIGO","Error: No hay coincidencia entre código y barrio código"),
                ("substring( CODIGO ,1,5) <> CODIGO_MUNICIPIO OR substring( BARRIO_CODIGO ,1,5) <> CODIGO_MUNICIPIO OR substring( CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO","Error: En código, barrio código o código anterior, difieren a código municipio"),
                ("SUBSTRING (CODIGO,1,9) || SUBSTRING (CODIGO,14,4) <> CODIGO_ANTERIOR","Error: NO hay coincidencia entre código y código anterior "),
                ("SUBSTRING(CODIGO ,6,2) =  '00' OR SUBSTRING( BARRIO_CODIGO ,6,2) =  '00' OR SUBSTRING( CODIGO_ANTERIOR ,6,2) =  '00'","Error: En código, código de barrio o código anterior (posiciones 6 Y 7 La Zona debe ser igual a 00)"),
                ("CODIGO LIKE '' or BARRIO_CODIGO LIKE '' or CODIGO_ANTERIOR LIKE '' OR CODIGO_MUNICIPIO LIKE ''           OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR BARRIO_CODIGO LIKE '% %' OR BARRIO_CODIGO LIKE '%  %' OR BARRIO_CODIGO LIKE '%   %' OR BARRIO_CODIGO LIKE '%    %' OR BARRIO_CODIGO LIKE '%     %' OR BARRIO_CODIGO LIKE '%      %' OR BARRIO_CODIGO LIKE '%       %' OR BARRIO_CODIGO LIKE '%        %' OR BARRIO_CODIGO LIKE '%         %' OR BARRIO_CODIGO LIKE '%          %' OR BARRIO_CODIGO LIKE '%           %' OR BARRIO_CODIGO LIKE '%            %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%     %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' or BARRIO_CODIGO LIKE ' *' or  CODIGO_ANTERIOR like ' *' OR CODIGO_MUNICIPIO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO is null or BARRIO_CODIGO is null or CODIGO_ANTERIOR is null OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CHAR_LENGTH( CODIGO ) <> 17 OR CHAR_LENGTH( BARRIO_CODIGO ) <> 13 OR CHAR_LENGTH( CODIGO_ANTERIOR ) <> 13 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '*null*' or BARRIO_CODIGO like '*null*' or CODIGO_ANTERIOR  like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),    
            ],
            "U_TERRENO_CTM12": [
                ("substring( CODIGO ,1,17) <> MANZANA_CODIGO","Error: No hay coincidencia entre código y manzana código"),
                ("((SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '8' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '008') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '008'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '9' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '009') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '009'))))) OR (SUBSTRING(CODIGO, 22, 1) = '5')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '005') OR (SUBSTRING(CODIGO, 22, 1) = '2')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '3' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '003') OR   (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '003'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '4' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '004') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '004'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '7' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '007') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '007')))))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código y/o codigo anterior."),
                ("substring( CODIGO ,23,4) > '0000' ","Error: En código  ( posiciones TORRE Y PISO)"),
                ("(substring ( CODIGO ,6,2) =  '00') OR (substring ( MANZANA_CODIGO ,6,2) =  '00') OR (substring( CODIGO_ANTERIOR ,6,2) =  '00')","Error: En código, manzana código o código anterior (La Zona NO debe ser igual a 00)"),
                ("CODIGO IS null or MANZANA_CODIGO is null or CODIGO_ANTERIOR is null OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like '*null*'  or MANZANA_CODIGO like '*null*' or CODIGO_ANTERIOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("CODIGO like ' *' or MANZANA_CODIGO like ' *' or CODIGO_ANTERIOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_LENGTH (CODIGO) <> 30 OR CHAR_LENGTH (MANZANA_CODIGO) <> 17 OR CHAR_LENGTH (CODIGO_ANTERIOR ) <> 20 OR  CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO LIKE '' OR MANZANA_CODIGO LIKE '' OR CODIGO_ANTERIOR LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR  CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %'  OR MANZANA_CODIGO LIKE '% %' OR MANZANA_CODIGO LIKE '%  %' OR MANZANA_CODIGO LIKE '%   %' OR MANZANA_CODIGO LIKE '%    %' OR MANZANA_CODIGO LIKE '%     %' OR MANZANA_CODIGO LIKE '%      %' OR MANZANA_CODIGO LIKE '%       %' OR MANZANA_CODIGO LIKE '%        %' OR MANZANA_CODIGO LIKE '%         %' OR MANZANA_CODIGO LIKE '%          %' OR MANZANA_CODIGO LIKE '%           %' OR MANZANA_CODIGO LIKE '%            %' OR MANZANA_CODIGO LIKE '%             %' OR MANZANA_CODIGO LIKE '%              %' OR MANZANA_CODIGO LIKE '%               %' OR MANZANA_CODIGO LIKE '%                %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("SUBSTRING(CODIGO, 22, 1) IN ('1','2', '6')","Error: En condición de predio (código), debe ser diferente a 1, 2,  ó 6"),
                
                ("(substring (CODIGO,18,4) ='0000') OR (substring(CODIGO_ANTERIOR,14,4)='0000')","Error: En código  ( Predio = 0000)"),
                
                ("(substring(CODIGO,1,5) <> CODIGO_MUNICIPIO) OR (substring(CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO) OR (substring(MANZANA_CODIGO,1,5) <> CODIGO_MUNICIPIO)","Error: En código, código de manzana o código anterior, difieren a código municipio"),
                
                ("((SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '8' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '008') OR      (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '008'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '9' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '009') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '009'))))) OR (SUBSTRING(CODIGO, 22, 1) = '5')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '005') OR (SUBSTRING(CODIGO, 22, 1) = '2')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '3' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '003') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '003'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '4' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '004') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '004'))))) OR (    (SUBSTRING(CODIGO, 22, 1) = '7' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '007') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '007')))))","Error: En código (ultimas 4 posiciones) ó Codigo_Anterior Mal Codificado")

            ],
            "U_CONSTRUCCION_CTM12": [
                
                
                ("CODIGO IS null or TERRENO_CODIGO is null or TIPO_CONSTRUCCION is null or NUMERO_PISOS is null or NUMERO_SOTANOS is null or NUMERO_MEZANINES is null or NUMERO_SEMISOTANOS is null or CODIGO_ANTERIOR is null OR CODIGO_MUNICIPIO is null","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like 'Null' or TERRENO_CODIGO like 'Null' or TIPO_CONSTRUCCION like 'Null' or  ETIQUETA like 'Null'  or CODIGO_ANTERIOR like 'Null' OR CODIGO_MUNICIPIO like 'Null'","Error: Atributos con valor falso 'null'"),
                ("CODIGO like ' *' or TERRENO_CODIGO like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *'  or CODIGO_ANTERIOR like ' *' or CODIGO_MUNICIPIO like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_LENGTH(CODIGO) <> 30 or CHAR_LENGTH(TERRENO_CODIGO) <> 30 or CHAR_LENGTH(CODIGO_ANTERIOR) <> 20 OR NUMERO_PISOS <= 0 OR NUMERO_SOTANOS < 0 OR NUMERO_MEZANINES < 0 OR NUMERO_SEMISOTANOS < 0 or CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '' or TERRENO_CODIGO like '' or TIPO_CONSTRUCCION like '' or CODIGO_ANTERIOR like '' OR CODIGO_MUNICIPIO like ''OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("substring(CODIGO, 22, 1) IN ('1', '2', '6') OR substring(TERRENO_CODIGO, 22, 1) IN ('1', '2', '5', '6')","Error: En condición de predio código o código terreno, deben diferir de 1,2,6 (codigo) y/o 5 si es terreno_codigo."),
                ("( substring ( CODIGO,18,4) = '0000'  ) or ( substring ( TERRENO_CODIGO ,18,4) = '0000'  ) or ( substring ( CODIGO_ANTERIOR ,14,4) = '0000'  )","Error: En código de terreno o código ( Predio = 0000)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring ( TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring ( CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, código o código anterior, difieren a código municipio"),

                ("((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '0') OR ((SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000'))))) OR ((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '8') OR  ((SUBSTRING(CODIGO, 22, 1) = '8' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '008') OR  (SUBSTRING(CODIGO, 23, 4) <> '0000') OR  (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '008'))))) OR (((SUBSTRING(CODIGO,1,21) || SUBSTRING(CODIGO,25,6)) <> (SUBSTRING(TERRENO_CODIGO,1,21) || SUBSTRING(TERRENO_CODIGO,25,6))) AND SUBSTRING(Codigo, 22, 1) = '9') OR ((SUBSTRING(CODIGO, 22, 1) = '9' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '009') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '009'))))) OR (SUBSTRING(Codigo, 22, 1) = '2') OR (SUBSTRING(TERRENO_CODIGO, 22, 1) = '2') OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '002') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR = (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002'))))) OR ((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '3') OR ((SUBSTRING(CODIGO, 22, 1) = '3' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '003') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR  (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '003'))))) OR ((CODIGO = TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '5') OR ((SUBSTRING(CODIGO, 22, 1) = '5' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '000') OR  (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || SUBSTRING(CODIGO, 28, 3)))))) OR ((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '3') OR ((SUBSTRING(CODIGO, 22, 1) = '4' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '004') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '004'))))) OR ((CODIGO <> TERRENO_CODIGO) AND SUBSTRING(Codigo, 22, 1) = '7') OR ((SUBSTRING(CODIGO, 22, 1) = '7' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '007') OR     (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '007')))))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o codigo anterior."),
                ("substring(CODIGO,6,2) =  '00' OR substring( TERRENO_CODIGO ,6,2) =  '00'  OR substring( CODIGO_ANTERIOR ,6,2) = '00'","Error: En código de terreno, código o código anterior (posiciones 6 Y 7 La Zona NO debe ser igual a 00)")


            ],
            "U_UNIDAD_CTM12": [
                
                ("substring(CODIGO ,6,2) = '00' OR substring(TERRENO_CODIGO ,6,2) = '00' OR substring(CONSTRUCCION_CODIGO ,6,2) = '00'","Error: En código, código de terreno o código de construcción (La Zona NO debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring( CONSTRUCCION_CODIGO ,1,5) <> CODIGO_MUNICIPIO","Error: En código, código de terreno o código de construcción difieren a código municipio"),
                ("substring(CONSTRUCCION_CODIGO, 22, 1) IN ('1', '2', '6') OR substring(CODIGO, 22, 1) IN ('1', '2', '6') OR substring(TERRENO_CODIGO, 22, 1) IN ('1', '2', '5', '6')","Error: En condición de predio no permitido. En código, código terreno o código construcción"),
                ("(substring(CODIGO,25,2)>'00' AND substring(CODIGO,22,1)<>'9') OR (substring(CODIGO,22,1)='9' AND ((substring(CODIGO,25,2)='01' AND (PLANTA<>'Piso 1' AND PLANTA<>'PS-01')) OR (substring(CODIGO,25,2)='02' AND (PLANTA<>'Piso 2' AND PLANTA<>'PS-02')) OR (substring(CODIGO,25,2)='03' AND (PLANTA<>'Piso 3' AND PLANTA<>'PS-03')) OR (substring(CODIGO,25,2)='04' AND (PLANTA<>'Piso 4' AND PLANTA<>'PS-04')) OR (substring(CODIGO,25,2)='05' AND (PLANTA<>'Piso 5' AND PLANTA<>'PS-05')) OR (substring(CODIGO,25,2)='06' AND (PLANTA<>'Piso 6' AND PLANTA<>'PS-06')) OR (substring(CODIGO,25,2)='07' AND (PLANTA<>'Piso 7' AND PLANTA<>'PS-07')) OR (substring(CODIGO,25,2)='08' AND (PLANTA<>'Piso 8' AND PLANTA<>'PS-08')) OR (substring(CODIGO,25,2)='09' AND (PLANTA<>'Piso 9' AND PLANTA<>'PS-09')) OR (substring(CODIGO,25,2)='10' AND (PLANTA<>'Piso 10' AND PLANTA<>'PS-10')) OR (substring(CODIGO,25,2)='11' AND (PLANTA<>'Piso 11' AND PLANTA<>'PS-11')) OR (substring(CODIGO,25,2)='12' AND (PLANTA<>'Piso 12' AND PLANTA<>'PS-12')) OR (substring(CODIGO,25,2)='13' AND (PLANTA<>'Piso 13' AND PLANTA<>'PS-13')) OR (substring(CODIGO,25,2)='14' AND (PLANTA<>'Piso 14' AND PLANTA<>'PS-14')) OR (substring(CODIGO,25,2)='15' AND (PLANTA<>'Piso 15' AND PLANTA<>'PS-15')) OR (substring(CODIGO,25,2)='16' AND (PLANTA<>'Piso 16' AND PLANTA<>'PS-16')) OR (substring(CODIGO,25,2)='17' AND (PLANTA<>'Piso 17' AND PLANTA<>'PS-17')) OR (substring(CODIGO,25,2)='18' AND (PLANTA<>'Piso 18' AND PLANTA<>'PS-18')) OR (substring(CODIGO,25,2)='19' AND (PLANTA<>'Piso 19' AND PLANTA<>'PS-19')) OR (substring(CODIGO,25,2)='20' AND (PLANTA<>'Piso 20' AND PLANTA<>'PS-20')) OR (substring(CODIGO,25,2)='21' AND (PLANTA<>'Piso 21' AND PLANTA<>'PS-21')) OR (substring(CODIGO,25,2)='22' AND (PLANTA<>'Piso 22' AND PLANTA<>'PS-22')) OR (substring(CODIGO,25,2)='23' AND (PLANTA<>'Piso 23' AND PLANTA<>'PS-23')) OR (substring(CODIGO,25,2)='24' AND (PLANTA<>'Piso 24' AND PLANTA<>'PS-24')) OR (substring(CODIGO,25,2)='25' AND (PLANTA<>'Piso 25' AND PLANTA<>'PS-25')) OR (substring(CODIGO,25,2)='26' AND (PLANTA<>'Piso 26' AND PLANTA<>'PS-26')) OR (substring(CODIGO,25,2)='27' AND (PLANTA<>'Piso 27' AND PLANTA<>'PS-27')) OR (substring(CODIGO,25,2)='28' AND (PLANTA<>'Piso 28' AND PLANTA<>'PS-28')) OR (substring(CODIGO,25,2)='29' AND (PLANTA<>'Piso 29' AND PLANTA<>'PS-29')) OR (substring(CODIGO,25,2)='30' AND (PLANTA<>'Piso 30' AND PLANTA<>'PS-30')) OR (substring(CODIGO,25,2)='31' AND (PLANTA<>'Piso 31' AND PLANTA<>'PS-31')) OR (substring(CODIGO,25,2)='32' AND (PLANTA<>'Piso 32' AND PLANTA<>'PS-32')) OR (substring(CODIGO,25,2)='33' AND (PLANTA<>'Piso 33' AND PLANTA<>'PS-33')) OR (substring(CODIGO,25,2)='34' AND (PLANTA<>'Piso 34' AND PLANTA<>'PS-34')) OR (substring(CODIGO,25,2)='35' AND (PLANTA<>'Piso 35' AND PLANTA<>'PS-35')) OR (substring(CODIGO,25,2)='36' AND (PLANTA<>'Piso 36' AND PLANTA<>'PS-36')) OR (substring(CODIGO,25,2)='37' AND (PLANTA<>'Piso 37' AND PLANTA<>'PS-37')) OR (substring(CODIGO,25,2)='38' AND (PLANTA<>'Piso 38' AND PLANTA<>'PS-38')) OR (substring(CODIGO,25,2)='39' AND (PLANTA<>'Piso 39' AND PLANTA<>'PS-39')) OR (substring(CODIGO,25,2)='40' AND (PLANTA<>'Piso 40' AND PLANTA<>'PS-40')) OR (substring(CODIGO,25,2)='41' AND (PLANTA<>'Piso 41' AND PLANTA<>'PS-41')) OR (substring(CODIGO,25,2)='42' AND (PLANTA<>'Piso 42' AND PLANTA<>'PS-42')) OR (substring(CODIGO,25,2)='43' AND (PLANTA<>'Piso 43' AND PLANTA<>'PS-43')) OR (substring(CODIGO,25,2)='44' AND (PLANTA<>'Piso 44' AND PLANTA<>'PS-44')) OR (substring(CODIGO,25,2)='45' AND (PLANTA<>'Piso 45' AND PLANTA<>'PS-45')) OR (substring(CODIGO,25,2)='46' AND (PLANTA<>'Piso 46' AND PLANTA<>'PS-46')) OR (substring(CODIGO,25,2)='47' AND (PLANTA<>'Piso 47' AND PLANTA<>'PS-47')) OR (substring(CODIGO,25,2)='48' AND (PLANTA<>'Piso 48' AND PLANTA<>'PS-48')) OR (substring(CODIGO,25,2)='49' AND (PLANTA<>'Piso 49' AND PLANTA<>'PS-49')) OR (substring(CODIGO,25,2)='50' AND (PLANTA<>'Piso 50' AND PLANTA<>'PS-50')) OR (substring(CODIGO,25,2)='51' AND (PLANTA<>'Piso 51' AND PLANTA<>'PS-51')) OR (substring(CODIGO,25,2)='52' AND (PLANTA<>'Piso 52' AND PLANTA<>'PS-52')) OR (substring(CODIGO,25,2)='53' AND (PLANTA<>'Piso 53' AND PLANTA<>'PS-53')) OR (substring(CODIGO,25,2)='54' AND (PLANTA<>'Piso 54' AND PLANTA<>'PS-54')) OR (substring(CODIGO,25,2)='55' AND (PLANTA<>'Piso 55' AND PLANTA<>'PS-55')) OR (substring(CODIGO,25,2)='56' AND (PLANTA<>'Piso 56' AND PLANTA<>'PS-56')) OR (substring(CODIGO,25,2)='57' AND (PLANTA<>'Piso 57' AND PLANTA<>'PS-57')) OR (substring(CODIGO,25,2)='58' AND (PLANTA<>'Piso 58' AND PLANTA<>'PS-58')) OR (substring(CODIGO,25,2)='59' AND (PLANTA<>'Piso 59' AND PLANTA<>'PS-59')) OR (substring(CODIGO,25,2)='60' AND (PLANTA<>'Piso 60' AND PLANTA<>'PS-60')) OR (substring(CODIGO,25,2)='61' AND (PLANTA<>'Piso 61' AND PLANTA<>'PS-61')) OR (substring(CODIGO,25,2)='62' AND (PLANTA<>'Piso 62' AND PLANTA<>'PS-62')) OR (substring(CODIGO,25,2)='63' AND (PLANTA<>'Piso 63' AND PLANTA<>'PS-63')) OR (substring(CODIGO,25,2)='64' AND (PLANTA<>'Piso 64' AND PLANTA<>'PS-64')) OR (substring(CODIGO,25,2)='65' AND (PLANTA<>'Piso 65' AND PLANTA<>'PS-65')) OR (substring(CODIGO,25,2)='66' AND (PLANTA<>'Piso 66' AND PLANTA<>'PS-66')) OR (substring(CODIGO,25,2)='67' AND (PLANTA<>'Piso 67' AND PLANTA<>'PS-67')) OR (substring(CODIGO,25,2)='68' AND (PLANTA<>'Piso 68' AND PLANTA<>'PS-68')) OR (substring(CODIGO,25,2)='69' AND (PLANTA<>'Piso 69' AND PLANTA<>'PS-69')) OR (substring(CODIGO,25,2)='70' AND (PLANTA<>'Piso 70' AND PLANTA<>'PS-70')) OR (substring(CODIGO,25,2)='71' AND (PLANTA<>'Piso 71' AND PLANTA<>'PS-71')) OR (substring(CODIGO,25,2)='72' AND (PLANTA<>'Piso 72' AND PLANTA<>'PS-72')) OR (substring(CODIGO,25,2)='73' AND (PLANTA<>'Piso 73' AND PLANTA<>'PS-73')) OR (substring(CODIGO,25,2)='74' AND (PLANTA<>'Piso 74' AND PLANTA<>'PS-74')) OR (substring(CODIGO,25,2)='75' AND (PLANTA<>'Piso 75' AND PLANTA<>'PS-75')) OR (substring(CODIGO,25,2)='76' AND (PLANTA<>'Piso 76' AND PLANTA<>'PS-76')) OR (substring(CODIGO,25,2)='77' AND (PLANTA<>'Piso 77' AND PLANTA<>'PS-77')) OR (substring(CODIGO,25,2)='78' AND (PLANTA<>'Piso 78' AND PLANTA<>'PS-78')) OR (substring(CODIGO,25,2)='79' AND (PLANTA<>'Piso 79' AND PLANTA<>'PS-79')) OR (substring(CODIGO,25,2)='80' AND (PLANTA<>'Piso 80' AND PLANTA<>'PS-80')) OR (substring(CODIGO,25,2)='99' AND PLANTA<>'ST-01') OR (substring(CODIGO,25,2)='98' AND PLANTA<>'ST-02') OR (substring(CODIGO,25,2)='97' AND PLANTA<>'ST-03') OR (substring(CODIGO,25,2)='96' AND PLANTA<>'ST-04') OR (substring(CODIGO,25,2)='95' AND PLANTA<>'ST-05') OR (substring(CODIGO,25,2)='94' AND PLANTA<>'ST-06') OR (substring(CODIGO,25,2)='93' AND PLANTA<>'ST-07') OR (substring(CODIGO,25,2)='92' AND PLANTA<>'ST-08') OR (substring(CODIGO,25,2)='91' AND PLANTA<>'ST-09') OR (substring(CODIGO,25,2)='90' AND PLANTA<>'ST-10') OR (substring(CODIGO,25,2)='89' AND PLANTA<>'ST-11') OR (substring(CODIGO,25,2)='88' AND PLANTA<>'ST-12') OR (substring(CODIGO,25,2)='87' AND PLANTA<>'ST-13') OR (substring(CODIGO,25,2)='86' AND PLANTA<>'ST-14') OR (substring(CODIGO,25,2)='85' AND PLANTA<>'ST-15') OR (substring(CODIGO,25,2)='84' AND PLANTA<>'ST-16') OR (substring(CODIGO,25,2)='83' AND PLANTA<>'ST-17') OR (substring(CODIGO,25,2)='82' AND PLANTA<>'ST-18') OR (substring(CODIGO,25,2)='81' AND PLANTA<>'ST-19')))","Error: En código (Posiciones 25 y 26 - Piso)"),

                ("(substring ( CODIGO ,18,4) = '0000') or ( substring ( TERRENO_CODIGO ,18,4)='0000') or ( substring ( CONSTRUCCION_CODIGO ,18,4) = '0000')","Error: En código de terreno, código de construcción o código ( Predio = 0000)"),

                ("(SUBSTRING(CODIGO, 22, 1) = '0' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '8' AND (SUBSTRING(TERRENO_CODIGO, 23, 4) <> '0000' OR SUBSTRING(TERRENO_CODIGO, 27, 4) = '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 4) <> '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 27, 4) = '0000' OR SUBSTRING(CODIGO, 23, 4) <> '0000' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '9' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 25, 6) <> '000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 2) = '00' OR SUBSTRING(CODIGO, 23, 2) = '00' OR SUBSTRING(CODIGO, 25, 2) = '00' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO = TERRENO_CODIGO OR CODIGO = CONSTRUCCION_CODIGO OR (SUBSTRING(CODIGO,1,21) <> SUBSTRING(TERRENO_CODIGO,1,21) OR SUBSTRING(CODIGO,1,21) <> SUBSTRING(CONSTRUCCION_CODIGO,1,21)))) OR SUBSTRING(CODIGO,22,1) = '2' OR SUBSTRING(TERRENO_CODIGO,22,1) = '2' OR SUBSTRING(CONSTRUCCION_CODIGO,22,1) = '2' OR (SUBSTRING(CODIGO, 22, 1) = '2' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '5' AND (SUBSTRING(TERRENO_CODIGO, 22, 9) <> '000000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 4) <> '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 27, 4) = '0000' OR SUBSTRING(CODIGO, 23, 4) <> '0000' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO = TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO OR (SUBSTRING(CODIGO,1,21) <> SUBSTRING(TERRENO_CODIGO,1,21) OR SUBSTRING(CODIGO,1,21) <> SUBSTRING(CONSTRUCCION_CODIGO,1,21)))) OR (SUBSTRING(CODIGO, 22, 1) = '3' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '4' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '7' AND (SUBSTRING(TERRENO_CODIGO, 23, 4) <> '0000' OR SUBSTRING(TERRENO_CODIGO, 27, 4) = '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 4) <> '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 27, 4) = '0000' OR SUBSTRING(CODIGO, 23, 4) <> '0000' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o codigo construccion"),



                ("CODIGO LIKE '' or TERRENO_CODIGO LIKE '' or CONSTRUCCION_CODIGO like '' or PLANTA like '' or TIPO_CONSTRUCCION like '' or (SUBSTRING(CODIGO, 22, 1) = '9' AND (ETIQUETA like ' ' OR  ETIQUETA like '' OR ETIQUETA IS NULL)) or IDENTIFICADOR like '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CONSTRUCCION_CODIGO LIKE '% %' OR CONSTRUCCION_CODIGO LIKE '%  %' OR CONSTRUCCION_CODIGO LIKE '%   %' OR CONSTRUCCION_CODIGO LIKE '%    %' OR CONSTRUCCION_CODIGO LIKE '%     %' OR CONSTRUCCION_CODIGO LIKE '%      %' OR CONSTRUCCION_CODIGO LIKE '%       %' OR CONSTRUCCION_CODIGO LIKE '%        %' OR CONSTRUCCION_CODIGO LIKE '%         %' OR CONSTRUCCION_CODIGO LIKE '%          %' OR CONSTRUCCION_CODIGO LIKE '%           %' OR CONSTRUCCION_CODIGO LIKE '%            %' OR CONSTRUCCION_CODIGO LIKE '%             %' OR CONSTRUCCION_CODIGO LIKE '%              %' OR CONSTRUCCION_CODIGO LIKE '%               %' OR CONSTRUCCION_CODIGO LIKE '%                %' OR CONSTRUCCION_CODIGO LIKE '%                 %' OR CONSTRUCCION_CODIGO LIKE '%                  %' OR CONSTRUCCION_CODIGO LIKE '%                   %' OR CONSTRUCCION_CODIGO LIKE '%                    %' OR CONSTRUCCION_CODIGO LIKE '%                     %' OR CONSTRUCCION_CODIGO LIKE '%                      %' OR CONSTRUCCION_CODIGO LIKE '%                       %' OR CONSTRUCCION_CODIGO LIKE '%                        %' OR CONSTRUCCION_CODIGO LIKE '%                         %' OR CONSTRUCCION_CODIGO LIKE '%                          %' OR CONSTRUCCION_CODIGO LIKE '%                           %' OR CONSTRUCCION_CODIGO LIKE '%                            %' OR CONSTRUCCION_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),

                ("CODIGO LIKE ' *' or TERRENO_CODIGO LIKE ' *' or CONSTRUCCION_CODIGO like ' *' or PLANTA like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *' or IDENTIFICADOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO IS NULL OR TERRENO_CODIGO IS NULL OR CONSTRUCCION_CODIGO IS NULL OR PLANTA IS NULL OR TIPO_CONSTRUCCION IS NULL OR IDENTIFICADOR IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),

                ("CHAR_LENGTH (CODIGO) <> 30 or CHAR_LENGTH (TERRENO_CODIGO) <> 30 or CHAR_LENGTH ( CONSTRUCCION_CODIGO) <> 30 or ( IDENTIFICADOR LIKE '*/*' or CHAR_LENGTH (IDENTIFICADOR) <> 1)  OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '*null*' or TERRENO_CODIGO like '*null*' or CONSTRUCCION_CODIGO like '*null*' or PLANTA like '*null*' or TIPO_CONSTRUCCION like '*null*' or ETIQUETA like '*null*' or IDENTIFICADOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'")
  
            ],
            "U_NOMEN_DOMICILIARIA_CTM12":[
                
                ("substring (TERRENO_CODIGO ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, difieren a código municipio"),
                ("TEXTO IS NULL or TERRENO_CODIGO IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("TEXTO LIKE'' OR TERRENO_CODIGO LIKE'' OR CODIGO_MUNICIPIO LIKE'' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("TEXTO LIKE '*_*' or CHAR_LENGTH( TERRENO_CODIGO ) <> 30 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: Formato de atributos, no cumplen longitud correcta"),
                ("TEXTO like '*null*'  or TERRENO_CODIGO like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("SUBSTRING (TERRENO_CODIGO ,6, 2) = '00'","Error: Código de terreno (zona NO debe ser 00)"),
                ("TEXTO LIKE ' *' or TERRENO_CODIGO LIKE ' *'","Error: Valor de atributo comienza con espacio en blanco")
    
            ],
            "U_NOMENCLATURA_VIAL_CTM12":[
                
                ("TEXTO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("TEXTO IS null","Error: Atributos con valor 'null' ó en Blanco"),
                ("TEXTO LIKE '*null*'","Error: Atributos con valor falso 'null'"),
                ("TEXTO LIKE ''","Error: Atributos con valor en blanco")

            ],
            ### SQL PARA INFORMALES URBANO_CTM12
            
            "U_TERRENO_INFORMAL":[
                ("((SUBSTRING(codigo, 22, 1) = '2') AND (SUBSTRING(codigo_anterior, 18, 3) <> '002'))","Error: En condición de propiedad de código anterior"),
                ("substring( CODIGO ,23,4) > '0000' ","Error: En código  ( posiciones TORRE Y PISO)"),
                ("(substring ( CODIGO ,6,2) =  '00')  OR (substring( CODIGO_ANTERIOR ,6,2) =  '00')","Error: En código o código anterior (La Zona NO debe ser igual a 00)"),
                ("CODIGO IS null or CODIGO_ANTERIOR is null OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like '*null*'  or CODIGO_ANTERIOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("CODIGO like ' *' or CODIGO_ANTERIOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_LENGTH (CODIGO) <> 30 OR CHAR_LENGTH (CODIGO_ANTERIOR ) <> 20 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO LIKE ''  OR CODIGO_ANTERIOR LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("substring(CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6',  '7', '8', '9')","Error: En condición de predio (código), solo se permite condicion ''2''."),
                ("(substring (CODIGO,18,4) ='0000') OR (substring(CODIGO_ANTERIOR,14,4)='0000')","Error: En código  ( Predio = 0000)"),
                ("(substring(CODIGO,1,5) <> CODIGO_MUNICIPIO) OR (substring(CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO) ","Error: En código o código anterior, difieren a código municipio"),

                ("(SUBSTRING(CODIGO, 22, 1) <> '2')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR    (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002')))))","Error: En condición del predio debe ser = 2 ó en codificación de las ultimas 8 o 4 posiciones, ó discrepancias entre código y/o código construcción.")
                

                
            ],
            "U_CONSTRUCCION_INFORMAL":[
                ("((CODIGO <> TERRENO_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) <> '2')  OR (SUBSTRING(TERRENO_CODIGO, 22, 1) <> '2')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR (  (SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002')))))","Error: En condición del predio debe ser = 2 ó en codificación de las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o código anterior"),
                ("substring(CODIGO,6,2) =  '00' OR substring( TERRENO_CODIGO ,6,2) =  '00'  OR substring( CODIGO_ANTERIOR ,6,2) = '00'","Error: En código de terreno, código o código anterior (posiciones 6 Y 7 La Zona NO debe ser igual a 00)"),
                ("CODIGO IS null or TERRENO_CODIGO is null or TIPO_CONSTRUCCION is null or NUMERO_PISOS is null or NUMERO_SOTANOS is null or NUMERO_MEZANINES is null or NUMERO_SEMISOTANOS is null or CODIGO_ANTERIOR is null OR CODIGO_MUNICIPIO is null","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like 'Null' or TERRENO_CODIGO like 'Null' or TIPO_CONSTRUCCION like 'Null' or  ETIQUETA like 'Null'  or CODIGO_ANTERIOR like 'Null' OR CODIGO_MUNICIPIO like 'Null'","Error: Atributos con valor falso 'null'"),
                ("CODIGO like ' *' or TERRENO_CODIGO like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *'  or CODIGO_ANTERIOR like ' *' or CODIGO_MUNICIPIO like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_LENGTH(CODIGO) <> 30 or CHAR_LENGTH(TERRENO_CODIGO) <> 30 or CHAR_LENGTH(CODIGO_ANTERIOR) <> 20 OR NUMERO_PISOS <= 0 OR NUMERO_SOTANOS < 0 OR NUMERO_MEZANINES < 0 OR NUMERO_SEMISOTANOS < 0 or CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '' or TERRENO_CODIGO like '' or TIPO_CONSTRUCCION like ''  or CODIGO_ANTERIOR like '' OR CODIGO_MUNICIPIO like '' OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("substring(CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6', '7','8','9') OR SUBSTRING(TERRENO_CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6', '7','9')","Error: En condición de predio código o código terreno, solo se permite condicion ''2''."),
                ("( substring ( CODIGO,18,4) = '0000'  ) or ( substring ( TERRENO_CODIGO ,18,4) = '0000'  ) or ( substring ( CODIGO_ANTERIOR ,14,4) = '0000'  )","Error: En código de terreno o código ( Predio = 0000)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring ( TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring ( CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, código o código anterior, difieren a código municipio"),
                ("((SUBSTRING(CODIGO, 22, 1) = '2' AND  ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000'))) )","Error: En código de terreno o código (ultimas 4 posiciones)")
                
                ],
            "U_UNIDAD_INFORMAL":[
                ("substring(CODIGO ,6,2) = '00' OR substring(TERRENO_CODIGO ,6,2) = '00' OR substring(CONSTRUCCION_CODIGO ,6,2) = '00'","Error: En código, código de terreno o código de construcción (La Zona NO debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring( CONSTRUCCION_CODIGO ,1,5) <> CODIGO_MUNICIPIO","Error: En código, código de terreno o código de construcción difieren a código municipio"),
                ("substring(CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6','7', '8','9') OR SUBSTRING(TERRENO_CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6', '7', '8','9') OR substring(CONSTRUCCION_CODIGO, 22, 1) IN ('0','1', '3','4', '5', '6','7','8','9')","Error: En condición de predio no permitido. En código, código terreno o código construcción"),
                ("(substring ( CODIGO ,18,4) = '0000') or ( substring ( TERRENO_CODIGO ,18,4)='0000') or ( substring ( CONSTRUCCION_CODIGO ,18,4) = '0000')","Error: En código de terreno, código de construcción o código ( Predio = 0000)"),
                ("SUBSTRING(CODIGO,22,1) <> '2' OR  SUBSTRING(TERRENO_CODIGO,22,1) <> '2' OR SUBSTRING(CONSTRUCCION_CODIGO,22,1) <> '2' OR (SUBSTRING(CODIGO, 22, 1) = '2' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO))","Error: En condición del predio debe ser = 2 ó en codificación de las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o código construcción."),
                ("CODIGO LIKE '' or TERRENO_CODIGO LIKE '' or CONSTRUCCION_CODIGO like '' or PLANTA like '' or TIPO_CONSTRUCCION like '' or IDENTIFICADOR like '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CONSTRUCCION_CODIGO LIKE '% %' OR CONSTRUCCION_CODIGO LIKE '%  %' OR CONSTRUCCION_CODIGO LIKE '%   %' OR CONSTRUCCION_CODIGO LIKE '%    %' OR CONSTRUCCION_CODIGO LIKE '%     %' OR CONSTRUCCION_CODIGO LIKE '%      %' OR CONSTRUCCION_CODIGO LIKE '%       %' OR CONSTRUCCION_CODIGO LIKE '%        %' OR CONSTRUCCION_CODIGO LIKE '%         %' OR CONSTRUCCION_CODIGO LIKE '%          %' OR CONSTRUCCION_CODIGO LIKE '%           %' OR CONSTRUCCION_CODIGO LIKE '%            %' OR CONSTRUCCION_CODIGO LIKE '%             %' OR CONSTRUCCION_CODIGO LIKE '%              %' OR CONSTRUCCION_CODIGO LIKE '%               %' OR CONSTRUCCION_CODIGO LIKE '%                %' OR CONSTRUCCION_CODIGO LIKE '%                 %' OR CONSTRUCCION_CODIGO LIKE '%                  %' OR CONSTRUCCION_CODIGO LIKE '%                   %' OR CONSTRUCCION_CODIGO LIKE '%                    %' OR CONSTRUCCION_CODIGO LIKE '%                     %' OR CONSTRUCCION_CODIGO LIKE '%                      %' OR CONSTRUCCION_CODIGO LIKE '%                       %' OR CONSTRUCCION_CODIGO LIKE '%                        %' OR CONSTRUCCION_CODIGO LIKE '%                         %' OR CONSTRUCCION_CODIGO LIKE '%                          %' OR CONSTRUCCION_CODIGO LIKE '%                           %' OR CONSTRUCCION_CODIGO LIKE '%                            %' OR CONSTRUCCION_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco o espacios vacios"),
                ("CODIGO LIKE ' *' or TERRENO_CODIGO LIKE ' *' or CONSTRUCCION_CODIGO like ' *' or PLANTA like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *' or IDENTIFICADOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO IS NULL OR TERRENO_CODIGO IS NULL OR CONSTRUCCION_CODIGO IS NULL OR PLANTA IS NULL OR TIPO_CONSTRUCCION IS NULL OR IDENTIFICADOR IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CHAR_LENGTH (CODIGO) <> 30 or CHAR_LENGTH (TERRENO_CODIGO) <> 30 or CHAR_LENGTH ( CONSTRUCCION_CODIGO) <> 30 or ( IDENTIFICADOR LIKE '*/*' or CHAR_LENGTH (IDENTIFICADOR) <> 1)  OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),

                ("CODIGO like '*null*' or TERRENO_CODIGO like '*null*' or CONSTRUCCION_CODIGO like '*null*' or PLANTA like '*null*' or TIPO_CONSTRUCCION like '*null*' or ETIQUETA like '*null*' or IDENTIFICADOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'")
            ]
        },
        "URBANO": {
            "U_SECTOR": [
                ("CODIGO LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' ","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_LENGTH(CODIGO) <> 9 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO IS NULL OR CODIGO = '' OR CODIGO = ' ' OR CODIGO_MUNICIPIO IS NULL OR CODIGO_MUNICIPIO = ' '","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO_MUNICIPIO LIKE '%     %' OR CODIGO LIKE '%      %' OR       CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '' OR CODIGO_MUNICIPIO LIKE ''","Error: Atributos con valor en blanco"),
                ("CODIGO like 'Null' OR CODIGO_MUNICIPIO LIKE 'Null'","Error: Atributos con valor falso 'null'"),
                ("substring( CODIGO ,6,2) =  '00'","Error: En código ( posiciones 6 Y 7 - La Zona NO debe ser igual a 00)"),
                ("substring( CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, sector código o código anterior, difieren a código municipio")
            ],
            "U_BARRIO":[
                ("CODIGO LIKE ' *' OR SECTOR_CODIGO LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' ","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO IS NULL OR CODIGO = '' OR CODIGO = ' ' OR SECTOR_CODIGO IS NULL OR SECTOR_CODIGO = ' ' OR SECTOR_CODIGO = '' OR CODIGO_MUNICIPIO IS NULL OR CODIGO_MUNICIPIO = ' ' OR CODIGO_MUNICIPIO =  ''","Error: Atributos con valor 'null' ó en Blanco"),
                ("SECTOR_CODIGO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '% %' OR SECTOR_CODIGO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%  %' OR SECTOR_CODIGO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%   %' OR SECTOR_CODIGO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%    %' OR SECTOR_CODIGO LIKE '%     %' OR CODIGO_MUNICIPIO LIKE '%     %' OR SECTOR_CODIGO LIKE '%      %' OR       SECTOR_CODIGO LIKE '%       %' OR SECTOR_CODIGO LIKE '%        %' OR SECTOR_CODIGO LIKE '%         %' OR SECTOR_CODIGO LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %'","Error: Atributos con valor en blanco"),
                ("CODIGO like 'Null' OR CODIGO_MUNICIPIO LIKE 'Null' OR SECTOR_CODIGO like 'Null'","Error: Atributos con valor falso 'null'"),
                ("substring( CODIGO ,6,2) =  '00' OR substring( SECTOR_CODIGO ,6,2) =  '00'","Error: En código o Sector Codigo ( posiciones 6 Y 7 - La Zona NO debe ser igual a 00)"),
                ("substring( CODIGO ,1,9) <> SECTOR_CODIGO","Error: No hay coincidencia entre código y sector código"),
                ("substring( CODIGO ,1,5) <> CODIGO_MUNICIPIO OR substring( SECTOR_CODIGO ,1,5) <> CODIGO_MUNICIPIO ","Error: En código o sector código, difieren a código municipio")

            ],

            "U_MANZANA": [
                ("substring( CODIGO ,1,13) <> BARRIO_CODIGO","Error: No hay coincidencia entre código y barrio código"),
                ("substring( CODIGO ,1,5) <> CODIGO_MUNICIPIO OR substring( BARRIO_CODIGO ,1,5) <> CODIGO_MUNICIPIO OR substring( CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO","Error: En código, barrio código o código anterior, difieren a código municipio"),
                ("SUBSTRING (CODIGO,1,9) || SUBSTRING (CODIGO,14,4) <> CODIGO_ANTERIOR","Error: NO hay coincidencia entre código y código anterior "),
                ("SUBSTRING(CODIGO ,6,2) =  '00' OR SUBSTRING( BARRIO_CODIGO ,6,2) =  '00' OR SUBSTRING( CODIGO_ANTERIOR ,6,2) =  '00'","Error: En código, código de barrio o código anterior (posiciones 6 Y 7 La Zona debe ser igual a 00)"),
                ("CODIGO LIKE '' or BARRIO_CODIGO LIKE '' or CODIGO_ANTERIOR LIKE '' OR CODIGO_MUNICIPIO LIKE ''           OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR BARRIO_CODIGO LIKE '% %' OR BARRIO_CODIGO LIKE '%  %' OR BARRIO_CODIGO LIKE '%   %' OR BARRIO_CODIGO LIKE '%    %' OR BARRIO_CODIGO LIKE '%     %' OR BARRIO_CODIGO LIKE '%      %' OR BARRIO_CODIGO LIKE '%       %' OR BARRIO_CODIGO LIKE '%        %' OR BARRIO_CODIGO LIKE '%         %' OR BARRIO_CODIGO LIKE '%          %' OR BARRIO_CODIGO LIKE '%           %' OR BARRIO_CODIGO LIKE '%            %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %' OR CODIGO_MUNICIPIO LIKE '%     %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' or BARRIO_CODIGO LIKE ' *' or  CODIGO_ANTERIOR like ' *' OR CODIGO_MUNICIPIO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO is null or BARRIO_CODIGO is null or CODIGO_ANTERIOR is null OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CHAR_LENGTH( CODIGO ) <> 17 OR CHAR_LENGTH( BARRIO_CODIGO ) <> 13 OR CHAR_LENGTH( CODIGO_ANTERIOR ) <> 13 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '*null*' or BARRIO_CODIGO like '*null*' or CODIGO_ANTERIOR  like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),    
            ],
            "U_TERRENO": [
                ("substring( CODIGO ,1,17) <> MANZANA_CODIGO","Error: No hay coincidencia entre código y manzana código"),
                ("((SUBSTRING(codigo, 22, 1) = '0') AND (SUBSTRING(codigo_anterior, 18, 3) <> '000')) OR ((SUBSTRING(codigo, 22, 1) = '9') AND (SUBSTRING(codigo_anterior, 18, 3) <> '009')) OR ((SUBSTRING(codigo, 22, 1) = '8') AND (SUBSTRING(codigo_anterior, 18, 3) <> '008')) OR ((SUBSTRING(codigo, 22, 1) = '7') AND (SUBSTRING(codigo_anterior, 18, 3) <> '007')) OR ((SUBSTRING(codigo, 22, 1) = '4') AND (SUBSTRING(codigo_anterior, 18, 3) <> '004')) OR ((SUBSTRING(codigo, 22, 1) = '3') AND (SUBSTRING(codigo_anterior, 18, 3) <> '003')) OR ((SUBSTRING(codigo, 22, 1) = '2') AND (SUBSTRING(codigo_anterior, 18, 3) <> '002'))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código y/o codigo anterior."),
                ("substring( CODIGO ,23,4) > '0000' ","Error: En código  ( posiciones TORRE Y PISO)"),
                ("(substring ( CODIGO ,6,2) =  '00') OR (substring ( MANZANA_CODIGO ,6,2) =  '00') OR (substring( CODIGO_ANTERIOR ,6,2) =  '00')","Error: En código, manzana código o código anterior (La Zona NO debe ser igual a 00)"),
                ("CODIGO IS null or MANZANA_CODIGO is null or CODIGO_ANTERIOR is null OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like '*null*'  or MANZANA_CODIGO like '*null*' or CODIGO_ANTERIOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("CODIGO like ' *' or MANZANA_CODIGO like ' *' or CODIGO_ANTERIOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_LENGTH (CODIGO) <> 30 OR CHAR_LENGTH (MANZANA_CODIGO) <> 17 OR CHAR_LENGTH (CODIGO_ANTERIOR ) <> 20 OR  CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO LIKE '' OR MANZANA_CODIGO LIKE '' OR CODIGO_ANTERIOR LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR  CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %'  OR MANZANA_CODIGO LIKE '% %' OR MANZANA_CODIGO LIKE '%  %' OR MANZANA_CODIGO LIKE '%   %' OR MANZANA_CODIGO LIKE '%    %' OR MANZANA_CODIGO LIKE '%     %' OR MANZANA_CODIGO LIKE '%      %' OR MANZANA_CODIGO LIKE '%       %' OR MANZANA_CODIGO LIKE '%        %' OR MANZANA_CODIGO LIKE '%         %' OR MANZANA_CODIGO LIKE '%          %' OR MANZANA_CODIGO LIKE '%           %' OR MANZANA_CODIGO LIKE '%            %' OR MANZANA_CODIGO LIKE '%             %' OR MANZANA_CODIGO LIKE '%              %' OR MANZANA_CODIGO LIKE '%               %' OR MANZANA_CODIGO LIKE '%                %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                #("SUBSTRING(CODIGO, 22, 1) IN ('1','2', '5', '6')","Error: En condición de predio (código), debe ser diferente a 1, 2, 5 ó 6"),
                
                ("(substring (CODIGO,18,4) ='0000') OR (substring(CODIGO_ANTERIOR,14,4)='0000')","Error: En código  ( Predio = 0000)"),
                
                ("(substring(CODIGO,1,5) <> CODIGO_MUNICIPIO) OR (substring(CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO) OR (substring(MANZANA_CODIGO,1,5) <> CODIGO_MUNICIPIO)","Error: En código, código de manzana o código anterior, difieren a código municipio"),
                
                ("((SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '8' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '008') OR      (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '008'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '9' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '009') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '009'))))) OR (SUBSTRING(CODIGO, 22, 1) = '5')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '005') OR (SUBSTRING(CODIGO, 22, 1) = '2')  OR (SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '002') OR ((SUBSTRING(CODIGO, 22, 1) = '2' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '002') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '002'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '3' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '003') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '003'))))) OR ((SUBSTRING(CODIGO, 22, 1) = '4' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '004') OR  (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '004'))))) OR (    (SUBSTRING(CODIGO, 22, 1) = '7' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '007') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000')  OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || '007')))))","Error: En código (ultimas 4 posiciones) ó Codigo_Anterior Mal Codificado")

            ],
            "U_CONSTRUCCION": [
                
                
                ("CODIGO IS null or TERRENO_CODIGO is null or TIPO_CONSTRUCCION is null or NUMERO_PISOS is null or NUMERO_SOTANOS is null or NUMERO_MEZANINES is null or NUMERO_SEMISOTANOS is null or CODIGO_ANTERIOR is null OR CODIGO_MUNICIPIO is null","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like 'Null' or TERRENO_CODIGO like 'Null' or TIPO_CONSTRUCCION like 'Null' or  ETIQUETA like 'Null'  or CODIGO_ANTERIOR like 'Null' OR CODIGO_MUNICIPIO like 'Null'","Error: Atributos con valor falso 'null'"),
                ("CODIGO like ' *' or TERRENO_CODIGO like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *'  or CODIGO_ANTERIOR like ' *' or CODIGO_MUNICIPIO like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CHAR_LENGTH(CODIGO) <> 30 or CHAR_LENGTH(TERRENO_CODIGO) <> 30 or CHAR_LENGTH(CODIGO_ANTERIOR) <> 20 OR NUMERO_PISOS <= 0 OR NUMERO_SOTANOS < 0 OR NUMERO_MEZANINES < 0 OR NUMERO_SEMISOTANOS < 0 or CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '' or TERRENO_CODIGO like '' or TIPO_CONSTRUCCION like '' or CODIGO_ANTERIOR like '' OR CODIGO_MUNICIPIO like ''OR NUMERO_PISOS is null  OR NUMERO_SOTANOS is null OR NUMERO_MEZANINES is null OR NUMERO_SEMISOTANOS is null OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_ANTERIOR LIKE '% %' OR CODIGO_ANTERIOR LIKE '%  %' OR CODIGO_ANTERIOR LIKE '%   %' OR CODIGO_ANTERIOR LIKE '%    %' OR CODIGO_ANTERIOR LIKE '%     %' OR CODIGO_ANTERIOR LIKE '%      %' OR CODIGO_ANTERIOR LIKE '%       %' OR CODIGO_ANTERIOR LIKE '%        %' OR CODIGO_ANTERIOR LIKE '%         %' OR CODIGO_ANTERIOR LIKE '%          %' OR CODIGO_ANTERIOR LIKE '%           %' OR CODIGO_ANTERIOR LIKE '%            %' OR CODIGO_ANTERIOR LIKE '%             %' OR CODIGO_ANTERIOR LIKE '%              %' OR CODIGO_ANTERIOR LIKE '%               %' OR CODIGO_ANTERIOR LIKE '%                %' OR CODIGO_ANTERIOR LIKE '%                 %' OR CODIGO_ANTERIOR LIKE '%                  %' OR CODIGO_ANTERIOR LIKE '%                   %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                #("substring(CODIGO, 22, 1) IN ('1', '2', '6') OR substring(TERRENO_CODIGO, 22, 1) IN ('1', '2', '5', '6')","Error: En condición de predio código o código terreno, deben diferir de 1,2,6 (codigo) y/o 5 si es terreno_codigo."),
                ("( substring ( CODIGO,18,4) = '0000'  ) or ( substring ( TERRENO_CODIGO ,18,4) = '0000'  ) or ( substring ( CODIGO_ANTERIOR ,14,4) = '0000'  )","Error: En código de terreno o código ( Predio = 0000)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring ( TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring ( CODIGO_ANTERIOR ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, código o código anterior, difieren a código municipio"),

                ("((CODIGO <> TERRENO_CODIGO AND SUBSTRING(Codigo, 22, 1) = '0') OR (SUBSTRING(CODIGO, 22, 1) = '0' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000')))) OR (CODIGO <> TERRENO_CODIGO AND SUBSTRING(Codigo, 22, 1) = '8') OR (SUBSTRING(CODIGO, 22, 1) = '8' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) < '800' OR SUBSTRING(CODIGO_ANTERIOR, 18, 3) >'899') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 4) || SUBSTRING(CODIGO, 27, 4) || SUBSTRING(CODIGO, 19, 3))))) OR (((SUBSTRING(CODIGO,1,21) || SUBSTRING(CODIGO,25,6)) <> (SUBSTRING(TERRENO_CODIGO,1,21) || SUBSTRING(TERRENO_CODIGO,25,6))) AND SUBSTRING(Codigo, 22, 1) = '9') OR (SUBSTRING(CODIGO, 22, 1) = '9' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) < '900') OR (SUBSTRING(CODIGO, 25, 6) <> '000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || SUBSTRING(CODIGO, 19, 3))))) OR (CODIGO = TERRENO_CODIGO AND SUBSTRING(Codigo, 22, 1) = '5') OR (SUBSTRING(CODIGO, 22, 1) = '5' AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) = '000') OR (SUBSTRING(CODIGO, 23, 4) <> '0000') OR (SUBSTRING(CODIGO, 27, 4) = '0000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || SUBSTRING(CODIGO, 28, 3))))) OR (SUBSTRING(CODIGO, 22, 1) NOT IN ('0','5','8','9') AND ((SUBSTRING(CODIGO_ANTERIOR, 18, 3) <> '000') OR (SUBSTRING(CODIGO, 23, 8) <> '00000000') OR (CODIGO_ANTERIOR <> (SUBSTRING(CODIGO, 1, 9) || SUBSTRING(CODIGO, 14, 8) || '000')))))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o codigo anterior."),
                ("substring(CODIGO,6,2) =  '00' OR substring( TERRENO_CODIGO ,6,2) =  '00'  OR substring( CODIGO_ANTERIOR ,6,2) = '00'","Error: En código de terreno, código o código anterior (posiciones 6 Y 7 La Zona NO debe ser igual a 00)")


            ],
            "U_UNIDAD": [
                
                ("substring(CODIGO ,6,2) = '00' OR substring(TERRENO_CODIGO ,6,2) = '00' OR substring(CONSTRUCCION_CODIGO ,6,2) = '00'","Error: En código, código de terreno o código de construcción (La Zona NO debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring(TERRENO_CODIGO,1,5) <> CODIGO_MUNICIPIO OR substring( CONSTRUCCION_CODIGO ,1,5) <> CODIGO_MUNICIPIO","Error: En código, código de terreno o código de construcción difieren a código municipio"),
                #("substring(CONSTRUCCION_CODIGO, 22, 1) IN ('1', '2', '6') OR substring(CODIGO, 22, 1) IN ('1', '2', '6') OR substring(TERRENO_CODIGO, 22, 1) IN ('1', '2', '5', '6')","Error: En condición de predio no permitido. En código, código terreno o código construcción"),
                ("(substring(CODIGO,25,2)>'00' AND substring(CODIGO,22,1)<>'9') OR (substring(CODIGO,22,1)='9' AND ((substring(CODIGO,25,2)='01' AND (PLANTA<>'Piso 1' AND PLANTA<>'PS-01')) OR (substring(CODIGO,25,2)='02' AND (PLANTA<>'Piso 2' AND PLANTA<>'PS-02')) OR (substring(CODIGO,25,2)='03' AND (PLANTA<>'Piso 3' AND PLANTA<>'PS-03')) OR (substring(CODIGO,25,2)='04' AND (PLANTA<>'Piso 4' AND PLANTA<>'PS-04')) OR (substring(CODIGO,25,2)='05' AND (PLANTA<>'Piso 5' AND PLANTA<>'PS-05')) OR (substring(CODIGO,25,2)='06' AND (PLANTA<>'Piso 6' AND PLANTA<>'PS-06')) OR (substring(CODIGO,25,2)='07' AND (PLANTA<>'Piso 7' AND PLANTA<>'PS-07')) OR (substring(CODIGO,25,2)='08' AND (PLANTA<>'Piso 8' AND PLANTA<>'PS-08')) OR (substring(CODIGO,25,2)='09' AND (PLANTA<>'Piso 9' AND PLANTA<>'PS-09')) OR (substring(CODIGO,25,2)='10' AND (PLANTA<>'Piso 10' AND PLANTA<>'PS-10')) OR (substring(CODIGO,25,2)='11' AND (PLANTA<>'Piso 11' AND PLANTA<>'PS-11')) OR (substring(CODIGO,25,2)='12' AND (PLANTA<>'Piso 12' AND PLANTA<>'PS-12')) OR (substring(CODIGO,25,2)='13' AND (PLANTA<>'Piso 13' AND PLANTA<>'PS-13')) OR (substring(CODIGO,25,2)='14' AND (PLANTA<>'Piso 14' AND PLANTA<>'PS-14')) OR (substring(CODIGO,25,2)='15' AND (PLANTA<>'Piso 15' AND PLANTA<>'PS-15')) OR (substring(CODIGO,25,2)='16' AND (PLANTA<>'Piso 16' AND PLANTA<>'PS-16')) OR (substring(CODIGO,25,2)='17' AND (PLANTA<>'Piso 17' AND PLANTA<>'PS-17')) OR (substring(CODIGO,25,2)='18' AND (PLANTA<>'Piso 18' AND PLANTA<>'PS-18')) OR (substring(CODIGO,25,2)='19' AND (PLANTA<>'Piso 19' AND PLANTA<>'PS-19')) OR (substring(CODIGO,25,2)='20' AND (PLANTA<>'Piso 20' AND PLANTA<>'PS-20')) OR (substring(CODIGO,25,2)='21' AND (PLANTA<>'Piso 21' AND PLANTA<>'PS-21')) OR (substring(CODIGO,25,2)='22' AND (PLANTA<>'Piso 22' AND PLANTA<>'PS-22')) OR (substring(CODIGO,25,2)='23' AND (PLANTA<>'Piso 23' AND PLANTA<>'PS-23')) OR (substring(CODIGO,25,2)='24' AND (PLANTA<>'Piso 24' AND PLANTA<>'PS-24')) OR (substring(CODIGO,25,2)='25' AND (PLANTA<>'Piso 25' AND PLANTA<>'PS-25')) OR (substring(CODIGO,25,2)='26' AND (PLANTA<>'Piso 26' AND PLANTA<>'PS-26')) OR (substring(CODIGO,25,2)='27' AND (PLANTA<>'Piso 27' AND PLANTA<>'PS-27')) OR (substring(CODIGO,25,2)='28' AND (PLANTA<>'Piso 28' AND PLANTA<>'PS-28')) OR (substring(CODIGO,25,2)='29' AND (PLANTA<>'Piso 29' AND PLANTA<>'PS-29')) OR (substring(CODIGO,25,2)='30' AND (PLANTA<>'Piso 30' AND PLANTA<>'PS-30')) OR (substring(CODIGO,25,2)='31' AND (PLANTA<>'Piso 31' AND PLANTA<>'PS-31')) OR (substring(CODIGO,25,2)='32' AND (PLANTA<>'Piso 32' AND PLANTA<>'PS-32')) OR (substring(CODIGO,25,2)='33' AND (PLANTA<>'Piso 33' AND PLANTA<>'PS-33')) OR (substring(CODIGO,25,2)='34' AND (PLANTA<>'Piso 34' AND PLANTA<>'PS-34')) OR (substring(CODIGO,25,2)='35' AND (PLANTA<>'Piso 35' AND PLANTA<>'PS-35')) OR (substring(CODIGO,25,2)='36' AND (PLANTA<>'Piso 36' AND PLANTA<>'PS-36')) OR (substring(CODIGO,25,2)='37' AND (PLANTA<>'Piso 37' AND PLANTA<>'PS-37')) OR (substring(CODIGO,25,2)='38' AND (PLANTA<>'Piso 38' AND PLANTA<>'PS-38')) OR (substring(CODIGO,25,2)='39' AND (PLANTA<>'Piso 39' AND PLANTA<>'PS-39')) OR (substring(CODIGO,25,2)='40' AND (PLANTA<>'Piso 40' AND PLANTA<>'PS-40')) OR (substring(CODIGO,25,2)='41' AND (PLANTA<>'Piso 41' AND PLANTA<>'PS-41')) OR (substring(CODIGO,25,2)='42' AND (PLANTA<>'Piso 42' AND PLANTA<>'PS-42')) OR (substring(CODIGO,25,2)='43' AND (PLANTA<>'Piso 43' AND PLANTA<>'PS-43')) OR (substring(CODIGO,25,2)='44' AND (PLANTA<>'Piso 44' AND PLANTA<>'PS-44')) OR (substring(CODIGO,25,2)='45' AND (PLANTA<>'Piso 45' AND PLANTA<>'PS-45')) OR (substring(CODIGO,25,2)='46' AND (PLANTA<>'Piso 46' AND PLANTA<>'PS-46')) OR (substring(CODIGO,25,2)='47' AND (PLANTA<>'Piso 47' AND PLANTA<>'PS-47')) OR (substring(CODIGO,25,2)='48' AND (PLANTA<>'Piso 48' AND PLANTA<>'PS-48')) OR (substring(CODIGO,25,2)='49' AND (PLANTA<>'Piso 49' AND PLANTA<>'PS-49')) OR (substring(CODIGO,25,2)='50' AND (PLANTA<>'Piso 50' AND PLANTA<>'PS-50')) OR (substring(CODIGO,25,2)='51' AND (PLANTA<>'Piso 51' AND PLANTA<>'PS-51')) OR (substring(CODIGO,25,2)='52' AND (PLANTA<>'Piso 52' AND PLANTA<>'PS-52')) OR (substring(CODIGO,25,2)='53' AND (PLANTA<>'Piso 53' AND PLANTA<>'PS-53')) OR (substring(CODIGO,25,2)='54' AND (PLANTA<>'Piso 54' AND PLANTA<>'PS-54')) OR (substring(CODIGO,25,2)='55' AND (PLANTA<>'Piso 55' AND PLANTA<>'PS-55')) OR (substring(CODIGO,25,2)='56' AND (PLANTA<>'Piso 56' AND PLANTA<>'PS-56')) OR (substring(CODIGO,25,2)='57' AND (PLANTA<>'Piso 57' AND PLANTA<>'PS-57')) OR (substring(CODIGO,25,2)='58' AND (PLANTA<>'Piso 58' AND PLANTA<>'PS-58')) OR (substring(CODIGO,25,2)='59' AND (PLANTA<>'Piso 59' AND PLANTA<>'PS-59')) OR (substring(CODIGO,25,2)='60' AND (PLANTA<>'Piso 60' AND PLANTA<>'PS-60')) OR (substring(CODIGO,25,2)='61' AND (PLANTA<>'Piso 61' AND PLANTA<>'PS-61')) OR (substring(CODIGO,25,2)='62' AND (PLANTA<>'Piso 62' AND PLANTA<>'PS-62')) OR (substring(CODIGO,25,2)='63' AND (PLANTA<>'Piso 63' AND PLANTA<>'PS-63')) OR (substring(CODIGO,25,2)='64' AND (PLANTA<>'Piso 64' AND PLANTA<>'PS-64')) OR (substring(CODIGO,25,2)='65' AND (PLANTA<>'Piso 65' AND PLANTA<>'PS-65')) OR (substring(CODIGO,25,2)='66' AND (PLANTA<>'Piso 66' AND PLANTA<>'PS-66')) OR (substring(CODIGO,25,2)='67' AND (PLANTA<>'Piso 67' AND PLANTA<>'PS-67')) OR (substring(CODIGO,25,2)='68' AND (PLANTA<>'Piso 68' AND PLANTA<>'PS-68')) OR (substring(CODIGO,25,2)='69' AND (PLANTA<>'Piso 69' AND PLANTA<>'PS-69')) OR (substring(CODIGO,25,2)='70' AND (PLANTA<>'Piso 70' AND PLANTA<>'PS-70')) OR (substring(CODIGO,25,2)='71' AND (PLANTA<>'Piso 71' AND PLANTA<>'PS-71')) OR (substring(CODIGO,25,2)='72' AND (PLANTA<>'Piso 72' AND PLANTA<>'PS-72')) OR (substring(CODIGO,25,2)='73' AND (PLANTA<>'Piso 73' AND PLANTA<>'PS-73')) OR (substring(CODIGO,25,2)='74' AND (PLANTA<>'Piso 74' AND PLANTA<>'PS-74')) OR (substring(CODIGO,25,2)='75' AND (PLANTA<>'Piso 75' AND PLANTA<>'PS-75')) OR (substring(CODIGO,25,2)='76' AND (PLANTA<>'Piso 76' AND PLANTA<>'PS-76')) OR (substring(CODIGO,25,2)='77' AND (PLANTA<>'Piso 77' AND PLANTA<>'PS-77')) OR (substring(CODIGO,25,2)='78' AND (PLANTA<>'Piso 78' AND PLANTA<>'PS-78')) OR (substring(CODIGO,25,2)='79' AND (PLANTA<>'Piso 79' AND PLANTA<>'PS-79')) OR (substring(CODIGO,25,2)='80' AND (PLANTA<>'Piso 80' AND PLANTA<>'PS-80')) OR (substring(CODIGO,25,2)='99' AND PLANTA<>'ST-01') OR (substring(CODIGO,25,2)='98' AND PLANTA<>'ST-02') OR (substring(CODIGO,25,2)='97' AND PLANTA<>'ST-03') OR (substring(CODIGO,25,2)='96' AND PLANTA<>'ST-04') OR (substring(CODIGO,25,2)='95' AND PLANTA<>'ST-05') OR (substring(CODIGO,25,2)='94' AND PLANTA<>'ST-06') OR (substring(CODIGO,25,2)='93' AND PLANTA<>'ST-07') OR (substring(CODIGO,25,2)='92' AND PLANTA<>'ST-08') OR (substring(CODIGO,25,2)='91' AND PLANTA<>'ST-09') OR (substring(CODIGO,25,2)='90' AND PLANTA<>'ST-10') OR (substring(CODIGO,25,2)='89' AND PLANTA<>'ST-11') OR (substring(CODIGO,25,2)='88' AND PLANTA<>'ST-12') OR (substring(CODIGO,25,2)='87' AND PLANTA<>'ST-13') OR (substring(CODIGO,25,2)='86' AND PLANTA<>'ST-14') OR (substring(CODIGO,25,2)='85' AND PLANTA<>'ST-15') OR (substring(CODIGO,25,2)='84' AND PLANTA<>'ST-16') OR (substring(CODIGO,25,2)='83' AND PLANTA<>'ST-17') OR (substring(CODIGO,25,2)='82' AND PLANTA<>'ST-18') OR (substring(CODIGO,25,2)='81' AND PLANTA<>'ST-19')))","Error: En código (Posiciones 25 y 26 - Piso)"),

                ("(substring ( CODIGO ,18,4) = '0000') or ( substring ( TERRENO_CODIGO ,18,4)='0000') or ( substring ( CONSTRUCCION_CODIGO ,18,4) = '0000')","Error: En código de terreno, código de construcción o código ( Predio = 0000)"),

                ("(SUBSTRING(CODIGO, 22, 1) NOT IN ('5', '8','9') AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CODIGO, 23, 8) <> '00000000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '8' AND (SUBSTRING(TERRENO_CODIGO, 23, 4) <> '0000' OR SUBSTRING(TERRENO_CODIGO, 27, 4) = '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 4) <> '0000' OR SUBSTRING(CONSTRUCCION_CODIGO, 27, 4) = '0000' OR SUBSTRING(CODIGO, 23, 4) <> '0000' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO <> TERRENO_CODIGO OR CODIGO <> CONSTRUCCION_CODIGO)) OR (SUBSTRING(CODIGO, 22, 1) = '9' AND (SUBSTRING(TERRENO_CODIGO, 23, 8) <> '00000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 25, 6) <> '000000' OR SUBSTRING(CONSTRUCCION_CODIGO, 23, 2) = '00' OR SUBSTRING(CODIGO, 23, 2) = '00' OR SUBSTRING(CODIGO, 25, 2) = '00' OR SUBSTRING(CODIGO, 27, 4) = '0000' OR CODIGO = TERRENO_CODIGO OR CODIGO = CONSTRUCCION_CODIGO OR (SUBSTRING(CODIGO,1,21) <> SUBSTRING(TERRENO_CODIGO,1,21) OR SUBSTRING(CODIGO,1,21) <> SUBSTRING(CONSTRUCCION_CODIGO,1,21))))","Error: En codificación, según condición del predio, en las ultimas 8 o 4 posiciones, ó discrepancias entre código, código de terreno y/o codigo construccion"),



                ("CODIGO LIKE '' or TERRENO_CODIGO LIKE '' or CONSTRUCCION_CODIGO like '' or PLANTA like '' or TIPO_CONSTRUCCION like '' or (SUBSTRING(CODIGO, 22, 1) = '9' AND (ETIQUETA like ' ' OR  ETIQUETA like '' OR ETIQUETA IS NULL)) or IDENTIFICADOR like '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO LIKE '%        %' OR CODIGO LIKE '%         %' OR CODIGO LIKE '%          %' OR CODIGO LIKE '%           %' OR CODIGO LIKE '%            %' OR CODIGO LIKE '%             %' OR CODIGO LIKE '%              %' OR CODIGO LIKE '%               %' OR CODIGO LIKE '%                %' OR CODIGO LIKE '%                 %' OR CODIGO LIKE '%                  %' OR CODIGO LIKE '%                   %' OR CODIGO LIKE '%                    %' OR CODIGO LIKE '%                     %' OR CODIGO LIKE '%                      %' OR CODIGO LIKE '%                       %' OR CODIGO LIKE '%                        %' OR CODIGO LIKE '%                         %' OR CODIGO LIKE '%                          %' OR CODIGO LIKE '%                           %' OR CODIGO LIKE '%                            %' OR CODIGO LIKE '%                             %' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CONSTRUCCION_CODIGO LIKE '% %' OR CONSTRUCCION_CODIGO LIKE '%  %' OR CONSTRUCCION_CODIGO LIKE '%   %' OR CONSTRUCCION_CODIGO LIKE '%    %' OR CONSTRUCCION_CODIGO LIKE '%     %' OR CONSTRUCCION_CODIGO LIKE '%      %' OR CONSTRUCCION_CODIGO LIKE '%       %' OR CONSTRUCCION_CODIGO LIKE '%        %' OR CONSTRUCCION_CODIGO LIKE '%         %' OR CONSTRUCCION_CODIGO LIKE '%          %' OR CONSTRUCCION_CODIGO LIKE '%           %' OR CONSTRUCCION_CODIGO LIKE '%            %' OR CONSTRUCCION_CODIGO LIKE '%             %' OR CONSTRUCCION_CODIGO LIKE '%              %' OR CONSTRUCCION_CODIGO LIKE '%               %' OR CONSTRUCCION_CODIGO LIKE '%                %' OR CONSTRUCCION_CODIGO LIKE '%                 %' OR CONSTRUCCION_CODIGO LIKE '%                  %' OR CONSTRUCCION_CODIGO LIKE '%                   %' OR CONSTRUCCION_CODIGO LIKE '%                    %' OR CONSTRUCCION_CODIGO LIKE '%                     %' OR CONSTRUCCION_CODIGO LIKE '%                      %' OR CONSTRUCCION_CODIGO LIKE '%                       %' OR CONSTRUCCION_CODIGO LIKE '%                        %' OR CONSTRUCCION_CODIGO LIKE '%                         %' OR CONSTRUCCION_CODIGO LIKE '%                          %' OR CONSTRUCCION_CODIGO LIKE '%                           %' OR CONSTRUCCION_CODIGO LIKE '%                            %' OR CONSTRUCCION_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),

                ("CODIGO LIKE ' *' or TERRENO_CODIGO LIKE ' *' or CONSTRUCCION_CODIGO like ' *' or PLANTA like ' *' or TIPO_CONSTRUCCION like ' *' or ETIQUETA like ' *' or IDENTIFICADOR like ' *' OR CODIGO_MUNICIPIO  like ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO IS NULL OR TERRENO_CODIGO IS NULL OR CONSTRUCCION_CODIGO IS NULL OR PLANTA IS NULL OR TIPO_CONSTRUCCION IS NULL OR IDENTIFICADOR IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),

                ("CHAR_LENGTH (CODIGO) <> 30 or CHAR_LENGTH (TERRENO_CODIGO) <> 30 or CHAR_LENGTH ( CONSTRUCCION_CODIGO) <> 30 or ( IDENTIFICADOR LIKE '*/*' or CHAR_LENGTH (IDENTIFICADOR) <> 1)  OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("CODIGO like '*null*' or TERRENO_CODIGO like '*null*' or CONSTRUCCION_CODIGO like '*null*' or PLANTA like '*null*' or TIPO_CONSTRUCCION like '*null*' or ETIQUETA like '*null*' or IDENTIFICADOR like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'")
  
            ],
            "U_NOMENCLATURA_DOMICILIARIA":[
                
                ("substring (TERRENO_CODIGO ,1,5) <> CODIGO_MUNICIPIO","Error: En código de terreno, difieren a código municipio"),
                ("TEXTO IS NULL or TERRENO_CODIGO IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("TEXTO LIKE'' OR TERRENO_CODIGO LIKE'' OR CODIGO_MUNICIPIO LIKE'' OR TERRENO_CODIGO LIKE '% %' OR TERRENO_CODIGO LIKE '%  %' OR TERRENO_CODIGO LIKE '%   %' OR TERRENO_CODIGO LIKE '%    %' OR TERRENO_CODIGO LIKE '%     %' OR TERRENO_CODIGO LIKE '%      %' OR TERRENO_CODIGO LIKE '%       %' OR TERRENO_CODIGO LIKE '%        %' OR TERRENO_CODIGO LIKE '%         %' OR TERRENO_CODIGO LIKE '%          %' OR TERRENO_CODIGO LIKE '%           %' OR TERRENO_CODIGO LIKE '%            %' OR TERRENO_CODIGO LIKE '%             %' OR TERRENO_CODIGO LIKE '%              %' OR TERRENO_CODIGO LIKE '%               %' OR TERRENO_CODIGO LIKE '%                %' OR TERRENO_CODIGO LIKE '%                 %' OR TERRENO_CODIGO LIKE '%                  %' OR TERRENO_CODIGO LIKE '%                   %' OR TERRENO_CODIGO LIKE '%                    %' OR TERRENO_CODIGO LIKE '%                     %' OR TERRENO_CODIGO LIKE '%                      %' OR TERRENO_CODIGO LIKE '%                       %' OR TERRENO_CODIGO LIKE '%                        %' OR TERRENO_CODIGO LIKE '%                         %' OR TERRENO_CODIGO LIKE '%                          %' OR TERRENO_CODIGO LIKE '%                           %' OR TERRENO_CODIGO LIKE '%                            %' OR TERRENO_CODIGO LIKE '%                             %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("TEXTO LIKE '*_*' or CHAR_LENGTH( TERRENO_CODIGO ) <> 30 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: Formato de atributos, no cumplen longitud correcta"),
                ("TEXTO like '*null*'  or TERRENO_CODIGO like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor falso 'null'"),
                ("SUBSTRING (TERRENO_CODIGO ,6, 2) = '00'","Error: Código de terreno (zona NO debe ser 00)"),
                ("TEXTO LIKE ' *' or TERRENO_CODIGO LIKE ' *'","Error: Valor de atributo comienza con espacio en blanco")
    
            ],
            "U_NOMENCLATURA_VIAL":[
                
                ("TEXTO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("TEXTO IS null","Error: Atributos con valor 'null' ó en Blanco"),
                ("TEXTO LIKE '*null*'","Error: Atributos con valor falso 'null'"),
                ("TEXTO LIKE ''","Error: Atributos con valor en blanco")

            ]
            
            
            },
        "ZONA_HOMOGENEA_RURAL_CTM12": {
            "R_ZONA_HOMOGENEA_FISICA_CTM12":[
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, difiere a código municipio"),
                ("CODIGO LIKE '' OR CODIGO_ZONA_FISICA LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR AREA_HOMOGENEA_TIERRA LIKE ''OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' OR CODIGO_ZONA_FISICA LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' OR  AREA_HOMOGENEA_TIERRA LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO IS NULL OR CODIGO_ZONA_FISICA IS NULL OR CODIGO_MUNICIPIO IS NULL  OR AREA_HOMOGENEA_TIERRA IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like '*null*' OR CODIGO_ZONA_FISICA like '*null*' OR CODIGO_MUNICIPIO like '*null*'  OR AREA_HOMOGENEA_TIERRA like '*null*'","Error: Atributos con valor falso 'null'"),
                ("CHAR_LENGTH(CODIGO) <> 7  OR CHAR_LENGTH(CODIGO_ZONA_FISICA) = 0 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("substring( CODIGO ,6,2) <>  '00'","Error: En código,  (La Zona debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, difiere a código municipio")
                   
            ],
            "R_ZONA_HOMO_GEOECONOMICA_CTM12":[
                ("CODIGO LIKE '' OR CODIGO_ZONA_GEOECONOMICA LIKE '' OR VALOR_HECTAREA LIKE '' OR SUBZONA_FISICA LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' OR CODIGO_ZONA_GEOECONOMICA LIKE ' *' OR VALOR_HECTAREA LIKE ' *' OR SUBZONA_FISICA LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO like '*null*' OR CODIGO_ZONA_GEOECONOMICA like '*null*' OR VALOR_HECTAREA like '*null*' OR SUBZONA_FISICA like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO IS NULL OR CODIGO_ZONA_GEOECONOMICA IS NULL OR VALOR_HECTAREA IS NULL OR SUBZONA_FISICA IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CHAR_LENGTH(CODIGO) <> 7 OR CHAR_LENGTH(CODIGO_ZONA_GEOECONOMICA) < 1 OR CHAR_LENGTH(VALOR_HECTAREA) <2 OR CHAR_LENGTH(SUBZONA_FISICA) <1 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5 OR CAST(CODIGO_ZONA_GEOECONOMICA AS FLOAT)< 1","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("substring( CODIGO ,6,2) <>  '00'","Error: En código,  (La Zona debe ser igual a 00)"),
                ("CAST(VALOR_HECTAREA AS FLOAT) = 0 OR CAST(VALOR_HECTAREA AS FLOAT) < 1","Error: El valor de Hectaria no puede ser 0"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, difiere a código municipio")
                    
            ]
                   
            },
        "ZONA_HOMOGENEA_URBANO_CTM12": {
            "U_ZONA_HOMOGENEA_FISICA_CTM12":[
                ("CODIGO LIKE '' OR CODIGO_ZONA_FISICA LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR TOPOGRAFIA LIKE '' OR NORMA_USO_SUELO LIKE ''OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' OR CODIGO_ZONA_FISICA LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' OR TOPOGRAFIA LIKE ' *' OR NORMA_USO_SUELO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO like '*null*' OR CODIGO_ZONA_FISICA like '*null*' OR CODIGO_MUNICIPIO like '*null*' OR TOPOGRAFIA like '*null*' OR NORMA_USO_SUELO like '*null*'","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO IS NULL OR CODIGO_ZONA_FISICA IS NULL OR CODIGO_MUNICIPIO IS NULL OR TOPOGRAFIA IS NULL OR NORMA_USO_SUELO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CHAR_LENGTH(CODIGO) <> 7  OR CHAR_LENGTH(CODIGO_ZONA_FISICA) = 0 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("substring( CODIGO ,6,2) =  '00'","Error: En código,  (La Zona NO debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, difiere a código municipio")
                
            ],
            "U_ZONA_HOMO_GEOECONOMICA_CTM12":[
                ("CODIGO LIKE '' OR CODIGO_ZONA_GEOECONOMICA LIKE '' OR SUBZONA_FISICA LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' OR CODIGO_ZONA_GEOECONOMICA LIKE ' *' OR SUBZONA_FISICA LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO like '*null*' OR CODIGO_ZONA_GEOECONOMICA like '*null*' OR SUBZONA_FISICA like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO IS NULL OR CODIGO_ZONA_GEOECONOMICA IS NULL OR VALOR_METRO IS NULL OR SUBZONA_FISICA IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CHAR_LENGTH(CODIGO) <> 7 OR CHAR_LENGTH(CODIGO_ZONA_GEOECONOMICA) < 1 OR (VALOR_METRO) <1 OR CHAR_LENGTH(SUBZONA_FISICA) <1 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5 OR CAST(CODIGO_ZONA_GEOECONOMICA AS FLOAT)< 1","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("substring( CODIGO ,6,2) =  '00'","Error: En código,  (La Zona NO debe ser igual a 00)"),
                ("VALOR_METRO = 0 OR VALOR_METRO < 1","Error: El valor de Hectaria no puede ser 0")

            ]
            
            },
        "ZONA_HOMOGENEA_RURAL": {
            "R_ZONA_HOMOGENEA_FISICA":[
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, difiere a código municipio"),
                ("CODIGO LIKE '' OR CODIGO_ZONA_FISICA LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR AREA_HOMOGENEA_TIERRA LIKE ''OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' OR CODIGO_ZONA_FISICA LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' OR  AREA_HOMOGENEA_TIERRA LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO IS NULL OR CODIGO_ZONA_FISICA IS NULL OR CODIGO_MUNICIPIO IS NULL  OR AREA_HOMOGENEA_TIERRA IS NULL","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO like '*null*' OR CODIGO_ZONA_FISICA like '*null*' OR CODIGO_MUNICIPIO like '*null*'  OR AREA_HOMOGENEA_TIERRA like '*null*'","Error: Atributos con valor falso 'null'"),
                ("CHAR_LENGTH(CODIGO) <> 7  OR CHAR_LENGTH(CODIGO_ZONA_FISICA) = 0 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("substring( CODIGO ,6,2) <>  '00'","Error: En código,  (La Zona debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, difiere a código municipio")
                   
            ],
            "R_ZONA_HOMOGENEA_GEOECONOMICA":[
                ("CODIGO LIKE '' OR CODIGO_ZONA_GEOECONOMICA LIKE '' OR VALOR_HECTAREA LIKE '' OR SUBZONA_FISICA LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' OR CODIGO_ZONA_GEOECONOMICA LIKE ' *' OR VALOR_HECTAREA LIKE ' *' OR SUBZONA_FISICA LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO like '*null*' OR CODIGO_ZONA_GEOECONOMICA like '*null*' OR VALOR_HECTAREA like '*null*' OR SUBZONA_FISICA like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO IS NULL OR CODIGO_ZONA_GEOECONOMICA IS NULL OR VALOR_HECTAREA IS NULL OR SUBZONA_FISICA IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CHAR_LENGTH(CODIGO) <> 7 OR CHAR_LENGTH(CODIGO_ZONA_GEOECONOMICA) < 1 OR CHAR_LENGTH(VALOR_HECTAREA) <2 OR CHAR_LENGTH(SUBZONA_FISICA) <1 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5 OR CAST(CODIGO_ZONA_GEOECONOMICA AS FLOAT)< 1","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("substring( CODIGO ,6,2) <>  '00'","Error: En código,  (La Zona debe ser igual a 00)"),
                ("CAST(VALOR_HECTAREA AS FLOAT) = 0 OR CAST(VALOR_HECTAREA AS FLOAT) < 1","Error: El valor de Hectaria no puede ser 0"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, difiere a código municipio")
                    
            ]
                   
            },
        "ZONA_HOMOGENEA_URBANO": {
            "U_ZONA_HOMOGENEA_FISICA":[
                ("CODIGO LIKE '' OR CODIGO_ZONA_FISICA LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR TOPOGRAFIA LIKE '' OR NORMA_USO_SUELO LIKE ''OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' OR CODIGO_ZONA_FISICA LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *' OR TOPOGRAFIA LIKE ' *' OR NORMA_USO_SUELO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO like '*null*' OR CODIGO_ZONA_FISICA like '*null*' OR CODIGO_MUNICIPIO like '*null*' OR TOPOGRAFIA like '*null*' OR NORMA_USO_SUELO like '*null*'","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO IS NULL OR CODIGO_ZONA_FISICA IS NULL OR CODIGO_MUNICIPIO IS NULL OR TOPOGRAFIA IS NULL OR NORMA_USO_SUELO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CHAR_LENGTH(CODIGO) <> 7  OR CHAR_LENGTH(CODIGO_ZONA_FISICA) = 0 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("substring( CODIGO ,6,2) =  '00'","Error: En código,  (La Zona NO debe ser igual a 00)"),
                ("substring(CODIGO,1,5) <> CODIGO_MUNICIPIO","Error: En código, difiere a código municipio")
                
            ],
            "U_ZONA_HOMOGENEA_GEOECONOMICA":[
                ("CODIGO LIKE '' OR CODIGO_ZONA_GEOECONOMICA LIKE '' OR SUBZONA_FISICA LIKE '' OR CODIGO_MUNICIPIO LIKE '' OR CODIGO LIKE '% %' OR CODIGO LIKE '%  %' OR CODIGO LIKE '%   %' OR CODIGO LIKE '%    %' OR CODIGO LIKE '%     %' OR CODIGO LIKE '%      %' OR CODIGO LIKE '%       %' OR CODIGO_MUNICIPIO LIKE '% %' OR CODIGO_MUNICIPIO LIKE '%  %' OR CODIGO_MUNICIPIO LIKE '%   %' OR CODIGO_MUNICIPIO LIKE '%    %'","Error: Atributos con valor en blanco"),
                ("CODIGO LIKE ' *' OR CODIGO_ZONA_GEOECONOMICA LIKE ' *' OR SUBZONA_FISICA LIKE ' *' OR CODIGO_MUNICIPIO LIKE ' *'","Error: Atributo contiene espacio en blanco al comienzo de su valor"),
                ("CODIGO like '*null*' OR CODIGO_ZONA_GEOECONOMICA like '*null*' OR SUBZONA_FISICA like '*null*' OR CODIGO_MUNICIPIO like '*null*'","Error: Atributos con valor 'null' ó en Blanco"),
                ("CODIGO IS NULL OR CODIGO_ZONA_GEOECONOMICA IS NULL OR VALOR_METRO IS NULL OR SUBZONA_FISICA IS NULL OR CODIGO_MUNICIPIO IS NULL","Error: Atributos con valor falso 'null'"),
                ("CHAR_LENGTH(CODIGO) <> 7 OR CHAR_LENGTH(CODIGO_ZONA_GEOECONOMICA) < 1 OR (VALOR_METRO) <1 OR CHAR_LENGTH(SUBZONA_FISICA) <1 OR CHAR_LENGTH(CODIGO_MUNICIPIO) <> 5 OR CAST(CODIGO_ZONA_GEOECONOMICA AS FLOAT)< 1","Error: En formato de atributos, valores que no cumplen longitud correcta."),
                ("substring( CODIGO ,6,2) =  '00'","Error: En código,  (La Zona NO debe ser igual a 00)"),
                ("VALOR_METRO = 0 OR VALOR_METRO < 1","Error: El valor de Hectaria no puede ser 0")

            ]
            
        }
        
    }

def validate_environment():
    """Valida que el ambiente tenga todo lo necesario para ejecutar el script"""
    try:
        # Verificar que arcpy esté disponible
        import arcpy
        
        # Verificar licencias necesarias
        if not arcpy.CheckProduct("ArcInfo") == "Available":
            raise RuntimeError("Se requiere licencia ArcInfo para ejecutar este script")
            
        # Verificar extensiones necesarias
        if not arcpy.CheckExtension("Spatial") == "Available":
            raise RuntimeError("Se requiere la extensión Spatial Analyst")
            
        return True
        
    except ImportError:
        raise RuntimeError("No se pudo importar arcpy. Asegúrese de estar ejecutando desde ArcGIS Pro")
    except Exception as e:
        raise RuntimeError(f"Error validando el ambiente: {str(e)}")

def setup_logging():
    """Configura el sistema de logging"""
    import logging
    from datetime import datetime
    
    # Crear directorio de logs si no existe
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Configurar archivo de log con timestamp
    log_file = os.path.join(log_dir, f'validacion_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    # Configurar formato de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def encontrar_gdb(directorio):
    """Busca archivos .gdb en el directorio especificado"""
    for item in os.listdir(directorio):
        if item.endswith('.gdb'):
            return os.path.join(directorio, item)
    return None

def main():
    try:
        # Configurar logging
        logger = setup_logging()
        logger.info("Iniciando proceso de validación de geodatabase")
        
        # Obtener directorio del proyecto (dos niveles arriba del script)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        proyecto_dir = os.path.dirname(os.path.dirname(script_dir))
        
        # Definir rutas
        temp_dir = os.path.join(proyecto_dir,  "Files", "Temporary_Files","MODELO_IGAC")
        temp_dir_config = os.path.join(proyecto_dir,  "Files", "Temporary_Files")
        config_path = os.path.join(temp_dir_config, "array_config.txt")
        
        # Verificar que existan las rutas necesarias
        if not os.path.exists(temp_dir):
            raise FileNotFoundError(f"No se encontró el directorio temporal en: {temp_dir}")
            
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"No se encontró el archivo de configuración en: {config_path}")
        
        # Buscar la geodatabase
        input_gdb = encontrar_gdb(temp_dir)
        if not input_gdb:
            raise FileNotFoundError("No se encontró ninguna geodatabase (.gdb) en el directorio temporal")
            
        logger.info(f"Geodatabase encontrada: {input_gdb}")
        
        # Configurar directorio de salida
        output_folder = os.path.join(temp_dir, 'consistencia_formato_temp')
        os.makedirs(output_folder, exist_ok=True)
        
        # Cargar configuración de datasets
        active_datasets = []
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    dataset_name = line.strip('",')
                    if dataset_name:
                        active_datasets.append(dataset_name)
        
        if not active_datasets:
            logger.warning("No se encontraron datasets activos en el archivo de configuración")
            logger.info("Se usará la configuración por defecto")
            active_datasets = ["URBANO_CTM12", "RURAL_CTM12"]
        
        logger.info("Datasets a procesar:")
        for dataset in active_datasets:
            logger.info(f"  - {dataset}")
        
        # Crear y ejecutar el validador
        try:
            validator = GDBValidator(proyecto_dir)
            validator.datasets_to_process = active_datasets
            validator.input_gdb = input_gdb
            validator.output_folder = output_folder
            validator.output_gdb = os.path.join(output_folder, 
                                              f"{os.path.splitext(os.path.basename(input_gdb))[0]}_validacion.gdb")
            
            # Ejecutar la validación
            validator.run_validation()
            
            logger.info("Proceso completado exitosamente")
            
        except Exception as e:
            logger.error(f"Error durante la validación: {str(e)}")
            raise
        
    except Exception as e:
        logger.error("Error fatal durante la ejecución:")
        logger.error(str(e))
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
        
    finally:
        # Limpiar recursos
        try:
            arcpy.Delete_management("in_memory")
        except:
            pass

if __name__ == "__main__":
    main()