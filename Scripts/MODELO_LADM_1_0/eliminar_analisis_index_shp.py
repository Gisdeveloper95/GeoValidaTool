#!/usr/bin/env python
# -*- coding: utf-8 -*-
import arcpy
import os

def create_index_for_joins(feature_class, join_field):
    try:
        print "\nProcesando:", os.path.basename(feature_class)
        
        # Crear un nombre corto y único para el índice
        table_short = os.path.basename(feature_class)[:3]
        field_short = join_field[:3]
        index_name = "IX_%s_%s" % (table_short, field_short)
        
        # Verificar si el índice ya existe
        existing_indexes = [index.name for index in arcpy.ListIndexes(feature_class)]
        if index_name in existing_indexes:
            print "El indice %s ya existe" % index_name
            return
            
        # Crear el índice
        print "Creando indice %s para campo %s" % (index_name, join_field)
        arcpy.AddIndex_management(
            in_table=feature_class,
            fields=join_field,
            index_name=index_name,
            unique="NON_UNIQUE",
            ascending="NON_ASCENDING"
        )
        print "Indice creado exitosamente"
        
    except Exception as e:
        print "Error al crear indice:", str(e)
        print arcpy.GetMessages()

def main():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.dirname(os.path.dirname(script_dir))
        temp_files = os.path.join(base_dir, 'Files', 'Temporary_Files', 'MODELO_LADM_1_0')
        
        # Encontrar la geodatabase
        gdbs = [f for f in os.listdir(temp_files) if f.endswith('.gdb')]
        if not gdbs:
            print "No se encontró ninguna geodatabase"
            return
            
        gdb_path = os.path.join(temp_files, gdbs[0])
        print "Procesando geodatabase:", gdb_path
        
        # Definir las tablas y sus campos de join
        table_configs = {
            "R_UNIDAD_CTM12": ["FID", "GLOBALID"],
            "R_UNIDAD_INFORMAL": ["FID", "GLOBALID"],
            "U_UNIDAD_INFORMAL": ["FID", "GLOBALID"],
            "U_UNIDAD_CTM12": ["FID", "GLOBALID"]
        }
        
        # Procesar cada tabla
        for table, fields in table_configs.items():
            table_path = os.path.join(gdb_path, table)
            if arcpy.Exists(table_path):
                for field in fields:
                    create_index_for_joins(table_path, field)
            else:
                print "\nTabla no encontrada:", table
                
    except Exception as e:
        print "Error general:", str(e)
        print arcpy.GetMessages()

if __name__ == '__main__':
    print "Iniciando creacion de indices para operaciones de join..."
    main()
    print "\nProceso completado."