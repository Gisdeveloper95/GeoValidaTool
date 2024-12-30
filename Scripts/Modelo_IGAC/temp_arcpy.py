import arcpy
import os

def analyze_geometries(original_gdb, validation_gdb, dataset_name="URBANO_CTM12"):
    """
    Analiza las geometrías enfocándose solo en casos problemáticos
    """
    print("\nANÁLISIS DE GEOMETRÍAS PROBLEMÁTICAS")
    print("====================================")

    arcpy.env.workspace = original_gdb
    feature_classes = arcpy.ListFeatureClasses(feature_dataset=dataset_name)
    if not feature_classes:
        with arcpy.EnvManager(workspace=os.path.join(original_gdb, dataset_name)):
            feature_classes = arcpy.ListFeatureClasses()

    for fc in feature_classes:
        print(f"\nAnalizando {fc}...")
        fc_path = os.path.join(original_gdb, dataset_name, fc)
        validation_fc = os.path.join(validation_gdb, dataset_name, fc)
        
        if not arcpy.Exists(validation_fc):
            continue

        # Obtener áreas originales
        original_areas = {}
        with arcpy.da.SearchCursor(fc_path, ['OID@', 'SHAPE@AREA', 'CODIGO']) as cursor:
            for row in cursor:
                if 'CODIGO' in [f.name for f in arcpy.ListFields(fc_path)]:
                    original_areas[row[2]] = {'oid': row[0], 'area': row[1] if row[1] else 0}
                else:
                    original_areas[row[0]] = {'oid': row[0], 'area': row[1] if row[1] else 0}

        # Comparar con validación
        problematic_count = 0
        total_validation = 0
        validation_fields = ['OID@', 'SHAPE@AREA', 'Error_Descripcion']
        if 'CODIGO' in [f.name for f in arcpy.ListFields(validation_fc)]:
            validation_fields.insert(2, 'CODIGO')

        with arcpy.da.SearchCursor(validation_fc, validation_fields) as cursor:
            for row in cursor:
                total_validation += 1
                has_codigo = len(validation_fields) > 3
                key = row[2] if has_codigo else row[0]
                
                if key in original_areas:
                    orig_area = original_areas[key]['area']
                    val_area = row[1] if row[1] else 0
                    
                    if val_area == 0 and orig_area > 0:
                        problematic_count += 1
                        error_desc = row[-1] if row[-1] else "No error description"
                        print(f"\nRegistro problemático encontrado:")
                        if has_codigo:
                            print(f"  Código: {key}")
                        print(f"  OID original: {original_areas[key]['oid']}")
                        print(f"  Área original: {orig_area}")
                        print(f"  Área en validación: {val_area}")
                        print(f"  Error reportado: {error_desc}")

        if problematic_count > 0:
            print(f"\nResumen para {fc}:")
            print(f"  Total registros en validación: {total_validation}")
            print(f"  Registros con área 0: {problematic_count}")
            print(f"  Porcentaje problemático: {(problematic_count/total_validation)*100:.2f}%")

if __name__ == "__main__":
    original_gdb = r"C:\Users\osori\Desktop\arreglando geovalidatool\GeoValidaTool\Files\Temporary_Files\MODELO_IGAC\50590_CONSOLIDADO_GENERAL.gdb"
    validation_gdb = r"C:\Users\osori\Desktop\arreglando geovalidatool\GeoValidaTool\Files\Temporary_Files\MODELO_IGAC\consistencia_formato_temp\50590_CONSOLIDADO_GENERAL_validacion.gdb"
    
    analyze_geometries(original_gdb, validation_gdb)