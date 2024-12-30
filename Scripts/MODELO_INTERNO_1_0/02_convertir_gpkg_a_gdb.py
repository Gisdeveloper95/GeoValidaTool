import arcpy
import os
from pathlib import Path


def migrate_gpkg_to_gdb():
    script_path = Path(__file__).resolve()
    geovalidatool_path = script_path.parent.parent.parent
    base_path = geovalidatool_path / "Files" / "Temporary_Files" / "MODELO_INTERNO_1_0"
    
    try:
        gpkg_files = list(base_path.glob("*.gpkg"))
        gdb_files = list(base_path.glob("*.gdb"))
        
        if not gpkg_files or not gdb_files:
            raise FileNotFoundError(f"No se encontraron archivos GPKG o GDB en {base_path}")
            
        gpkg_path = str(gpkg_files[0])
        gdb_path = str(gdb_files[0])
        
        print(f"GPKG: {gpkg_path}")
        print(f"GDB: {gdb_path}")
        
        arcpy.env.overwriteOutput = True
        
        arcpy.AddMessage("Migrando INFORMACION DEL GPKG A GDB")
        # 1. Predio formal rural
        arcpy.AddMessage("Procesando R_TERRENO_CTM12...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_predio"),
            out_layer="rpredio_formal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)<>'2' AND SUBSTR(numero_predial_nacional,6,2)='00'"
        )

        arcpy.management.RepairGeometry("rpredio_formal")
        arcpy.management.Append(
            inputs=["rpredio_formal"],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_TERRENO_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false false 30 Text 0 0,First,#,rpredio_formal,numero_predial_nacional,0,29;VEREDA_CODIGO \"Vereda_Codigo\" true true false 17 Text 0 0,First,#;CODIGO_ANTERIOR \"Codigo_Anterior\" true true false 20 Text 0 0,First,#;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;GLOBALID \"GLOBALID\" false false true 38 GlobalID 0 0,First,#;CODIGO_MUNICIPIO \"CODIGO_MUNICIPIO\" true true false 5 Text 0 0,First,#"
        )
        arcpy.management.Delete("rpredio_formal")

        # 2. Predio informal rural
        arcpy.AddMessage("Procesando R_TERRENO_INFORMAL...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_predio"),
            out_layer="rpredio_informal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)='2' AND SUBSTR(numero_predial_nacional,6,2)='00'"
        )

        arcpy.management.RepairGeometry("rpredio_informal")
        arcpy.management.Append(
            inputs=["rpredio_informal"],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_TERRENO_INFORMAL"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false false 30 Text 0 0,First,#,rpredio_informal,numero_predial_nacional,0,29;CODIGO_ANTERIOR \"Codigo_Anterior\" true true false 20 Text 0 0,First,#;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;GLOBALID \"GLOBALID\" false false true 38 GlobalID 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )
        arcpy.management.Delete("rpredio_informal")

        # 3. Corregimiento
        arcpy.AddMessage("Procesando R_CORREGIMIENTO_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_Corregimiento")],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_CORREGIMIENTO_CTM12"),
            schema_type="NO_TEST",
            field_mapping="codigo \"codigo\" true true false 7 Text 0 0,First,#,main.CC_Centro_Poblado,codigo,0,6,main.CC_Corregimiento,codigo,0,6;nombre \"nombre\" true true false 50 Text 0 0,First,#,main.CC_Centro_Poblado,nombre,0,49,main.CC_Corregimiento,nombre,0,49"
        )

        # 4. Barrio
        arcpy.AddMessage("Procesando U_BARRIO_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_barrio")],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_BARRIO_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false true 13 Text 0 0,First,#,main.CC_barrio,codigo,0,12;SECTOR_CODIGO \"Sector_Codigo\" true true false 9 Text 0 0,First,#,main.CC_barrio,codigo_sector,0,1;NOMBRE \"Nombre\" true false true 100 Text 0 0,First,#,main.CC_barrio,nombre,0,99"
        )

        # 5. Centro Poblado
        arcpy.AddMessage("Procesando R_CENTRO_POBLADO_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_Centro_Poblado")],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_CENTRO_POBLADO_CTM12"),
            schema_type="NO_TEST",
            field_mapping="codigo \"codigo\" true true false 7 Text 0 0,First,#,main.CC_Centro_Poblado,codigo,0,6;nombre \"nombre\" true true false 50 Text 0 0,First,#,main.CC_Centro_Poblado,nombre,0,49"
        )

        # 6. Dirección formal rural
        arcpy.AddMessage("Procesando R_DIRECCION_FORMAL_CTM12...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.Extdireccion"),
            out_layer="r_direccion_formal",
            where_clause="substr(numero_predial_nacional,22,1) <>'2' AND substr(numero_predial_nacional,6,2) = '00'"
        )

        arcpy.management.RepairGeometry("r_direccion_formal")
        arcpy.management.Append(
            inputs=["r_direccion_formal"],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_DIRECCION_FORMAL_CTM12"),
            schema_type="NO_TEST",
            field_mapping="numero_predial \"numero_predial\" true true false 30 Text 0 0,First,#,r_direccion_formal,numero_predial_nacional,0,29;numero_predial_anterior \"numero_predial_anterior\" true true false 255 Text 0 0,First,#"
        )
        arcpy.management.Delete("r_direccion_formal")





                # 7. Dirección informal rural
        arcpy.AddMessage("Procesando R_DIRECCION_INFORMAL_CTM12...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.Extdireccion"),
            out_layer="r_direccion_informal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)='2' AND SUBSTR(numero_predial_nacional,6,2)='00'"
        )

        arcpy.management.RepairGeometry("r_direccion_informal")
        arcpy.management.Append(
            inputs=["r_direccion_informal"],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_DIRECCION_INFORMAL_CTM12"),
            schema_type="NO_TEST",
            field_mapping="NUMERO_PREDIAL \"numero_predial\" true true false 30 Text 0 0,First,#,r_direccion_informal,numero_predial_nacional,0,29;NUMERO_PREDIAL_ANTERIOR \"numero_predial_anterior\" true true false 255 Text 0 0,First,#"
        )
        arcpy.management.Delete("r_direccion_informal")

        # 8. Límite Municipal
        arcpy.AddMessage("Procesando R_LIMITE_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_Limite_Municipio")],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_LIMITEMUN_CTM12"),
            schema_type="NO_TEST",
            field_mapping="codigo_departamento \"codigo_departamento\" true true false 2 Text 0 0,First,#,main.CC_Limite_Municipio,codigo_departamento,0,1;codigo_municipio \"codigo_municipio\" true true false 5 Text 0 0,First,#,main.CC_Limite_Municipio,codigo_municipio,0,4;nombre_municipio \"nombre_municipio\" true true false 255 Text 0 0,First,#,main.CC_Limite_Municipio,nombre_municipio,0,254;codigo_limite \"codigo_limite\" true true false 255 Text 0 0,First,#"
        )

        # 9. Localidad Comuna
        arcpy.AddMessage("Procesando U_LOCALIDADCOM_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_Localidad_comuna")],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_LOCALIDADCOM_CTM12"),
            schema_type="NO_TEST",
            field_mapping="""codigo_ "codigo" true true false 11 Text 0 0,First,#,main.CC_Localidad_comuna,codigo,0,10;nombre_ "nombre" true true false 50 Text 0 0,First,#,main.CC_Localidad_comuna,nombre,0,49"""
        )

        # 10. Manzana
        arcpy.AddMessage("Procesando U_MANZANA_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_manzana")],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_MANZANA_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false true 17 Text 0 0,First,#,main.CC_manzana,codigo,0,16;BARRIO_CODIGO \"Barrio_Codigo\" true true false 13 Text 0 0,First,#,main.CC_manzana,codigo_barrio,0,12;CODIGO_ANTERIOR \"Codigo_Anterior\" true true false 255 Text 0 0,First,#;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )

        # 11. Perímetro
        arcpy.AddMessage("Procesando U_PERIMETRO_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_Perimetro")],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_PERIMETRO_CTM12"),
            schema_type="NO_TEST",
            field_mapping="DEPARTAMENTO_CODIGO \"Departamento_Codigo\" true true false 2 Text 0 0,First,#,main.CC_Perimetro,codigo_departamento,0,1;MUNICIPIO_CODIGO \"Municipio_Codigo\" true true false 5 Text 0 0,First,#,main.CC_Perimetro,codigo_municipio,0,4;TIPO_AVALUO \"Tipo_Avaluo\" true true false 30 Text 0 0,First,#,main.CC_Perimetro,tipo_avaluo,0,29;NOMBRE_GEOGRAFICO \"Nombre_Geografico\" true true false 50 Text 0 0,First,#,main.CC_Perimetro,nombre_geografico,0,49;CODIGO_NOMBRE \"Codigo_Nombre\" true true false 255 Text 0 0,First,#,main.CC_Perimetro,codigo_nombre,0,254;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#,main.CC_Perimetro,codigo_municipio,0,4;DEP_MUNICIPIO \"DEP_MUNICIPIO\" true true false 255 Text 0 0,Join,\",\",main.CC_Perimetro,codigo_departamento,0,1,main.CC_Perimetro,codigo_municipio,0,4;codigo_perimetro \"codigo_perimetro\" true true false 255 Text 0 0,First,#"
        )




        # 12. Sector Rural
        arcpy.AddMessage("Procesando R_SECTOR_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_Sector_rural")],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_SECTOR_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false true 17 Text 0 0,First,#,main.CC_Sector_rural,codigo,0,8;BARRIO_CODIGO \"Barrio_Codigo\" true true false 13 Text 0 0,First,#;CODIGO_ANTERIOR \"Codigo_Anterior\" true true false 255 Text 0 0,First,#;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )

        # 13. Sector Urbano
        arcpy.AddMessage("Procesando U_SECTOR_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_Sector_urbano")],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_SECTOR_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false true 9 Text 0 0,First,#,main.CC_Sector_urbano,codigo,0,8;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )

        # 14. Vereda
        arcpy.AddMessage("Procesando R_VEREDA_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_vereda")],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_VEREDA_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false true 17 Text 0 0,First,#,main.CC_vereda,codigo,0,16;SECTOR_CODIGO \"Sector_Codigo\" true true false 9 Text 0 0,First,#,main.CC_vereda,codigo_sector,0,1;NOMBRE \"Nombre\" true false true 100 Text 0 0,First,#,main.CC_vereda,nombre,0,49;CODIGO_ANTERIOR \"Codigo_Anterior\" true true false 13 Text 0 0,First,#;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;GLOBALID \"GLOBALID\" false false true 38 GlobalID 0 0,First,#;CODIGO_MUNICIPIO \"CODIGO_MUNICIPIO\" true true false 5 Text 0 0,First,#"
        )

        # 15. Terreno Urbano Formal
        arcpy.AddMessage("Procesando U_TERRENO_CTM12...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_predio"),
            out_layer="upredio_formal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)<>'2' AND SUBSTR(numero_predial_nacional,6,2)<>'00'"
        )

        arcpy.management.RepairGeometry("upredio_formal")
        arcpy.management.Append(
            inputs=["upredio_formal"],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_TERRENO_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false false 30 Text 0 0,First,#,upredio_formal,numero_predial_nacional,0,29;MANZANA_CODIGO \"Manzana_Codigo\" true true false 17 Text 0 0,First,#;CODIGO_ANTERIOR \"Codigo_Anterior\" true true false 20 Text 0 0,First,#;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )
        arcpy.management.Delete("upredio_formal")

        # 16. Terreno Urbano Informal
        arcpy.AddMessage("Procesando U_TERRENO_INFORMAL...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_predio"),
            out_layer="upredio_informal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)='2' AND SUBSTR(numero_predial_nacional,6,2)<>'00'"
        )

        arcpy.management.RepairGeometry("upredio_informal")
        arcpy.management.Append(
            inputs=["upredio_informal"],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_TERRENO_INFORMAL"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false false 30 Text 0 0,First,#,upredio_informal,numero_predial_nacional,0,29;CODIGO_ANTERIOR \"Codigo_Anterior\" true true false 20 Text 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;USUARIO_LOG \"USUARIO_LOG\" true true false 100 Text 0 0,First,#;FECHA_LOG \"FECHA_LOG\" true true false 8 Date 0 0,First,#;GLOBALID_SNC \"globalid_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )
        arcpy.management.Delete("upredio_informal")



        # 17. Unidad Informal Urbana
        arcpy.AddMessage("Procesando U_UNIDAD_INFORMAL...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_unidadconstruccion"),
            out_layer="u_unidad_informal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)='2' AND SUBSTR(numero_predial_nacional,6,2)<>'00'"
        )

        arcpy.management.RepairGeometry("u_unidad_informal")
        arcpy.management.Append(
            inputs=["u_unidad_informal"],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_UNIDAD_INFORMAL"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false false 30 Text 0 0,First,#,u_unidad_informal,numero_predial_nacional,0,29;TERRENO_CODIGO \"Terreno_Codigo\" true true false 30 Text 0 0,First,#,u_unidad_informal,numero_predial_nacional,0,29;CONSTRUCCION_CODIGO \"Construccion_Codigo\" true true false 30 Text 0 0,First,#,u_unidad_informal,numero_predial_nacional,0,29;PLANTA \"Planta\" true false false 10 Text 0 0,First,#,u_unidad_informal,piso_total,0,9;TIPO_CONSTRUCCION \"Tipo_Construccion\" true false false 20 Text 0 0,First,#,u_unidad_informal,iliCode,0,19;ETIQUETA \"Etiqueta\" true true false 50 Text 0 0,First,#,u_unidad_informal,etiqueta,0,49;IDENTIFICADOR \"Identificador\" true false false 2 Text 0 0,First,#,u_unidad_informal,identificador,0,1;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;USUARIO_LOG \"USUARIO_LOG\" true true false 50 Text 0 0,First,#;FECHA_LOG \"FECHA_LOG\" true true false 8 Date 0 0,First,#;GLOBALID_SNC \"globalid_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )
        arcpy.management.Delete("u_unidad_informal")

        # 18. Unidad Informal Rural
        arcpy.AddMessage("Procesando R_UNIDAD_INFORMAL...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_unidadconstruccion"),
            out_layer="r_unidad_informal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)='2' AND SUBSTR(numero_predial_nacional,6,2)='00'"
        )

        arcpy.management.RepairGeometry("r_unidad_informal")
        arcpy.management.Append(
            inputs=["r_unidad_informal"],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_UNIDAD_INFORMAL"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false false 30 Text 0 0,First,#,r_unidad_informal,numero_predial_nacional,0,29;TERRENO_CODIGO \"Terreno_Codigo\" true true false 30 Text 0 0,First,#,r_unidad_informal,numero_predial_nacional,0,29;CONSTRUCCION_CODIGO \"Construccion_Codigo\" true true false 30 Text 0 0,First,#,r_unidad_informal,numero_predial_nacional,0,29;PLANTA \"Planta\" true false false 10 Text 0 0,First,#,r_unidad_informal,piso_total,0,9;TIPO_CONSTRUCCION \"Tipo_Construccion\" true false false 50 Text 0 0,First,#,r_unidad_informal,iliCode,0,49;ETIQUETA \"Etiqueta\" true true false 50 Text 0 0,First,#,r_unidad_informal,etiqueta,0,49;IDENTIFICADOR \"Identificador\" true false false 2 Text 0 0,First,#,r_unidad_informal,identificador,0,1;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )
        arcpy.management.Delete("r_unidad_informal")

        # 19. Unidad Formal Rural
        arcpy.AddMessage("Procesando R_UNIDAD_CTM12...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_unidadconstruccion"),
            out_layer="r_unidad_formal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)<>'2' AND SUBSTR(numero_predial_nacional,6,2)='00'"
        )

        arcpy.management.RepairGeometry("r_unidad_formal")
        arcpy.management.Append(
            inputs=["r_unidad_formal"],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_UNIDAD_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false false 30 Text 0 0,First,#,r_unidad_formal,numero_predial_nacional,0,29;TERRENO_CODIGO \"Terreno_Codigo\" true true false 30 Text 0 0,First,#,r_unidad_formal,numero_predial_nacional,0,29;CONSTRUCCION_CODIGO \"Construccion_Codigo\" true true false 30 Text 0 0,First,#,r_unidad_formal,numero_predial_nacional,0,29;PLANTA \"Planta\" true false false 10 Text 0 0,First,#,r_unidad_formal,piso_total,0,9;TIPO_CONSTRUCCION \"Tipo_Construccion\" true false false 50 Text 0 0,First,#,r_unidad_formal,iliCode,0,49;ETIQUETA \"Etiqueta\" true true false 100 Text 0 0,First,#,r_unidad_formal,etiqueta,0,99;IDENTIFICADOR \"Identificador\" true false false 2 Text 0 0,First,#,r_unidad_formal,identificador,0,1;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )
        arcpy.management.Delete("r_unidad_formal")

        # 20. Unidad Formal Urbana
        arcpy.AddMessage("Procesando U_UNIDAD_CTM12...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_unidadconstruccion"),
            out_layer="u_unidad_formal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)<>'2' AND SUBSTR(numero_predial_nacional,6,2)<>'00'"
        )

        arcpy.management.RepairGeometry("u_unidad_formal")
        arcpy.management.Append(
            inputs=["u_unidad_formal"],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_UNIDAD_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false false 30 Text 0 0,First,#,u_unidad_formal,numero_predial_nacional,0,29;TERRENO_CODIGO \"Terreno_Codigo\" true true false 30 Text 0 0,First,#,u_unidad_formal,numero_predial_nacional,0,29;CONSTRUCCION_CODIGO \"Construccion_Codigo\" true true false 30 Text 0 0,First,#,u_unidad_formal,numero_predial_nacional,0,29;PLANTA \"Planta\" true false false 10 Text 0 0,First,#,u_unidad_formal,piso_total,0,9;TIPO_CONSTRUCCION \"Tipo_Construccion\" true false false 20 Text 0 0,First,#,u_unidad_formal,iliCode,0,19;ETIQUETA \"Etiqueta\" true true false 50 Text 0 0,First,#,u_unidad_formal,etiqueta,0,49;IDENTIFICADOR \"Identificador\" true false false 2 Text 0 0,First,#,u_unidad_formal,identificador,0,1;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )
        arcpy.management.Delete("u_unidad_formal")



                # 21. Zona Homogénea Física Rural
        arcpy.AddMessage("Procesando ZHR_FISICA_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.Zona_homo_fisicarural")],
            target=os.path.join(gdb_path, "ZONA_HOMOGENEA_RURAL_CTM12", "ZHR_FISICA_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true true true 7 Text 0 0,First,#,main.Zona_homo_fisicarural,codigo,0,6;CODIGO_ZONA_FISICA \"Codigo_Zona_Fisica\" true false true 4 Text 0 0,First,#,main.Zona_homo_fisicarural,codigo_zona_fisica,0,3;AREA_HOMOGENEA_TIERRA \"Area_Homogenea_Tierra\" true true false 600 Text 0 0,First,#,main.Zona_homo_fisicarural,area_homogenea_tierra,0,599;DISPONIBILIDAD_AGUA \"Disponibilidad_Agua\" true true false 4 Long 0 0,First,#,main.Zona_homo_fisicarural,disponibilidad_agua,-1,-1;INFLUENCIA_VIAL \"Influencia_Vial\" true true false 4 Long 0 0,First,#,main.Zona_homo_fisicarural,influencia_vial,-1,-1;USO_ACTUAL_SUELO \"Uso_Actual_Suelo\" true false false 4 Long 0 0,First,#,main.Zona_homo_fisicarural,uso_actual_suelo,-1,-1;NORMA_USO_SUELO \"Norma_Uso_Suelo\" true true false 250 Text 0 0,First,#,main.Zona_homo_fisicarural,norma_uso_suelo,0,249;VIGENCIA \"Vigencia\" true true false 8 Date 0 0,First,#,main.Zona_homo_fisicarural,vigencia,-1,-1;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )

        # 22. Zona Homogénea Geoeconómica Rural
        arcpy.AddMessage("Procesando ZHR_GEOECONOMICA_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.Zona_homo_geoeconomicarural")],
            target=os.path.join(gdb_path, "ZONA_HOMOGENEA_RURAL_CTM12", "ZHR_GEOECONOMICA_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true true true 7 Text 0 0,First,#,main.Zona_homo_geoeconomicarural,codigo,0,6;CODIGO_ZONA_GEOECONOMICA \"Codigo_Zona_Geoeconomica\" true true false 4 Text 0 0,First,#,main.Zona_homo_geoeconomicarural,codigo_zona_geoeconomica,0,3;VALOR_HECTAREA \"Valor_Hectarea\" true true false 20 Text 0 0,First,#,main.Zona_homo_geoeconomicarural,valor_hectarea,0,19;SUBZONA_FISICA \"Subzona_Fisica\" true true false 600 Text 0 0,First,#,main.Zona_homo_geoeconomicarural,subzona_fisica,0,599;VIGENCIA \"Vigencia\" true true false 8 Date 0 0,First,#,main.Zona_homo_geoeconomicarural,vigencia,-1,-1;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )

        # 23. Zona Homogénea Física Urbana
        arcpy.AddMessage("Procesando ZHU_FISICA_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.Zona_homo_fisicaurbana")],
            target=os.path.join(gdb_path, "ZONA_HOMOGENEA_URBANO_CTM12", "ZHU_FISICA_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false true 7 Text 0 0,First,#,main.Zona_homo_fisicaurbana,codigo,0,6;CODIGO_ZONA_FISICA \"Codigo_Zona_Fisica\" true true false 4 Text 0 0,First,#,main.Zona_homo_fisicaurbana,codigo_zona_fisica,0,3;TOPOGRAFIA \"Topografia\" true true false 20 Text 0 0,First,#,main.Zona_homo_fisicaurbana,topografia,0,19;INFLUENCIA_VIAL \"Influencia_Vias\" true true false 4 Long 0 0,First,#,main.Zona_homo_fisicaurbana,influencia_vial,-1,-1;SERVICIO_PUBLICO \"Servicios_Publicos\" true true false 4 Long 0 0,First,#,main.Zona_homo_fisicaurbana,servicio_publico,-1,-1;USO_ACTUAL_SUELO \"Uso_Actual_Suelo\" true true false 4 Long 0 0,First,#,main.Zona_homo_fisicaurbana,uso_actual_suelo,-1,-1;NORMA_USO_SUELO \"Norma_Uso_Suelo\" true true false 250 Text 0 0,First,#,main.Zona_homo_fisicaurbana,norma_uso_suelo,0,249;TIPIFICACION_CONSTRUCCION \"Tipificacion_Construccion\" true true false 4 Long 0 0,First,#,main.Zona_homo_fisicaurbana,tipificacion_construccion,-1,-1;VIGENCIA \"Vigencia\" true true false 8 Date 0 0,First,#,main.Zona_homo_fisicaurbana,vigencia,-1,-1;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )

        # 24. Zona Homogénea Geoeconómica Urbana
        arcpy.AddMessage("Procesando ZHU_GEOECONOMICA_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.Zona_homo_geoeconomicaurbana")],
            target=os.path.join(gdb_path, "ZONA_HOMOGENEA_URBANO_CTM12", "ZHU_GEOECONOMICA_CTM12"),
            schema_type="NO_TEST",
            field_mapping="CODIGO \"Codigo\" true false true 7 Text 0 0,First,#,main.Zona_homo_geoeconomicaurbana,codigo,0,6;CODIGO_ZONA_GEOECONOMICA \"Codigo_Zona_Geoeconomica\" true true true 4 Text 0 0,First,#,main.Zona_homo_geoeconomicaurbana,codigo_zona_geoeconomica,0,3;VALOR_METRO \"Valor_Metro\" true true false 8 Double 0 0,First,#,main.Zona_homo_geoeconomicaurbana,valor_metro,-1,-1;SUBZONA_FISICA \"Subzona_Fisica\" true true false 100 Text 0 0,First,#,main.Zona_homo_geoeconomicaurbana,subzona_fisica,0,99;VIGENCIA \"Vigencia\" true true false 8 Date 0 0,First,#,main.Zona_homo_geoeconomicaurbana,vigencia,-1,-1;USUARIO_LOG \"Usuario_Log\" true true false 100 Text 0 0,First,#;FECHA_LOG \"Fecha_Log\" true true false 8 Date 0 0,First,#;GLOBALID \"GlobalID\" false false true 38 GlobalID 0 0,First,#;GLOBALID_SNC \"GLOBALID_SNC\" true true false 38 Text 0 0,First,#;CODIGO_MUNICIPIO \"codigo_municipio\" true true false 5 Text 0 0,First,#"
        )

        arcpy.AddMessage("Migración completada con éxito")

                # 25. Dirección Formal Urbana
        arcpy.AddMessage("Procesando U_DIRECCION_FORMAL_CTM12...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.Extdireccion"),
            out_layer="u_direccion_formal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)<>'2' AND SUBSTR(numero_predial_nacional,6,2)<>'00'"
        )

        arcpy.management.RepairGeometry("u_direccion_formal")
        arcpy.management.Append(
            inputs=["u_direccion_formal"],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_DIRECCION_FORMAL_CTM12"),
            schema_type="NO_TEST",
            field_mapping="numero_predial \"CODIGO\" true true false 30 Text 0 0,First,#,u_direccion_formal,numero_predial_nacional,0,29;numero_predial_anterior \"CODIGO_ANTERIOR\" true true false 20 Text 0 0,First,#"
        )
        arcpy.management.Delete("u_direccion_formal")

        # 26. Dirección Informal Urbana
        arcpy.AddMessage("Procesando U_DIRECCION_INFORMAL_CTM12...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.Extdireccion"),
            out_layer="u_direccion_informal",
            where_clause="SUBSTR(numero_predial_nacional,22,1)='2' AND SUBSTR(numero_predial_nacional,6,2)<>'00'"
        )

        arcpy.management.RepairGeometry("u_direccion_informal")
        arcpy.management.Append(
            inputs=["u_direccion_informal"],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_DIRECCION_INFORMAL_CTM12"),
            schema_type="NO_TEST",
            field_mapping="numero_preidal \"CODIGO\" true true false 30 Text 0 0,First,#,u_direccion_informal,numero_predial_nacional,0,29;numero_predial_anterior \"CODIGO_ANTERIOR\" true true false 20 Text 0 0,First,#"
        )
        arcpy.management.Delete("u_direccion_informal")

        # 27. Derecho Rural
        arcpy.AddMessage("Procesando R_DERECHO...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_derecho_tipo"),
            out_layer="derecho_rural",
            where_clause="SUBSTR(numero_predial_nacional,6,2)='00'"
        )

        arcpy.management.Append(
            inputs=["derecho_rural"],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_DERECHO"),
            schema_type="NO_TEST",
            field_mapping="NUMERO_PREDIAL \"NUMERO_PREDIAL\" true true false 30 Text 0 0,First,#,derecho_rural,numero_predial_nacional,0,29;NUMERO_PREDIAL_ANTERIOR \"NUMERO_PREDIAL_ANTERIOR\" true true false 20 Text 0 0,First,#;TITULARIDAD \"TITULARIDAD\" true true false 50 Text 0 0,First,#,derecho_rural,iliCode,0,49"
        )
        arcpy.management.Delete("derecho_rural")

        # 28. Derecho Urbano
        arcpy.AddMessage("Procesando U_DERECHO...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.lc_derecho_tipo"),
            out_layer="derecho_urbano",
            where_clause="SUBSTR(numero_predial_nacional,6,2)<>'00'"
        )

        arcpy.management.Append(
            inputs=["derecho_urbano"],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_DERECHO"),
            schema_type="NO_TEST",
            field_mapping="NUMERO_PREDIAL \"NUMERO_PREDIAL\" true true false 30 Text 0 0,First,#,derecho_urbano,numero_predial_nacional,0,29;NUMERO_PREDIAL_ANTERIOR \"NUMERO_PREDIAL_ANTERIOR\" true true false 20 Text 0 0,First,#;TITULARIDAD \"TITULARIDAD\" true true false 50 Text 0 0,First,#,derecho_urbano,iliCode,0,49"
        )
        arcpy.management.Delete("derecho_urbano")

        # 29. Tipo Predio Rural
        arcpy.AddMessage("Procesando R_TIPO_PREDIO...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.Lc_Tipo_predio"),
            out_layer="tipo_predio_rural",
            where_clause="SUBSTR(numero_predial_nacional,6,2)='00'"
        )

        arcpy.management.Append(
            inputs=["tipo_predio_rural"],
            target=os.path.join(gdb_path, "RURAL_CTM12", "R_TIPO_PREDIO"),
            schema_type="NO_TEST",
            field_mapping="NUMERO_PREDIAL \"NUMERO_PREDIAL\" true true false 30 Text 0 0,First,#,tipo_predio_rural,numero_predial_nacional,0,29;NUMERO_PREDIAL_ANTERIOR \"NUMERO_PREDIAL_ANTERIOR\" true true false 20 Text 0 0,First,#;TIPO_PREDIO \"TIPO_PREDIO\" true true false 50 Text 0 0,First,#,tipo_predio_rural,iliCode,0,49"
        )
        arcpy.management.Delete("tipo_predio_rural")

        # 30. Tipo Predio Urbano
        arcpy.AddMessage("Procesando U_TIPO_PREDIO...")
        arcpy.management.MakeFeatureLayer(
            in_features=os.path.join(gpkg_path, "main.Lc_Tipo_predio"),
            out_layer="tipo_predio_urbano",
            where_clause="SUBSTR(numero_predial_nacional,6,2)<>'00'"
        )

        arcpy.management.Append(
            inputs=["tipo_predio_urbano"],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_TIPO_PREDIO"),
            schema_type="NO_TEST",
            field_mapping="NUMERO_PREDIAL \"NUMERO_PREDIAL\" true true false 30 Text 0 0,First,#,tipo_predio_urbano,numero_predial_nacional,0,29;NUMERO_PREDIAL_ANTERIOR \"NUMERO_PREDIAL_ANTERIOR\" true true false 20 Text 0 0,First,#;TIPO_PREDIO \"TIPO_PREDIO\" true true false 50 Text 0 0,First,#,tipo_predio_urbano,iliCode,0,49"
        )
        arcpy.management.Delete("tipo_predio_urbano")

        # 31. Límite Municipal Urbano
        arcpy.AddMessage("Procesando U_LIMITEMUN_CTM12...")
        arcpy.management.Append(
            inputs=[os.path.join(gpkg_path, "main.CC_Limite_Municipio")],
            target=os.path.join(gdb_path, "URBANO_CTM12", "U_LIMITEMUN_CTM12"),
            schema_type="NO_TEST",
            field_mapping="codicodigo_municipiogo_departamento \"codigo_departamento\" true true false 2 Text 0 0,First,#,main.CC_Limite_Municipio,codigo_departamento,0,1;codigo_municipio \"codigo_municipio\" true true false 5 Text 0 0,First,#,main.CC_Limite_Municipio,codigo_municipio,0,4;nombre_municipio \"nombre_municipio\" true true false 255 Text 0 0,First,#,main.CC_Limite_Municipio,nombre_municipio,0,254;DEP_MUNICIPIO_LIMITE \"DEP_MUNICIPIO_LIMITE\" true true false 255 Text 0 0,First,#"
        )

        arcpy.AddMessage("Migración completada con éxito")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    migrate_gpkg_to_gdb()