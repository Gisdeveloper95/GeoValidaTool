import arcpy
import json
from pathlib import Path
import os

def analyze_gpkg_structure(gpkg_path):
    """Analyzes GPKG structure and returns layer and field information"""
    structure = {}
    
    try:
        arcpy.env.workspace = str(gpkg_path)
        feature_classes = arcpy.ListFeatureClasses()
        
        for fc in feature_classes:
            fields = arcpy.ListFields(fc)
            field_info = {}
            
            for field in fields:
                # Skip system fields and common fields that don't need validation
                if field.name.upper() in ['GLOBALID', 'GLOBALID_SNC', 'FECHA_LOG', 'USUARIO_LOG']:
                    continue
                    
                field_info[field.name] = {
                    'type': field.type,
                    'required': not field.isNullable
                }
            
            # Get geometry type
            desc = arcpy.Describe(fc)
            structure[fc] = {
                'fields': field_info,
                'geometry_type': desc.shapeType.upper()
            }
            
        return structure
        
    except Exception as e:
        print(f"Error analyzing {gpkg_path}: {str(e)}")
        return None

def create_reference_jsons(original_gpkg, modified_gpkg, output_dir):
    """Creates reference JSON files for both GPKGs"""
    try:
        # Analyze original GPKG
        original_structure = analyze_gpkg_structure(original_gpkg)
        if original_structure:
            original_json_path = os.path.join(output_dir, 'gpkg_reference_ladm_1_0_original.json')
            with open(original_json_path, 'w') as f:
                json.dump(original_structure, f, indent=4)
            print(f"Created reference JSON for original GPKG: {original_json_path}")
        
        # Analyze modified GPKG
        modified_structure = analyze_gpkg_structure(modified_gpkg)
        if modified_structure:
            modified_json_path = os.path.join(output_dir, 'gpkg_reference_ladm_1_0_modificado.json')
            with open(modified_json_path, 'w') as f:
                json.dump(modified_structure, f, indent=4)
            print(f"Created reference JSON for modified GPKG: {modified_json_path}")
            
        return original_structure, modified_structure
        
    except Exception as e:
        print(f"Error creating reference JSONs: {str(e)}")
        return None, None

def main():
    # GPKG paths
    original_gpkg = r"C:\Users\osori\Downloads\Nueva carpeta (12)\Valencia_23855_20240214.gpkg"
    modified_gpkg = r"C:\Users\osori\Downloads\Nueva carpeta (12)\Valencia_23855_20240214_output.gpkg"
    
    # Get the directory of the current script for JSON output
    script_dir = Path(__file__).resolve().parent
    reference_dir = script_dir / "reference"
    
    # Create reference directory if it doesn't exist
    os.makedirs(reference_dir, exist_ok=True)
    
    # Create reference JSONs
    original_structure, modified_structure = create_reference_jsons(
        original_gpkg,
        modified_gpkg,
        reference_dir
    )
    
    if original_structure and modified_structure:
        print("Successfully created reference JSON files")
    else:
        print("Failed to create reference JSON files")

if __name__ == "__main__":
    main()