import sqlite3
import json
import os
from typing import Dict, Any

class GeoPackageAnalyzer:
    def __init__(self, gpkg_path: str):
        self.gpkg_path = gpkg_path
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to the GeoPackage database"""
        self.connection = sqlite3.connect(self.gpkg_path)
        self.cursor = self.connection.cursor()
        
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            
    def get_tables(self) -> list:
        """Get all feature tables from the GeoPackage"""
        self.cursor.execute("""
            SELECT table_name
            FROM gpkg_contents
            WHERE data_type = 'features'
        """)
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_table_structure(self, table_name: str) -> Dict[str, Any]:
        """Get the structure of a specific table"""
        # Get column information
        self.cursor.execute(f"PRAGMA table_info('{table_name}')")
        columns = self.cursor.fetchall()
        
        # Get geometry type
        self.cursor.execute("""
            SELECT geometry_type_name
            FROM gpkg_geometry_columns
            WHERE table_name = ?
        """, (table_name,))
        geometry_result = self.cursor.fetchone()
        geometry_type = geometry_result[0] if geometry_result else None
        
        # Build field information
        fields = {}
        for col in columns:
            col_id, name, dtype, notnull, default, pk = col
            field_info = {
                "type": dtype,
                "length": 0,  # Default value as GPKG doesn't store length
                "precision": 0,
                "scale": 0,
                "is_nullable": not notnull,
                "required": bool(pk or not notnull)
            }
            fields[name] = field_info
            
        # Create table structure
        table_structure = {
            "fields": fields,
            "geometry_type": geometry_type
        }
        
        return table_structure
    
    def analyze(self) -> Dict[str, Any]:
        """Analyze the complete GeoPackage structure"""
        try:
            self.connect()
            
            # Get all feature tables
            tables = self.get_tables()
            
            # Initialize structure
            structure = {}
            
            # Analyze each table
            for table in tables:
                structure[table] = self.get_table_structure(table)
            
            return structure
            
        finally:
            self.close()
    
    def save_to_json(self, output_path: str):
        """Analyze GeoPackage and save structure to JSON file"""
        structure = self.analyze()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(structure, f, indent=4)

def analyze_gpkg(gpkg_path: str, output_path: str):
    """
    Analyze a GeoPackage file and save its structure to JSON
    
    Args:
        gpkg_path: Path to the GeoPackage file
        output_path: Path where to save the JSON output
    """
    analyzer = GeoPackageAnalyzer(gpkg_path)
    analyzer.save_to_json(output_path)

if __name__ == "__main__":
    # Example usage
    gpkg_file = r'C:\Users\osori\Desktop\MAS DE GEOVALIDATOOL\GeoValidaTool\Files\Temporary_Files\MODELO_LADM_1_2\52835_Tumaco_20241108_modelo1_2_output.gpkg'
    json_output = "structure_ladm_!_2.json"
    
    try:
        analyze_gpkg(gpkg_file, json_output)
        print(f"Analysis complete. Structure saved to {json_output}")
    except Exception as e:
        print(f"Error analyzing GeoPackage: {str(e)}")
    