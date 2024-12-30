import os
import pandas as pd
import sqlite3
from pathlib import Path
import re
import sys
sys.stdout.reconfigure(encoding='utf-8')

def get_project_root():
    """Encuentra la raíz del proyecto verificando la estructura de directorios esperada."""
    current_path = Path(os.path.abspath(__file__)).parent
    
    while current_path.parent != current_path:
        required_paths = [
            current_path / "Files" / "Temporary_Files" / "MODELO_IGAC",
            current_path / "Files" / "Temporary_Files" / "array_config.txt",
            current_path / "Files" / "Temporary_Files" / "MODELO_IGAC" / "db",
            current_path / "Files" / "Temporary_Files" / "MODELO_IGAC" / "Omision_comision_temp"
        ]
        
        if all(path.exists() for path in required_paths):
            print(f"Raíz del proyecto encontrada en: {current_path}")
            return current_path
        
        current_path = current_path.parent
    
    raise Exception("No se encontró la raíz del proyecto.")

def classify_record(numero_predial):
    """Clasifica un registro como RURAL o URBANO basado en el número predial."""
    try:
        predial_str = str(numero_predial).replace('.0', '')
        if not predial_str or len(predial_str) < 7:
            return 'URBANO'
        digits_6_7 = predial_str[5:7]
        return 'RURAL' if digits_6_7 == '00' else 'URBANO'
    except Exception as e:
        print(f"Error clasificando registro: {str(e)}")
        return 'URBANO'

def shorten_sheet_name(name, zone_type):
    """Acorta el nombre de la hoja a menos de 31 caracteres."""
    name = name.replace('_Urbana-Rural', '')
    base_name = name.split('.')[0]
    
    replacements = {
        'Terrenos': 'Terr',
        'Unidades': 'Unid',
        'Construccion': 'Const',
        'Duplicados': 'Dup'
    }
    
    for full, short in replacements.items():
        base_name = base_name.replace(full, short)
    
    suffix = f"_{zone_type}"
    shortened = f"{base_name}{suffix}"
    shortened = re.sub(r'_+', '_', shortened)
    shortened = shortened.strip('_')
    
    return shortened[:31]

def get_sheet_names(file_name):
    """Determina las hojas de Excel basado en el nombre del archivo."""
    file_name = file_name.lower()
    
    # Para archivos dual (Urbana-Rural)
    if 'urbana-rural' in file_name:
        if 'omision' in file_name:
            return ['Omision_Rural', 'Omision_Urbana']
        elif 'comision' in file_name:
            return ['Comision_Rural', 'Comision_Urbana']
    
    # Para archivos solo rurales
    elif '_rural' in file_name:
        if 'omision' in file_name:
            return ['Omision_Rural']
        elif 'comision' in file_name:
            return ['Comision_Rural']
    
    # Para archivos solo urbanos
    elif '_urbana' in file_name and 'rural' not in file_name:
        if 'omision' in file_name:
            return ['Omision_Urbana']
        elif 'comision' in file_name:
            return ['Comision_Urbana']
    
    # Para duplicados
    elif 'duplicados' in file_name:
        return ['Duplicados']
    
    return None


def process_duplicates(df, codigo_column):
    """Procesa y separa los duplicados en rurales y urbanos."""
    df_rural = df[df[codigo_column].apply(lambda x: classify_record(x) == 'RURAL')].copy()
    df_urbano = df[df[codigo_column].apply(lambda x: classify_record(x) == 'URBANO')].copy()
    return df_rural, df_urbano

def get_correct_sheet_name(file_name, sheet_type):
    """
    Determina el nombre correcto de la hoja basado en el tipo de archivo.
    """
    file_name = file_name.lower()
    
    if 'duplicados' in file_name:
        return 'Duplicados'
        
    # Archivos de comisión
    if 'comision' in file_name:
        if 'rural' in sheet_type:
            return 'Comision_Rural'
        return 'Comision_Urbana'
    
    # Archivos de omisión
    if 'omision' in file_name:
        if 'rural' in sheet_type:
            return 'Omision_Rural'
        return 'Omision_Urbana'


        
def map_excel_contents(excel_dir):
    """Mapea el contenido de cada Excel y sus hojas."""
    excel_maps = {}
    
    for excel_file in excel_dir.glob('*.xlsx'):
        file_name = excel_file.name
        if 'Omision_Comision.xlsx' in file_name:
            continue
            
        try:
            xl = pd.ExcelFile(excel_file)
            sheet_maps = {}
            
            # Capturar información de cada hoja
            for sheet in xl.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet)
                sheet_maps[sheet] = {
                    'records': len(df),
                    'columns': list(df.columns),
                    'type': 'omision' if 'omision' in file_name.lower() else 'comision' if 'comision' in file_name.lower() else 'duplicados',
                    'zone': 'rural' if 'rural' in sheet.lower() else 'urbana' if 'urbana' in sheet.lower() else 'both'
                }
            
            excel_maps[file_name] = sheet_maps
            
        except Exception as e:
            print(f"Error mapeando {file_name}: {str(e)}")
            
    return excel_maps


def determine_file_type(file_name):
    """Determina el tipo de archivo basado en su nombre."""
    file_name = file_name.lower()
    
    patterns = {
        r'1.*omision.*terrenos': ('omision_terrenos', 'R1_TERRENO.Numero_Predial'),
        r'2.*comision.*terrenos': ('comision_terrenos', 'TERRENO_TOTAL.CODIGO'),
        r'3.*omision.*unidades': ('omision_unidades', 'R1_UNIDAD.Numero_Predial'),
        r'4.*comision.*unidades': ('comision_unidades', 'UNIDAD_TOTAL.CODIGO'),
        r'5.*omision.*mejoras': ('omision_mejoras', 'R1_TERRENO.Numero_Predial'),
        r'6.*comision.*mejoras': ('comision_mejoras', 'MEJORAS_TOTAL.CODIGO'),
        r'.*duplicados.*': ('duplicados', 'CODIGO')
    }
    
    for pattern, (file_type, column) in patterns.items():
        if re.search(pattern, file_name):
            return file_type, column
            
    return None, None

def process_excel_files():
    try:
        root_dir = get_project_root()
        excel_dir = root_dir / "Files" / "Temporary_Files" / "MODELO_IGAC" / "Omision_comision_temp"
        db_dir = root_dir / "Files" / "Temporary_Files" / "MODELO_IGAC" / "db"
        db_path = db_dir / "omision_comision.db"
        output_file = excel_dir / "Omision_Comision.xlsx"

        os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Verificar y recrear tablas con el nuevo esquema
        for table in ['RURAL', 'URBANO']:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            cursor.execute(f"""
            CREATE TABLE {table} (
                omision_terrenos INTEGER,
                comision_terrenos INTEGER,
                omision_unidades INTEGER,
                comision_unidades INTEGER,
                omision_mejoras INTEGER,
                comision_mejoras INTEGER,
                duplicados INTEGER
            )
            """)
        
        data = {
            'omision_terrenos': {'rural': None, 'urbano': None},
            'comision_terrenos': {'rural': None, 'urbano': None},
            'omision_unidades': {'rural': None, 'urbano': None},
            'comision_unidades': {'rural': None, 'urbano': None},
            'omision_mejoras': {'rural': None, 'urbano': None},
            'comision_mejoras': {'rural': None, 'urbano': None},
            'duplicados': {'rural': None, 'urbano': None}
        }
        
        print("\nProcesando archivos Excel...")
        
        for excel_file in excel_dir.glob('*.xlsx'):
            if excel_file.name == output_file.name:
                continue
                
            print(f"\nProcesando archivo: {excel_file.name}")
            file_name = excel_file.name.lower()
            xl = pd.ExcelFile(excel_file)
            available_sheets = xl.sheet_names
            
            try:
                # Determinar si es rural, urbano o dual
                is_rural = 'rural' in file_name and 'urbana' not in file_name
                is_urban = 'urbana' in file_name and 'rural' not in file_name
                is_dual = 'urbana-rural' in file_name
                
                # Procesar cada tipo de archivo
                if '1_omision_terrenos' in file_name:
                    if (is_rural or is_dual) and 'Omision_Rural' in available_sheets:
                        data['omision_terrenos']['rural'] = pd.read_excel(xl, 'Omision_Rural')
                        print(f"  Procesada hoja Omision_Rural con {len(data['omision_terrenos']['rural'])} registros")
                    if (is_urban or is_dual) and 'Omision_Urbana' in available_sheets:
                        data['omision_terrenos']['urbano'] = pd.read_excel(xl, 'Omision_Urbana')
                        print(f"  Procesada hoja Omision_Urbana con {len(data['omision_terrenos']['urbano'])} registros")

                elif '2_comision_terrenos' in file_name:
                    if (is_rural or is_dual) and 'Comision_Rural' in available_sheets:
                        data['comision_terrenos']['rural'] = pd.read_excel(xl, 'Comision_Rural')
                        print(f"  Procesada hoja Comision_Rural con {len(data['comision_terrenos']['rural'])} registros")
                    if (is_urban or is_dual) and 'Comision_Urbana' in available_sheets:
                        data['comision_terrenos']['urbano'] = pd.read_excel(xl, 'Comision_Urbana')
                        print(f"  Procesada hoja Comision_Urbana con {len(data['comision_terrenos']['urbano'])} registros")

                elif '3_omision_unidades' in file_name:
                    if (is_rural or is_dual) and 'Omision_Rural' in available_sheets:
                        data['omision_unidades']['rural'] = pd.read_excel(xl, 'Omision_Rural')
                        print(f"  Procesada hoja Omision_Rural con {len(data['omision_unidades']['rural'])} registros")
                    if (is_urban or is_dual) and 'Omision_Urbana' in available_sheets:
                        data['omision_unidades']['urbano'] = pd.read_excel(xl, 'Omision_Urbana')
                        print(f"  Procesada hoja Omision_Urbana con {len(data['omision_unidades']['urbano'])} registros")

                elif '4_comision_unidades' in file_name:
                    if (is_rural or is_dual) and 'Comision_Rural' in available_sheets:
                        data['comision_unidades']['rural'] = pd.read_excel(xl, 'Comision_Rural')
                        print(f"  Procesada hoja Comision_Rural con {len(data['comision_unidades']['rural'])} registros")
                    if (is_urban or is_dual) and 'Comision_Urbana' in available_sheets:
                        data['comision_unidades']['urbano'] = pd.read_excel(xl, 'Comision_Urbana')
                        print(f"  Procesada hoja Comision_Urbana con {len(data['comision_unidades']['urbano'])} registros")

                elif '5_omision_mejoras' in file_name:
                    if (is_rural or is_dual) and 'Omision_Rural' in available_sheets:
                        data['omision_mejoras']['rural'] = pd.read_excel(xl, 'Omision_Rural')
                        print(f"  Procesada hoja Omision_Rural con {len(data['omision_mejoras']['rural'])} registros")
                    if (is_urban or is_dual) and 'Omision_Urbana' in available_sheets:
                        data['omision_mejoras']['urbano'] = pd.read_excel(xl, 'Omision_Urbana')
                        print(f"  Procesada hoja Omision_Urbana con {len(data['omision_mejoras']['urbano'])} registros")

                elif '6_comision_mejoras' in file_name:
                    if (is_rural or is_dual) and 'Comision_Rural' in available_sheets:
                        data['comision_mejoras']['rural'] = pd.read_excel(xl, 'Comision_Rural')
                        print(f"  Procesada hoja Comision_Rural con {len(data['comision_mejoras']['rural'])} registros")
                    if (is_urban or is_dual) and 'Comision_Urbana' in available_sheets:
                        data['comision_mejoras']['urbano'] = pd.read_excel(xl, 'Comision_Urbana')
                        print(f"  Procesada hoja Comision_Urbana con {len(data['comision_mejoras']['urbano'])} registros")

                elif '9_duplicados' in file_name:
                    df = pd.read_excel(xl)
                    print(f"  Procesada hoja Duplicados con {len(df)} registros")
                    data['duplicados']['rural'] = df[df['CODIGO'].apply(lambda x: classify_record(x) == 'RURAL')]
                    data['duplicados']['urbano'] = df[df['CODIGO'].apply(lambda x: classify_record(x) == 'URBANO')]

            except Exception as e:
                print(f"Error procesando {file_name}: {str(e)}")

        # Actualizar conteos para ambos tipos
        counts = {
            'RURAL': {
                'omision_terrenos': len(data['omision_terrenos']['rural']) if data['omision_terrenos']['rural'] is not None else 0,
                'comision_terrenos': len(data['comision_terrenos']['rural']) if data['comision_terrenos']['rural'] is not None else 0,
                'omision_unidades': len(data['omision_unidades']['rural']) if data['omision_unidades']['rural'] is not None else 0,
                'comision_unidades': len(data['comision_unidades']['rural']) if data['comision_unidades']['rural'] is not None else 0,
                'omision_mejoras': len(data['omision_mejoras']['rural']) if data['omision_mejoras']['rural'] is not None else 0,
                'comision_mejoras': len(data['comision_mejoras']['rural']) if data['comision_mejoras']['rural'] is not None else 0,
                'duplicados': len(data['duplicados']['rural']) if data['duplicados']['rural'] is not None else 0
            },
            'URBANO': {
                'omision_terrenos': len(data['omision_terrenos']['urbano']) if data['omision_terrenos']['urbano'] is not None else 0,
                'comision_terrenos': len(data['comision_terrenos']['urbano']) if data['comision_terrenos']['urbano'] is not None else 0,
                'omision_unidades': len(data['omision_unidades']['urbano']) if data['omision_unidades']['urbano'] is not None else 0,
                'comision_unidades': len(data['comision_unidades']['urbano']) if data['comision_unidades']['urbano'] is not None else 0,
                'omision_mejoras': len(data['omision_mejoras']['urbano']) if data['omision_mejoras']['urbano'] is not None else 0,
                'comision_mejoras': len(data['comision_mejoras']['urbano']) if data['comision_mejoras']['urbano'] is not None else 0,
                'duplicados': len(data['duplicados']['urbano']) if data['duplicados']['urbano'] is not None else 0
            }
        }

        # Insertar datos en ambas tablas
        for table_name, table_counts in counts.items():
            cursor.execute(f"""
            INSERT INTO {table_name} (
                omision_terrenos, comision_terrenos,
                omision_unidades, comision_unidades,
                omision_mejoras, comision_mejoras,
                duplicados
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                table_counts['omision_terrenos'],
                table_counts['comision_terrenos'],
                table_counts['omision_unidades'],
                table_counts['comision_unidades'],
                table_counts['omision_mejoras'],
                table_counts['comision_mejoras'],
                table_counts['duplicados']
            ))

        conn.commit()
        conn.close()

        # Generar Excel con ambos tipos de datos
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            summary_data = {
                'Tipo': ['RURAL', 'URBANO'],
                'Omisión Terrenos': [counts['RURAL']['omision_terrenos'], counts['URBANO']['omision_terrenos']],
                'Comisión Terrenos': [counts['RURAL']['comision_terrenos'], counts['URBANO']['comision_terrenos']],
                'Omisión Unidades': [counts['RURAL']['omision_unidades'], counts['URBANO']['omision_unidades']],
                'Comisión Unidades': [counts['RURAL']['comision_unidades'], counts['URBANO']['comision_unidades']],
                'Omisión Mejoras': [counts['RURAL']['omision_mejoras'], counts['URBANO']['omision_mejoras']],
                'Comisión Mejoras': [counts['RURAL']['comision_mejoras'], counts['URBANO']['comision_mejoras']],
                'Duplicados': [counts['RURAL']['duplicados'], counts['URBANO']['duplicados']]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Resumen', index=False)
            
            # Escribir hojas detalladas
            for key, value in data.items():
                if value['rural'] is not None:
                    sheet_name = shorten_sheet_name(f"{key}_Rural", 'RUR')
                    value['rural'].to_excel(writer, sheet_name=sheet_name, index=False)
                if value['urbano'] is not None:
                    sheet_name = shorten_sheet_name(f"{key}_Urbana", 'URB')
                    value['urbano'].to_excel(writer, sheet_name=sheet_name, index=False)
        
        print("\nProceso completado exitosamente.")
        print(f"Archivo compilado generado en: {output_file}")

    except Exception as e:
        print(f"\nError durante el proceso: {str(e)}")
        if 'conn' in locals():
            conn.close()
            
if __name__ == "__main__":
    try:
        process_excel_files()
    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")