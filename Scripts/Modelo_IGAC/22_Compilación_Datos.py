import os
import shutil
from pathlib import Path
import glob
import logging
import subprocess
import sys
sys.stdout.reconfigure(encoding='utf-8')
# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MinisoftOrganizer:
    def __init__(self):
        # Primero obtenemos la ruta raíz
        self.root_path = self._get_root_path()
        # Luego configuramos las demás rutas
        self.temp_files_path = self.root_path / "Files" / "Temporary_Files" / "MODELO_IGAC"
        self.reports_path = self.root_path / "Reportes" / "MODELO_IGAC"
        # Finalmente cargamos la configuración
        self.DATASETS_TO_PROCESS = self._load_dataset_configuration()

    def _get_root_path(self):
        """
        Encuentra la raíz del proyecto verificando la estructura de directorios esperada.
        """
        current_path = Path(__file__).resolve().parent.parent.parent
        
        required_paths = [
            current_path / "Files" / "Temporary_Files",
            current_path / "Files" / "Temporary_Files" / "array_config.txt",
            current_path / "Scripts",
            current_path / "Files"
        ]
        
        if all(path.exists() for path in required_paths):
            logger.info(f"Raíz del proyecto encontrada en: {current_path}")
            return current_path
            
        error_msg = (
            "No se encontró la raíz del proyecto.\n"
            "Verifique que está ejecutando el script desde dentro del proyecto "
            "y que existe la siguiente estructura:\n"
            "- Files/Temporary_Files\n"
            "- Files/Temporary_Files/array_config.txt\n"
            "- Scripts\n"
            "- Files"
        )
        logger.error(error_msg)
        raise Exception(error_msg)

    def _load_dataset_configuration(self):
        """Carga la configuración de datasets desde el archivo externo"""
        try:
            config_path = self.root_path / "Files" / "Temporary_Files" / "array_config.txt"

            datasets = []
            with open(config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        dataset_name = line.strip('",[]').strip()
                        if dataset_name:
                            datasets.append(dataset_name)

            logger.info("\nConfiguración de datasets cargada:")
            logger.info("--------------------------------")
            logger.info("Datasets que serán procesados:")
            for ds in datasets:
                logger.info(f"  - {ds}")
            logger.info("--------------------------------\n")

            return datasets

        except Exception as e:
            logger.error(f"Error al cargar configuración: {str(e)}")
            default_datasets = ["URBANO_CTM12", "RURAL_CTM12"]
            logger.info("\nUsando configuración por defecto:")
            logger.info("--------------------------------")
            logger.info("Datasets que serán procesados:")
            for ds in default_datasets:
                logger.info(f"  - {ds}")
            logger.info("--------------------------------\n")
            return default_datasets

    def clean_reports_directory(self):
        """
        Verifica si existe el directorio de reportes y lo elimina si existe.
        Luego crea un nuevo directorio limpio.
        """
        try:
            reports_base = self.root_path / "Reportes"
            if reports_base.exists():
                logger.info(f"Eliminando directorio de reportes existente: {reports_base}")
                shutil.rmtree(reports_base)
            
            logger.info(f"Creando nuevo directorio de reportes: {reports_base}")
            reports_base.mkdir(parents=True)
            return True
        except Exception as e:
            logger.error(f"Error al limpiar directorio de reportes: {str(e)}")
            return False

    def create_directory_structure(self):
        """Crea la estructura de directorios necesaria"""
        try:
            # Lista base de directorios
            base_directories = [
                "00_GDB",
                "01_OMISION_COMISION",
                "02_TOPOLOGIA",
                "INSUMOS"
            ]
            
            # Crear directorios base
            for dir_name in base_directories:
                dir_path = self.reports_path / dir_name
                dir_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Directorio creado: {dir_path}")
            
            # Crear estructura específica para inconsistencias y consistencia formato
            for dataset in self.DATASETS_TO_PROCESS:
                inconsistencias_path = self.reports_path / "03_INCONSISTENCIAS" / "CONSISTENCIA_FORMATO" / dataset
                inconsistencias_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Directorio creado: {inconsistencias_path}")
                
            return True
        except Exception as e:
            logger.error(f"Error creando estructura de directorios: {str(e)}")
            return False

    def copy_gdb_files(self):
        """Copia archivos .gdb al directorio 00_GDB"""
        try:
            gdb_files = list(self.temp_files_path.glob("*.gdb"))
            if not gdb_files:
                logger.warning("No se encontraron archivos .gdb para copiar")
                return False
                
            for gdb_file in gdb_files:
                dest_path = self.reports_path / "00_GDB" / gdb_file.name
                if gdb_file.is_dir():
                    shutil.copytree(gdb_file, dest_path, dirs_exist_ok=True)
                else:
                    shutil.copy2(gdb_file, dest_path)
                logger.info(f"Archivo GDB copiado: {gdb_file.name}")
                
            return True
        except Exception as e:
            logger.error(f"Error copiando archivos GDB: {str(e)}")
            return False

    def process_omision_comision(self):
        """
        Procesa archivos de omisión/comisión y todos los directorios encontrados en la ruta origen.
        Copia tanto el archivo Excel principal como cualquier directorio y su contenido.
        """
        try:
            # Ruta origen del archivo Excel y directorios
            source_excel = self.temp_files_path / "Omision_comision_temp" / "Omision_Comision.xlsx"
            source_dir = self.temp_files_path / "Omision_comision_temp"
            dest_dir = self.reports_path / "01_OMISION_COMISION"

            # Crear el directorio de destino si no existe
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Copiar el archivo Excel principal si existe
            if source_excel.exists():
                shutil.copy2(source_excel, dest_dir / "Omision_Comision.xlsx")
                logger.info(f"Archivo principal de omisión/comisión copiado: Omision_Comision.xlsx")
            else:
                logger.warning("Archivo Omision_Comision.xlsx no encontrado en la ruta origen")

            # Procesar todos los directorios en la ruta origen
            if source_dir.exists():
                for item in source_dir.iterdir():
                    if item.is_dir():
                        # Ruta de destino para el directorio
                        dir_dest = dest_dir / item.name
                        
                        # Copiar el directorio y todo su contenido
                        shutil.copytree(item, dir_dest, dirs_exist_ok=True)
                        logger.info(f"Directorio copiado con su contenido: {item.name}")

                logger.info("Proceso de copia de directorios de omisión/comisión completado")
                return True
            else:
                logger.warning(f"Directorio origen no encontrado: {source_dir}")
                return False

        except Exception as e:
            logger.error(f"Error procesando archivos y directorios de omisión/comisión: {str(e)}")
            return False

    def process_topologia(self):
        """Procesa archivos de topología para cada dataset"""
        try:
            for dataset in self.DATASETS_TO_PROCESS:
                source_dir = self.temp_files_path / "02_TOPOLOGIA" / dataset
                if not source_dir.exists():
                    logger.warning(f"Directorio de topología no encontrado para dataset: {dataset}")
                    continue

                dest_dir = self.reports_path / "02_TOPOLOGIA" / dataset
                dest_dir.mkdir(parents=True, exist_ok=True)

                for xlsx_file in source_dir.glob("*.xlsx"):
                    shutil.copy2(xlsx_file, dest_dir / xlsx_file.name)
                    logger.info(f"Archivo de topología copiado: {xlsx_file.name}")
                    
            return True
        except Exception as e:
            logger.error(f"Error procesando archivos de topología: {str(e)}")
            return False

    def copy_inconsistencias(self):
        """Copia archivos de inconsistencias y consistencia"""
        try:
            # Copiar directorio de inconsistencias si existe
            source_dir = self.temp_files_path / "03_INCONSISTENCIAS"
            if source_dir.exists():
                dest_dir = self.reports_path / "03_INCONSISTENCIAS"
                shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
                logger.info("Directorio de inconsistencias copiado")

            # Obtener los archivos de consistencia (ahora buscando directamente en las carpetas temp)
            formato_path = self.temp_files_path / "consistencia_formato_temp"
            geo_path = self.temp_files_path / "consistencia_geoespacial_temp"

            # Obtener los primeros archivos .xlsx encontrados
            formato_files = list(formato_path.glob("*.xlsx"))
            geo_files = list(geo_path.glob("*.xlsx"))

            # Copiar los archivos para cada dataset
            for dataset in self.DATASETS_TO_PROCESS:
                dest_dir = self.reports_path / "03_INCONSISTENCIAS" / "CONSISTENCIA_FORMATO" / dataset

                # Copiar archivo de consistencia formato si existe
                if formato_files:
                    formato_file = formato_files[0]  # Tomar el primer archivo encontrado
                    shutil.copy2(formato_file, dest_dir / formato_file.name)
                    logger.info(f"Archivo de consistencia formato copiado para {dataset}: {formato_file.name}")
                else:
                    logger.warning("No se encontró archivo de consistencia formato")

                # Copiar archivo de consistencia geoespacial si existe
                if geo_files:
                    geo_file = geo_files[0]  # Tomar el primer archivo encontrado
                    shutil.copy2(geo_file, dest_dir / geo_file.name)
                    logger.info(f"Archivo de consistencia geoespacial copiado para {dataset}: {geo_file.name}")
                else:
                    logger.warning("No se encontró archivo de consistencia geoespacial")

            return True
        except Exception as e:
            logger.error(f"Error copiando inconsistencias: {str(e)}")
            return False

    def copy_insumos(self):
        """Copia carpeta de insumos completa"""
        try:
            source_dir = self.temp_files_path / "INSUMOS"
            if not source_dir.exists():
                logger.warning("Directorio INSUMOS no encontrado")
                return False

            dest_dir = self.reports_path / "INSUMOS"
            shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
            logger.info("Carpeta INSUMOS copiada completamente")
            return True
        except Exception as e:
            logger.error(f"Error copiando insumos: {str(e)}")
            return False

    def copy_docx_files(self):
        """Copia archivos .docx a la raíz de Reportes/MODELO_IGAC"""
        try:
            for docx_file in self.temp_files_path.glob("*.docx"):
                shutil.copy2(docx_file, self.reports_path / docx_file.name)
                logger.info(f"Archivo DOCX copiado: {docx_file.name}")
            return True
        except Exception as e:
            logger.error(f"Error copiando archivos DOCX: {str(e)}")
            return False

    def run(self):
        """Ejecuta todo el proceso de organización"""
        logger.info("Iniciando proceso de organización de archivos")
        
        # Primero limpiamos el directorio de reportes
        if not self.clean_reports_directory():
            logger.error("Error al limpiar el directorio de reportes")
            return

        # Crear estructura de directorios
        if not self.create_directory_structure():
            logger.error("Error en la creación de directorios")
            return

        # Ejecutar todos los procesos
        processes = [
            self.copy_gdb_files,
            self.process_omision_comision,
            self.process_topologia,
            self.copy_inconsistencias,
            self.copy_insumos,
            self.copy_docx_files
        ]

        for process in processes:
            process()

        logger.info("Proceso de organización completado")

if __name__ == "__main__":
    organizer = MinisoftOrganizer()
    organizer.run()