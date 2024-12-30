import os
import shutil
from pathlib import Path
import logging
import sys
from PySide6.QtWidgets import QApplication, QMessageBox
import psutil

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MinisoftOrganizer:
    def __init__(self):
        self.root_path = self._get_root_path()
        self.temp_files_path = self.root_path / "Files" / "Temporary_Files" / "MODELO_INTERNO_1_0"
        self.reports_path = self.root_path / "Reportes" / "MODELO_INTERNO_1_0"
        self.DATASETS_TO_PROCESS = self._load_dataset_configuration()
        # Crear instancia de QApplication si no existe
        if not QApplication.instance():
            self.app = QApplication(sys.argv)

    def show_error_dialog(self, message):
        """Muestra un diálogo de error usando PySide6"""
        msg_box = QMessageBox()
        msg_box.setIcon(QMessageBox.Critical)
        msg_box.setWindowTitle("Error al eliminar directorio")
        msg_box.setText(message)
        msg_box.exec()

    def get_process_using_file(self, file_path):
        """Obtiene el proceso que está usando un archivo"""
        for proc in psutil.process_iter(['name', 'pid']):
            try:
                for item in proc.open_files():
                    if str(file_path) in item.path:
                        return proc.name()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return None

    def clean_reports_directory(self):
        """
        Verifica si existe el directorio MODELO_INTERNO_1_0 y lo elimina si existe.
        Muestra un diálogo si hay archivos en uso.
        """
        try:
            # Asegurarse de que exista el directorio base de Reportes
            reports_base = self.root_path / "Reportes"
            if not reports_base.exists():
                reports_base.mkdir(parents=True)
                logger.info(f"Creado directorio base de reportes: {reports_base}")

            # Intentar eliminar solo el directorio MODELO_INTERNO_1_0
            if self.reports_path.exists():
                logger.info(f"Intentando eliminar directorio: {self.reports_path}")
                try:
                    shutil.rmtree(self.reports_path)
                    logger.info("Directorio MODELO_INTERNO_1_0 eliminado exitosamente")
                except PermissionError as e:
                    # Intentar identificar qué archivo está causando el problema
                    for root, dirs, files in os.walk(self.reports_path):
                        for name in files:
                            try:
                                file_path = Path(root) / name
                                os.remove(file_path)
                            except PermissionError:
                                process_name = self.get_process_using_file(file_path)
                                error_msg = (
                                    f"No se puede eliminar el directorio MODELO_INTERNO_1_0 porque\n"
                                    f"el archivo '{file_path.name}' está siendo usado"
                                )
                                if process_name:
                                    error_msg += f"\npor el proceso: {process_name}"
                                
                                logger.error(error_msg)
                                self.show_error_dialog(error_msg)
                                return False

            # Crear el directorio MODELO_INTERNO_1_0
            self.reports_path.mkdir(parents=True)
            logger.info(f"Creado directorio: {self.reports_path}")
            return True
            
        except Exception as e:
            error_msg = f"Error al limpiar el directorio MODELO_INTERNO_1_0: {str(e)}"
            logger.error(error_msg)
            self.show_error_dialog(error_msg)
            return False
    
    
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


    def create_directory_structure(self):
        """Crea la estructura de directorios necesaria"""
        try:
            # Lista base de directorios
            base_directories = [
                "01_GDB",
                "02_TOPOLOGIA",
                "03_VALIDACIONES_CALIDAD",
                "INSUMOS"
            ]
            
            # Crear directorios base
            for dir_name in base_directories:
                dir_path = self.reports_path / dir_name
                dir_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Directorio creado: {dir_path}")
            
            return True
        except Exception as e:
            logger.error(f"Error creando estructura de directorios: {str(e)}")
            return False

    def copy_gdb_files(self):
        """Copia archivos .gdb al directorio 01_GDB"""
        try:
            gdb_files = list(self.temp_files_path.glob("*.gdb"))
            if not gdb_files:
                logger.warning("No se encontraron archivos .gdb para copiar")
                return False
                
            for gdb_file in gdb_files:
                dest_path = self.reports_path / "01_GDB" / gdb_file.name
                if gdb_file.is_dir():
                    shutil.copytree(gdb_file, dest_path, dirs_exist_ok=True)
                else:
                    shutil.copy2(gdb_file, dest_path)
                logger.info(f"Archivo GDB copiado: {gdb_file.name}")
                
            return True
        except Exception as e:
            logger.error(f"Error copiando archivos GDB: {str(e)}")
            return False

    def process_topologia(self):
        """Copia todo el contenido del directorio 02_TOPOLOGIA"""
        try:
            source_dir = self.temp_files_path / "02_TOPOLOGIA"
            if not source_dir.exists():
                logger.warning("Directorio de topología no encontrado")
                return False

            dest_dir = self.reports_path / "02_TOPOLOGIA"
            
            # Copiar todo el contenido del directorio de topología
            if source_dir.exists():
                # Si el directorio destino existe, lo eliminamos primero para asegurar una copia limpia
                if dest_dir.exists():
                    shutil.rmtree(dest_dir)
                
                # Copiar todo el directorio y su contenido
                shutil.copytree(source_dir, dest_dir)
                logger.info(f"Contenido de topología copiado completamente desde: {source_dir}")
                return True
            else:
                logger.warning(f"Directorio origen de topología no encontrado: {source_dir}")
                return False
                
        except Exception as e:
            logger.error(f"Error procesando archivos de topología: {str(e)}")
            return False

    def process_validaciones_calidad(self):
        """Copia todo el contenido del directorio Validaciones_Calidad"""
        try:
            source_dir = self.temp_files_path / "Validaciones_Calidad"
            if not source_dir.exists():
                logger.warning("Directorio Validaciones_Calidad no encontrado")
                return False

            dest_dir = self.reports_path / "03_VALIDACIONES_CALIDAD"
            shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
            logger.info("Contenido de Validaciones_Calidad copiado completamente")
            return True
        except Exception as e:
            logger.error(f"Error copiando Validaciones_Calidad: {str(e)}")
            return False

    def copy_gpkg_files(self):
        """Copia solo archivos .gpkg al directorio INSUMOS"""
        try:
            gpkg_files = list(self.temp_files_path.glob("*.gpkg"))
            if not gpkg_files:
                logger.warning("No se encontraron archivos .gpkg para copiar")
                return False

            dest_dir = self.reports_path / "INSUMOS"
            for gpkg_file in gpkg_files:
                shutil.copy2(gpkg_file, dest_dir / gpkg_file.name)
                logger.info(f"Archivo GPKG copiado: {gpkg_file.name}")
            
            return True
        except Exception as e:
            logger.error(f"Error copiando archivos GPKG: {str(e)}")
            return False

    def copy_docx_files(self):
        """Copia archivos .docx a la raíz de Reportes/MODELO_INTERNO_1_0"""
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
            self.process_topologia,
            self.process_validaciones_calidad,
            self.copy_gpkg_files,
            self.copy_docx_files
        ]

        for process in processes:
            process()

        logger.info("Proceso de organización completado")

if __name__ == "__main__":
    organizer = MinisoftOrganizer()
    organizer.run()