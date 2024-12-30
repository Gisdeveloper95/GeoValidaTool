import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import sys
import threading
import subprocess
import time
import os
from pathlib import Path
from dependency_checker import DependencyChecker

class DependencyInstaller:
    def __init__(self):
        # Definir paquetes que necesitan reinstalación
        self.packages_to_reinstall = {
            'numpy': 'numpy',
            'pandas': 'pandas'
        }
        
        # Definir nuevas dependencias
        self.dependencies = {
            'PySide6': 'PySide6',
            'rich': 'rich',
            'python-docx': 'python-docx',
            'XlsxWriter': 'XlsxWriter'
        }
        
        self.root = tk.Tk()
        self.root.title("Instalación de Dependencias")
        
        # Centrar la ventana
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        window_width = 500
        window_height = 350
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        self.root.geometry(f'{window_width}x{window_height}+{x}+{y}')
        self.root.resizable(False, False)
        
        # Configurar estilo
        self.style = ttk.Style()
        self.style.theme_use('default')
        self.style.configure('Custom.Horizontal.TProgressbar',
                           troughcolor='#E0E0E0',
                           background='#4CAF50')
        
        # Crear la interfaz
        self.create_interface()
        
        self.cancelled = False
        self.installation_success = False

    def create_interface(self):
        """Crea la interfaz gráfica completa"""
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Etiquetas de estado
        self.main_status_label = ttk.Label(
            main_frame,
            text="Preparando instalación...",
            justify=tk.CENTER,
            font=('Arial', 11, 'bold')
        )
        self.main_status_label.pack(pady=(0, 10))
        
        self.current_lib_label = ttk.Label(
            main_frame,
            text="",
            justify=tk.CENTER
        )
        self.current_lib_label.pack(pady=(0, 5))
        
        # Marco para la lista
        self.list_frame = ttk.Frame(main_frame)
        self.list_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Sección de pip
        self.pip_label = ttk.Label(
            self.list_frame,
            text="Actualización inicial:",
            font=('Arial', 9, 'bold')
        )
        self.pip_label.pack(anchor='w')
        
        self.pip_status_label = ttk.Label(
            self.list_frame,
            text="⭕ pip (administrador de paquetes)",
            font=('Arial', 9)
        )
        self.pip_status_label.pack(anchor='w', padx=(20, 0))
        
        ttk.Separator(self.list_frame, orient='horizontal').pack(fill='x', pady=5)
        
        # Sección de reinstalación
        self.reinstall_label = ttk.Label(
            self.list_frame,
            text="Paquetes a reinstalar:",
            font=('Arial', 9, 'bold')
        )
        self.reinstall_label.pack(anchor='w')
        
        # Labels para paquetes a reinstalar
        self.reinstall_labels = {}
        for dep in self.packages_to_reinstall.keys():
            label = ttk.Label(
                self.list_frame,
                text=f"⭕ {dep}",
                font=('Arial', 9)
            )
            label.pack(anchor='w', padx=(20, 0))
            self.reinstall_labels[dep] = label
            
        ttk.Separator(self.list_frame, orient='horizontal').pack(fill='x', pady=5)
        
        # Sección de nuevas dependencias
        self.new_deps_label = ttk.Label(
            self.list_frame,
            text="Nuevas dependencias:",
            font=('Arial', 9, 'bold')
        )
        self.new_deps_label.pack(anchor='w')
        
        # Labels para nuevas dependencias
        self.dependency_labels = {}
        for dep in self.dependencies.keys():
            label = ttk.Label(
                self.list_frame,
                text=f"⭕ {dep}",
                font=('Arial', 9)
            )
            label.pack(anchor='w', padx=(20, 0))
            self.dependency_labels[dep] = label
        
        # Barra de progreso y botón
        self.progress = ttk.Progressbar(
            main_frame,
            style='Custom.Horizontal.TProgressbar',
            length=400,
            mode='determinate'
        )
        self.progress.pack(pady=(0, 10))
        
        self.cancel_button = ttk.Button(
            main_frame,
            text="Cancelar",
            command=self.cancel_installation
        )
        self.cancel_button.pack()

    def update_pip(self):
        """Actualiza pip a la última versión"""
        python_path = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
        self.pip_status_label.configure(text="🔄 pip (actualizando...)")
        process = subprocess.Popen(
            [python_path, "-m", "pip", "install", "--upgrade", "pip"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            self.pip_status_label.configure(text="✅ pip (actualizado)")
            return True
        else:
            self.pip_status_label.configure(text="❌ pip (error al actualizar)")
            return False

    def run_pip_command(self, command, package_name):
        """Ejecuta un comando pip"""
        python_path = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
        process = subprocess.Popen(
            [python_path, "-m", "pip"] + command.split() + [package_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        return process.returncode == 0

    def update_status(self, package, status, is_reinstall=False):
        """Actualiza el estado visual de un paquete"""
        labels = self.reinstall_labels if is_reinstall else self.dependency_labels
        icons = {
            "pending": "⭕",
            "uninstalling": "🗑️",
            "installing": "🔄",
            "success": "✅",
            "error": "❌"
        }
        labels[package].configure(text=f"{icons[status]} {package}")

    def run_installation(self):
        """Ejecuta el proceso de instalación completo"""
        try:
            # Actualizar pip primero
            self.main_status_label.config(text="Actualizando pip...")
            self.current_lib_label.config(text="Esto puede tomar unos momentos")
            self.progress['value'] = 5
            
            if not self.update_pip():
                raise Exception("Error al actualizar pip")
            
            # Calcular total de pasos
            total_steps = len(self.packages_to_reinstall) * 2 + len(self.dependencies)
            completed_steps = 0
            self.progress['value'] = 10
            
            # Desinstalar pandas y numpy si es necesario
            if self.packages_to_reinstall:
                for package in self.packages_to_reinstall:
                    if self.cancelled:
                        break
                    
                    self.main_status_label.config(text="Desinstalando paquetes...")
                    self.current_lib_label.config(text=f"Eliminando {package}...")
                    self.update_status(package, "uninstalling", True)
                    
                    if self.run_pip_command("uninstall -y", package):
                        completed_steps += 1
                        self.progress['value'] = 10 + (completed_steps / total_steps) * 90
                    else:
                        self.update_status(package, "error", True)
                        raise Exception(f"Error al desinstalar {package}")
                
                # Reinstalar numpy y pandas en orden
                for package in self.packages_to_reinstall:
                    if self.cancelled:
                        break
                    
                    self.main_status_label.config(text="Reinstalando paquetes...")
                    self.current_lib_label.config(text=f"Instalando {package}...")
                    self.update_status(package, "installing", True)
                    
                    if self.run_pip_command("install", package):
                        self.update_status(package, "success", True)
                        completed_steps += 1
                        self.progress['value'] = 10 + (completed_steps / total_steps) * 90
                    else:
                        self.update_status(package, "error", True)
                        raise Exception(f"Error al instalar {package}")
            
            # Instalar nuevas dependencias
            for dep_name, package in self.dependencies.items():
                if self.cancelled:
                    break
                
                self.main_status_label.config(text="Instalando nuevas dependencias...")
                self.current_lib_label.config(text=f"Instalando {dep_name}...")
                self.update_status(dep_name, "installing")
                
                if self.run_pip_command("install", package):
                    self.update_status(dep_name, "success")
                    completed_steps += 1
                    self.progress['value'] = 10 + (completed_steps / total_steps) * 90
                else:
                    self.update_status(dep_name, "error")
                    raise Exception(f"Error al instalar {dep_name}")
            
            if not self.cancelled:
                self.installation_success = True
                self.main_status_label.config(text="¡Instalación completada con éxito!")
                self.current_lib_label.config(text="Todas las dependencias están instaladas")
                self.cancel_button.config(state='disabled')
                self.progress['value'] = 100
                
                time.sleep(1)
                self.restart_application()
                
        except Exception as e:
            if not self.cancelled:
                messagebox.showerror(
                    "Error de Instalación",
                    f"Error durante la instalación:\n{str(e)}\n\nPor favor, contacte al soporte técnico."
                )
            self.root.quit()

    def restart_application(self):
        """Reinicia la aplicación principal"""
        python_path = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Main.py")
        
        self.main_status_label.config(text="Reiniciando aplicación...")
        self.current_lib_label.config(text="Por favor espere...")
        self.root.update()
        
        time.sleep(1)
        self.root.destroy()
        
        # Crear script temporal de reinicio
        restart_script = """
import subprocess
import os
import time
import sys

def cleanup():
    try:
        # Intenta eliminar este script temporal
        os.remove(__file__)
    except:
        pass

try:
    time.sleep(1)
    python_path = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
    script_path = r"{}"
    subprocess.Popen([python_path, script_path])
    cleanup()
except Exception as e:
    print(f"Error al reiniciar: {{str(e)}}")
    cleanup()
    sys.exit(1)
        """.format(script_path.replace("\\", "\\\\"))
        
        restart_script_path = os.path.join(os.path.dirname(script_path), "restart_temp.py")
        with open(restart_script_path, "w") as f:
            f.write(restart_script)
        
        subprocess.Popen([python_path, restart_script_path])
        sys.exit(0)

    def start_installation(self):
        """Inicia el proceso de instalación en un hilo separado"""
        install_thread = threading.Thread(target=self.run_installation)
        install_thread.start()
        self.root.mainloop()
        return 0 if self.installation_success else 1

    def cancel_installation(self):
        """Cancela el proceso de instalación"""
        self.cancelled = True
        self.root.quit()

def show_detailed_status(status_dict):
    """Muestra una ventana con el estado detallado de las dependencias"""
    status_window = tk.Tk()
    status_window.title("Estado Detallado de Dependencias")
    status_window.geometry("400x300")
    
    text = tk.Text(status_window, wrap=tk.WORD, padx=10, pady=10)
    text.pack(fill=tk.BOTH, expand=True)
    
    text.insert(tk.END, "Estado de Dependencias:\n\n")
    text.insert(tk.END, f"Archivo de configuración existe: {status_dict['config_file_exists']}\n")
    text.insert(tk.END, f"Archivo de configuración válido: {status_dict['config_file_valid']}\n")
    text.insert(tk.END, f"Versiones correctas: {status_dict['versions_correct']}\n\n")
    
    text.insert(tk.END, "Versiones Actuales:\n")
    for pkg, ver in status_dict['current_versions'].items():
        text.insert(tk.END, f"{pkg}: {ver}\n")
    
    text.insert(tk.END, "\nVersiones Requeridas:\n")
    for pkg, ver in status_dict['required_versions'].items():
        text.insert(tk.END, f"{pkg}: {ver}\n")
    
    text.insert(tk.END, f"\nEntorno ArcGIS Pro: {status_dict['arcgis_environment']}")
    
    text.config(state='disabled')
    
    status_window.mainloop()

if __name__ == "__main__":
    # Verificar si estamos en modo debug
    debug_mode = "--debug" in sys.argv
    
    # Crear el verificador de dependencias
    checker = DependencyChecker()
    
    # Si estamos en modo debug, mostrar estado detallado
    if debug_mode:
        show_detailed_status(checker.get_detailed_status())
        sys.exit(0)
    
    # Verificar el entorno de ArcGIS Pro
    if not checker.verify_arcgis_environment():
        messagebox.showerror(
            "Error de Entorno",
            "Esta aplicación debe ejecutarse desde el entorno de Python de ArcGIS Pro."
        )
        sys.exit(1)
    
    # Verificar estado de instalación
    needs_reinstall = not checker.check_installation_status()
    
    # Preparar mensaje según el estado
    if needs_reinstall:
        packages_message = (
            "Se requiere realizar las siguientes acciones:\n\n"
            "1. Actualizar pip a la última versión\n"
            "2. Reinstalar pandas y numpy (requerido solo la primera vez)\n"
            "3. Instalar nuevos componentes:\n"
            "   - PySide6 (Interfaz gráfica)\n"
            "   - rich (Formato de texto)\n"
            "   - python-docx (Manejo de documentos)\n"
            "   - XlsxWriter (Manejo de Excel)\n\n"
            "¿Desea proceder con la instalación?"
        )
    else:
        packages_message = (
            "Se requiere instalar los siguientes componentes:\n\n"
            "1. Actualizar pip a la última versión\n"
            "2. Instalar nuevos componentes:\n"
            "   - PySide6 (Interfaz gráfica)\n"
            "   - rich (Formato de texto)\n"
            "   - python-docx (Manejo de documentos)\n"
            "   - XlsxWriter (Manejo de Excel)\n\n"
            "¿Desea proceder con la instalación?"
        )

    root = tk.Tk()
    root.withdraw()
    
    if messagebox.askyesno("Instalación Requerida", packages_message, icon='info'):
        root.destroy()
        
        installer = DependencyInstaller()
        # Si no necesitamos reinstalar, vaciar la lista de reinstalación
        if not needs_reinstall:
            installer.packages_to_reinstall = {}
        
        result = installer.start_installation()
        
        # Si la instalación fue exitosa, marcar como completada
        if result == 0 and needs_reinstall:
            checker.mark_installation_complete()
        
        sys.exit(result)
    else:
        root.destroy()
        sys.exit(1)