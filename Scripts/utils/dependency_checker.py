import pkg_resources
import subprocess
import sys
import os
import tkinter as tk
from tkinter import ttk, messagebox
import threading
import sys
sys.stdout.reconfigure(encoding='utf-8')
class InstallProgressWindow:
   def __init__(self, required_packages, title="Instalación de Dependencias   ~ dev by andres.osorio@igac.gov.co"):
       # Asegurar una sola instancia de Tk
       if tk._default_root:
           for widget in tk._default_root.winfo_children()[:]:
               widget.destroy()
           self.root = tk._default_root
           self.root.deiconify()
       else:
           self.root = tk.Tk()
           
       self.root.title(title)
       
       # Centrar la ventana
       window_width = 500
       window_height = 350
       screen_width = self.root.winfo_screenwidth()
       screen_height = self.root.winfo_screenheight()
       x = (screen_width - window_width) // 2
       y = (screen_height - window_height) // 2
       self.root.geometry(f'{window_width}x{window_height}+{x}+{y}')
       
       # Marco principal
       main_frame = ttk.Frame(self.root, padding="20")
       main_frame.pack(fill=tk.BOTH, expand=True)
       
       # Etiqueta de estado
       self.status_label = ttk.Label(
           main_frame,
           text="Verificando dependencias...",
           font=('Arial', 11, 'bold')
       )
       self.status_label.pack(pady=(0, 10))
       
       # Lista de paquetes
       self.package_labels = {}
       for package, version in required_packages.items():
           label = ttk.Label(
               main_frame,
               text=f"⭕ {package} {'(' + version + ')' if version else '(última versión)'}"
           )
           label.pack(anchor='w', padx=(20, 0))
           self.package_labels[package] = label
       
       # Barra de progreso
       self.progress = ttk.Progressbar(
           main_frame,
           length=400,
           mode='determinate'
       )
       self.progress.pack(pady=(10, 10))
       
       # Botón cancelar
       self.cancel_button = ttk.Button(
           main_frame,
           text="Cancelar",
           command=self.safe_cancel
       )
       self.cancel_button.pack()
       
       self.cancelled = False
       self.root.protocol("WM_DELETE_WINDOW", self.safe_cancel)

   def safe_cancel(self):
       """Cancela de forma segura"""
       if messagebox.askyesno("Confirmar", "¿Desea cancelar la instalación?"):
           self.cancelled = True
           self.status_label['text'] = "Cancelando..."
           self.cancel_button.configure(state='disabled')
           self.root.after(1000, self.close)
   
   def update_package_status(self, package, status, version=None):
       """Actualiza el estado visual de un paquete"""
       if not self.root:
           return
           
       try:
           icons = {
               "checking": "🔍",
               "installing": "🔄",
               "success": "✅", 
               "error": "❌",
               "skip": "⏭️"
           }
           version_text = f" ({version})" if version else ""
           self.package_labels[package].configure(
               text=f"{icons.get(status, '⭕')} {package}{version_text}"
           )
           self.root.update_idletasks()
           self.root.update()
       except tk.TclError:
           pass
   
   def update_progress(self, value):
       """Actualiza la barra de progreso"""
       if not self.root:
           return
       try:
           self.progress['value'] = value
           self.root.update_idletasks() 
           self.root.update()
       except tk.TclError:
           pass

   def close(self):
       """Cierra la ventana correctamente"""
       try:
           if self.root:
               self.root.withdraw()
               for widget in self.root.winfo_children():
                   widget.destroy()
               if not self.cancelled:
                   self.root.quit()
       except:
           pass
           
   def start_installation(self):
       """Inicia la instalación en hilo separado"""
       self.install_thread = threading.Thread(target=self.run_installation)
       self.install_thread.daemon = True
       self.install_thread.start()
       self.root.mainloop()
       return self.cancelled

class DependencyChecker:
    def __init__(self, parent=None):
        self.parent = parent
        self.python_path = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
        self.required_versions = {
            'numpy': '1.24.3',
            'pandas': '2.0.3',
            'PySide6': None,
            'rich': None,
            'python-docx': None,
            'XlsxWriter': None
            #'qt_material': None
        }
    
    def get_installed_version(self, package):
        try:
            return pkg_resources.get_distribution(package).version
        except pkg_resources.DistributionNotFound:
            return None
    
    def check_and_install_dependencies(self):
        try:
            window = InstallProgressWindow(self.required_versions)
            total_steps = len(self.required_versions)
            current_step = 0

            for package, required_version in self.required_versions.items():
                if window.cancelled:
                    break

                window.update_package_status(package, "checking")
                current_version = self.get_installed_version(package)

                if current_version is None or (required_version and current_version != required_version):
                    window.update_package_status(package, "installing")
                    cmd = [self.python_path, "-m", "pip", "install"]
                    if required_version:
                        cmd.append(f"{package}=={required_version}")
                    else:
                        cmd.append(package)
                        
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        window.update_package_status(package, "success")
                    else:
                        window.update_package_status(package, "error")
                        raise Exception(f"Error instalando {package}")
                else:
                    window.update_package_status(package, "skip")

                current_step += 1
                window.update_progress((current_step / total_steps) * 100)

            window.close()
            return True

        except Exception as e:
            if 'window' in locals():
                window.close()
            return False

    def check_installation_status(self):
        """Verifica si todas las dependencias están instaladas con las versiones correctas"""
        for package, required_version in self.required_versions.items():
            current_version = self.get_installed_version(package)
            if current_version is None:
                return False
            if required_version and current_version != required_version:
                return False
        return True

if __name__ == "__main__":
    checker = DependencyChecker()
    checker.check_and_install_dependencies()