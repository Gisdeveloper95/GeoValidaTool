@echo off
REM Definir la ruta del entorno de Python específico
set PYTHON_PATH="C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

REM Actualizar numpy y pandas
%PYTHON_PATH% -m pip install numpy==1.24.3
%PYTHON_PATH% -m pip install pandas=2.0.3

REM Instalar otras librerías
%PYTHON_PATH% -m pip install PySide6
%PYTHON_PATH% -m pip install rich
%PYTHON_PATH% -m pip install python-docx
%PYTHON_PATH% -m pip install XlsxWriter

echo Instalación completada.
pause
