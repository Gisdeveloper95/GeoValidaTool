@echo off
REM Definir la ruta del entorno de Python específico
set PYTHON_PATH="C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

REM Ejecutar el script Main.py en el mismo directorio
%PYTHON_PATH% "%~dp0Main.py"

pause
