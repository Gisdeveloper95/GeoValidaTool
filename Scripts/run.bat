@echo off
title GeoValidaTool
echo Iniciando GeoValidaTool...

:: Ejecutar el launcher en lugar de Main.py directamente
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" "%~dp0launcher.py"

if errorlevel 1 (
    echo.
    echo Se encontró un error al ejecutar la aplicación.
    echo Por favor, contacte al soporte técnico si el problema persiste.
    pause
    exit /b 1
)