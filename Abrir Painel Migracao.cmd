@echo off
setlocal
set "PYTHON=%~dp0.venv\Scripts\pythonw.exe"
if not exist "%PYTHON%" (
    echo Execute Instalar.cmd antes de abrir o painel.
    pause
    exit /b 1
)
set "PYTHONUTF8=1"
start "" "%PYTHON%" "%~dp0painel_migracao\app.py"
