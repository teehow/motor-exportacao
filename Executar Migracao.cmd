@echo off
setlocal
if not exist "%~dp0.venv\Scripts\python.exe" (
    echo Execute Instalar.cmd primeiro.
    exit /b 1
)
set "PYTHONUTF8=1"
"%~dp0.venv\Scripts\python.exe" "%~dp0motor\executar_migracao.py" %*
exit /b %ERRORLEVEL%
