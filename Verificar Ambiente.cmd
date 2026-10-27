@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    echo Execute Instalar.cmd primeiro.
    pause
    exit /b 1
)
".venv\Scripts\python.exe" verificar_ambiente.py --integridade
set "RESULTADO=%ERRORLEVEL%"
pause
exit /b %RESULTADO%
