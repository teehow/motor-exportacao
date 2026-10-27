@echo off
setlocal
cd /d "%~dp0"
set "PYTHONUTF8=1"
echo Instalacao do Painel de Migracao Bex
echo.
if exist ".venv\Scripts\python.exe" goto dependencias
where py >nul 2>&1
if errorlevel 1 goto usar_python
py -3 -c "import sys,struct,tkinter; sys.exit(0 if sys.version_info >= (3,12) and struct.calcsize('P') == 8 else 1)"
if errorlevel 1 goto python_invalido
py -3 -m venv ".venv"
if errorlevel 1 goto erro
goto dependencias
:usar_python
python -c "import sys,struct,tkinter; sys.exit(0 if sys.version_info >= (3,12) and struct.calcsize('P') == 8 else 1)"
if errorlevel 1 goto python_invalido
python -m venv ".venv"
if errorlevel 1 goto erro
:dependencias
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto erro
".venv\Scripts\python.exe" verificar_ambiente.py
if errorlevel 1 goto erro
echo.
echo Instalacao concluida. Abra Abrir Painel Migracao.cmd.
pause
exit /b 0
:python_invalido
echo Instale Python 3.12 ou superior, 64 bits, incluindo Tcl/Tk e o launcher py.
echo Depois execute este arquivo novamente.
pause
exit /b 1
:erro
echo.
echo A instalacao nao foi concluida. Confira o erro acima.
pause
exit /b 1
