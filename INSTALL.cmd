@echo off
cd /d "%~dp0"
where py >nul 2>nul
if errorlevel 1 (
  echo Install Python 3.10+ from https://www.python.org/downloads/
  pause
  exit /b 1
)
py -3 -m venv .venv
if errorlevel 1 goto failed
.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 goto failed
echo Installed. Run SETUP.cmd next.
pause
exit /b 0
:failed
echo Installation failed. Check the error above.
pause
exit /b 1
