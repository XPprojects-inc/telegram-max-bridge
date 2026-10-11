@echo off
cd /d "%~dp0"
set PYTHONUTF8=1
if not exist .venv\Scripts\python.exe (
  echo Run INSTALL.cmd first.
  pause
  exit /b 1
)
.venv\Scripts\python.exe bridge.py run
pause
