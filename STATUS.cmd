@echo off
cd /d "%~dp0"
set PYTHONUTF8=1
.venv\Scripts\python.exe bridge.py status
pause
