@echo off
echo ===================================================
echo   Compiling JudiQ AI Standalone Windows Executable
echo ===================================================
echo.
cd /d "%~dp0"
python build_exe.py
pause
