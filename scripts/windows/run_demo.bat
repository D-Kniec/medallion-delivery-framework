@echo off
SETLOCAL EnableDelayedExpansion

cd /d "%~dp0..\.."

set "PROJECT_ROOT=%CD%"
set "PYTHONPATH=%PROJECT_ROOT%"

echo ==========================================
echo   MEDALLION DELIVERY FRAMEWORK 
echo ==========================================
echo Root: %PROJECT_ROOT%

python scripts/orchestrator.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Orchestrator failed or Python is not in PATH.
    pause
)

ENDLOCAL