@echo off
SETLOCAL ENABLEDELAYEDEXPANSION
SET "SCRIPT_DIR=%~dp0"
SET "REPO_ROOT=%SCRIPT_DIR%.."
SET "VENV_PY=%REPO_ROOT%\.venv\Scripts\python.exe"
SET "SCRIPT_NAME=%~n0.py"
SET "SCRIPT_PATH=%SCRIPT_DIR%%SCRIPT_NAME%"

echo -----------------------------------------
echo Running %SCRIPT_NAME%
echo Script  : "%SCRIPT_PATH%"
echo venv    : "%VENV_PY%"
echo Args    : %*
echo -----------------------------------------

IF NOT EXIST "%SCRIPT_PATH%" exit /B 1
IF NOT EXIST "%VENV_PY%" exit /B 1

"%VENV_PY%" "%SCRIPT_PATH%" %*
IF ERRORLEVEL 1 exit /B %ERRORLEVEL%
echo [INFO] %SCRIPT_NAME% completed successfully.
exit /B 0
