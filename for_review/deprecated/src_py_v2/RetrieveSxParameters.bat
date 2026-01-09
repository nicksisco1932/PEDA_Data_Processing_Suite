@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

REM -----------------------------------------------------------------
REM  RetrieveSxParameters.bat
REM  Thin wrapper to run RetrieveSxParameters.py inside the .venv
REM -----------------------------------------------------------------

REM Resolve script directory (src_py_v2\)
SET "SCRIPT_DIR=%~dp0"

REM Project root is one level up from src_py_v2
SET "PROJ_ROOT=%SCRIPT_DIR%\.."

REM Path to venv python
SET "VENV_PY=%PROJ_ROOT%\.venv\Scripts\python.exe"

IF NOT EXIST "%VENV_PY%" (
    ECHO [ERROR] Virtual environment python not found:
    ECHO        "%VENV_PY%"
    EXIT /B 1
)

ECHO -----------------------------------------
ECHO Running RetrieveSxParameters.py
ECHO Script  : "%SCRIPT_DIR%RetrieveSxParameters.py"
ECHO venv    : "%VENV_PY%"
ECHO Args    : %*
ECHO -----------------------------------------

"%VENV_PY%" "%SCRIPT_DIR%RetrieveSxParameters.py" %*
SET "EXITCODE=%ERRORLEVEL%"

IF NOT "%EXITCODE%"=="0" (
    ECHO [ERROR] RetrieveSxParameters.py exited with code %EXITCODE%
    EXIT /B %EXITCODE%
)

ECHO [INFO] RetrieveSxParameters.py completed successfully.
ENDLOCAL
