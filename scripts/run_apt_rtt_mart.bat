@echo off
setlocal EnableDelayedExpansion

REM ==========================================================================
REM  run_apt_att_mart.bat
REM  Runs the apartment trade-trend Gold mart script (apt_rtt_mart.py).
REM  apt_rtt_mart.py takes a single optional AS_OF_DATE argument (YYYY-MM-DD)
REM  and re-checks the trailing 90 days from that date (today if omitted),
REM  upserting only the day-partitions (base_date=YYYY-MM-DD, one per actual
REM  deal_date) that are new or changed - unchanged partitions are skipped.
REM  NOTE: all user-facing messages are in English on purpose (not Korean) -
REM  this project's Windows/cmd.exe environment has shown repeated, inconsistent
REM  parsing corruption with Korean text in .bat files regardless of the file
REM  encoding tried (UTF-8 no BOM, UTF-8+BOM, CP949 native), so plain ASCII is
REM  used here to avoid that class of bug entirely. Do not add non-ASCII text
REM  back into this file.
REM ==========================================================================

REM --- [1] Project paths and venv python executable (relative to scripts\) ---
set "PROJECT_ROOT=%~dp0.."
set "PYTHON_EXE=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "PIPELINE_SCRIPT=%PROJECT_ROOT%\src\transformation\gold\apt_rtt_mart.py"

if not exist "%PYTHON_EXE%" (
    echo [ERROR] Cannot find the virtual environment Python: %PYTHON_EXE%
    pause
    exit /b 1
)

if not exist "%PIPELINE_SCRIPT%" (
    echo [ERROR] Cannot find the target script: %PIPELINE_SCRIPT%
    pause
    exit /b 1
)

REM --- [2] MinIO(S3) credentials / bucket: verify env/.env values exist ---
REM     apt_rtt_mart.py loads env/.env itself via load_config() (override=False),
REM     so this step isn't strictly required for the run itself, but if it fails
REM     only after the script has already started up, diagnosing the cause takes
REM     much longer - so the required values are pre-checked here.
set "ENV_LOADER_SCRIPT=%TEMP%\_apt_rtt_mart_env_%RANDOM%.py"

echo import os> "%ENV_LOADER_SCRIPT%"
echo from dotenv import dotenv_values>> "%ENV_LOADER_SCRIPT%"
echo env_path = os.path.join(os.environ["PROJECT_ROOT"], "env", ".env")>> "%ENV_LOADER_SCRIPT%"
echo values = dotenv_values(env_path)>> "%ENV_LOADER_SCRIPT%"
echo for key in ["S3_END_POINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "LAKE"]:>> "%ENV_LOADER_SCRIPT%"
echo     val = values.get(key)>> "%ENV_LOADER_SCRIPT%"
echo     print(key + "=" + (val if val is not None else ""))>> "%ENV_LOADER_SCRIPT%"

set "S3_END_POINT="
set "S3_ACCESS_KEY="
set "S3_SECRET_KEY="
set "LAKE="
for /f "usebackq tokens=1,* delims==" %%A in (`""%PYTHON_EXE%" "%ENV_LOADER_SCRIPT%""`) do set "%%A=%%B"
del "%ENV_LOADER_SCRIPT%" >nul 2>&1

if not defined S3_END_POINT goto :ENV_LOAD_FAILED
if not defined S3_ACCESS_KEY goto :ENV_LOAD_FAILED
if not defined S3_SECRET_KEY goto :ENV_LOAD_FAILED
if not defined LAKE goto :ENV_LOAD_FAILED
goto :ENV_LOAD_OK

:ENV_LOAD_FAILED
echo [ERROR] Could not find S3_END_POINT / S3_ACCESS_KEY / S3_SECRET_KEY / LAKE in env\.env.
echo         Please check that %PROJECT_ROOT%\env\.env has all of these values set.
pause
exit /b 1

:ENV_LOAD_OK

REM --- [3] Prompt for the anchor date (AS_OF_DATE) ---
echo Enter the anchor date (AS_OF_DATE) for the apartment trade-trend mart (RTT).
echo apt_rtt_mart.py rechecks the trailing 90 days from this date and upserts only
echo the day-partitions that are new or changed (unchanged partitions are skipped).
echo Format: YYYY-MM-DD (e.g. 2026-08-20).
echo Leave blank and press Enter to use today's date as the anchor.
echo.

set "AS_OF_DATE="
set /p AS_OF_DATE=Anchor date (e.g. 2026-08-20):
echo.

if "%AS_OF_DATE%"=="" (
    echo No date entered - running with today's date as the anchor...
    echo.
    "%PYTHON_EXE%" "%PIPELINE_SCRIPT%"
) else (
    echo Running with anchor date %AS_OF_DATE% ^(rechecking last 90 days from this date^)...
    echo.
    "%PYTHON_EXE%" "%PIPELINE_SCRIPT%" %AS_OF_DATE%
)

if errorlevel 1 (
    echo.
    echo [FAILED] An error occurred while running the apartment trade-trend mart ^(RTT^).
) else (
    echo.
    echo [DONE] Apartment trade-trend mart ^(RTT^) run finished.
)

pause
endlocal
