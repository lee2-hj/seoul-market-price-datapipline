@echo off
setlocal EnableDelayedExpansion

REM ==========================================================================
REM  run_main_mart.bat
REM  Silver(Iceberg: dim_apartment, fact_apt_transactions) -> Gold
REM  Backfill script for the top-priority Gold mart (dm_main, main_mart.py).
REM  main_mart.py accepts a single BASE_DATE and aggregates the trailing 90
REM  days from it. This script prompts once for BASE_DATE (blank = today,
REM  handled by main_mart.py's own default) and runs a single Spark job.
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
set "PIPELINE_SCRIPT=%PROJECT_ROOT%\src\transformation\gold\main_mart.py"

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

REM The project path contains non-ASCII characters and a space, which breaks
REM Spark's Windows launcher scripts (spark-class2.cmd's java classpath and
REM temp-file round-trip), causing a Could-not-find-or-load-main-class error
REM for SparkSubmit. Work around this by pointing SPARK_HOME at an ASCII-only
REM directory junction (an alias, not a copy, so it always reflects the
REM current venv contents even after pyspark is reinstalled).
set "SPARK_HOME=%LOCALAPPDATA%\dataengineer_spark_home"
if not exist "%SPARK_HOME%" (
    mklink /J "%SPARK_HOME%" "%PROJECT_ROOT%\.venv\Lib\site-packages\pyspark" >nul
)

REM When Spark downloads the Iceberg/Hadoop-AWS jars via spark.jars.packages,
REM it chmods them on Windows, which needs winutils.exe under HADOOP_HOME\bin.
REM Fetch it once from a community mirror (cdarlint/winutils) if missing.
set "HADOOP_HOME=%LOCALAPPDATA%\hadoop"
set "PATH=%HADOOP_HOME%\bin;%PATH%"
if not exist "%HADOOP_HOME%\bin\winutils.exe" (
    echo [SETUP] Downloading winutils.exe for the first time...
    mkdir "%HADOOP_HOME%\bin" >nul 2>&1
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri 'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe' -OutFile '%HADOOP_HOME%\bin\winutils.exe' -UseBasicParsing; Invoke-WebRequest -Uri 'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll' -OutFile '%HADOOP_HOME%\bin\hadoop.dll' -UseBasicParsing"
)

REM --- [2] MinIO(S3) credentials / bucket: verify env/.env values exist ---
REM     main_mart.py loads env/.env itself via load_dotenv() (override=False),
REM     so this step isn't strictly required for the run itself, but if it
REM     fails only after Spark has already started up, diagnosing the cause
REM     takes much longer - so the required values (S3_*, LAKE) are
REM     pre-checked here. KAKAO_MAP_REST_API_KEY is not required (the script
REM     just logs a warning and continues with latitude/longitude left NULL),
REM     so it is excluded from the required check.
set "ENV_LOADER_SCRIPT=%TEMP%\_main_mart_env_%RANDOM%.py"

echo import os> "%ENV_LOADER_SCRIPT%"
echo from dotenv import dotenv_values>> "%ENV_LOADER_SCRIPT%"
echo env_path = os.path.join(os.environ["PROJECT_ROOT"], "env", ".env")>> "%ENV_LOADER_SCRIPT%"
echo values = dotenv_values(env_path)>> "%ENV_LOADER_SCRIPT%"
echo for key in ["S3_END_POINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "LAKE", "KAKAO_MAP_REST_API_KEY"]:>> "%ENV_LOADER_SCRIPT%"
echo     val = values.get(key)>> "%ENV_LOADER_SCRIPT%"
echo     print(key + "=" + (val if val is not None else ""))>> "%ENV_LOADER_SCRIPT%"

set "S3_END_POINT="
set "S3_ACCESS_KEY="
set "S3_SECRET_KEY="
set "LAKE="
set "KAKAO_MAP_REST_API_KEY="
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
if not defined KAKAO_MAP_REST_API_KEY (
    echo [WARN] KAKAO_MAP_REST_API_KEY is not set - continuing without geocoding
    echo        ^(latitude/longitude will be NULL^).
    echo.
)

REM --- [3] Prompt for the single BASE_DATE to run (blank = today) ---
echo Enter the base_date to run for the main mart (dm_main).
echo Format: YYYY-MM-DD (e.g. 2026-08-24). It aggregates the trailing 90 days from it.
echo Leave blank and press Enter to use today's date (main_mart.py's own default).
echo.

set "BASE_DATE="
set /p BASE_DATE=Base date (blank = today):
echo.

if "%BASE_DATE%"=="" (
    echo ==========================================================
    echo  Starting main mart (dm_main) run for TODAY (default).
    echo ==========================================================
    echo.
    "%PYTHON_EXE%" "%PIPELINE_SCRIPT%"
) else (
    echo ==========================================================
    echo  Starting main mart (dm_main) run for base_date=%BASE_DATE%.
    echo  Writes to s3a://LAKE/mart/dm_main/base_date=%BASE_DATE% (overwrite mode).
    echo ==========================================================
    echo.
    "%PYTHON_EXE%" "%PIPELINE_SCRIPT%" %BASE_DATE%
)

if errorlevel 1 (
    echo.
    echo [FAILED] An error occurred while running the main mart.
    pause
    exit /b 1
)

echo.
echo [DONE] Main mart (dm_main) run finished.
pause
endlocal
