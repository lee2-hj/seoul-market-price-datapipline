@echo off
setlocal EnableDelayedExpansion

REM ==========================================================================
REM  run_apt_recent_trade_mart.bat
REM  Silver(Iceberg: dim_apartment, fact_apt_transactions) -> Gold
REM  Backfill script for the apartment recent-trade mart (dm_apt_recent_trade).
REM  build_apt_recent_trade_mart.py only accepts a single BASE_DATE and aggregates
REM  the trailing 90 days from it, so for a given start/end date range this script
REM  loops day by day and re-runs the script (same pattern as run_dong_mart.bat).
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
set "PIPELINE_SCRIPT=%PROJECT_ROOT%\src\transformation\gold\build_apt_recent_trade_mart.py"

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

REM --- [2] MinIO(S3) credentials / bucket / data.go.kr API key: verify env/.env values exist ---
REM     build_apt_recent_trade_mart.py loads env/.env itself via load_config()
REM     (override=False), so this step isn't strictly required for the run itself,
REM     but if it fails only after Spark has already started up, diagnosing the
REM     cause takes much longer - so the required values (S3_*, LAKE) are
REM     pre-checked here. DATA_GO_KR_KEY is not required (the script just logs a
REM     warning and continues with household_count/use_approval_date left NULL),
REM     so it is excluded from the required check.
set "ENV_LOADER_SCRIPT=%TEMP%\_apt_recent_trade_env_%RANDOM%.py"

echo import os> "%ENV_LOADER_SCRIPT%"
echo from dotenv import dotenv_values>> "%ENV_LOADER_SCRIPT%"
echo env_path = os.path.join(os.environ["PROJECT_ROOT"], "env", ".env")>> "%ENV_LOADER_SCRIPT%"
echo values = dotenv_values(env_path)>> "%ENV_LOADER_SCRIPT%"
echo for key in ["S3_END_POINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "LAKE", "DATA_GO_KR_KEY"]:>> "%ENV_LOADER_SCRIPT%"
echo     val = values.get(key)>> "%ENV_LOADER_SCRIPT%"
echo     print(key + "=" + (val if val is not None else ""))>> "%ENV_LOADER_SCRIPT%"

set "S3_END_POINT="
set "S3_ACCESS_KEY="
set "S3_SECRET_KEY="
set "LAKE="
set "DATA_GO_KR_KEY="
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
if not defined DATA_GO_KR_KEY (
    echo [WARN] DATA_GO_KR_KEY is not set - continuing without the BldRgstHub API lookup
    echo        ^(household_count/use_approval_date will be NULL^).
    echo.
)

REM --- [3] Prompt for the backfill target period (base_date range) ---
echo Enter the base_date range to backfill for the apartment recent-trade mart (dm_apt_recent_trade).
echo Format: YYYY-MM-DD (e.g. 2026-07-01) - each base_date aggregates the trailing 90 days from it.
echo.

set "START_DATE="
set "END_DATE="
set /p START_DATE=Start date (e.g. 2026-07-01):
set /p END_DATE=End date   (e.g. 2026-08-12):
echo.

if "%START_DATE%"=="" (
    echo [ERROR] You must enter a start date.
    pause
    exit /b 1
)
if "%END_DATE%"=="" (
    echo [ERROR] You must enter an end date.
    pause
    exit /b 1
)

REM --- [4] Build the list of dates in the start~end range (computed via Python datetime) ---
set "DATE_LIST_SCRIPT=%TEMP%\_apt_recent_trade_dates_%RANDOM%.py"

echo import sys> "%DATE_LIST_SCRIPT%"
echo from datetime import datetime, timedelta>> "%DATE_LIST_SCRIPT%"
echo start = datetime.strptime(sys.argv[1], "%%Y-%%m-%%d").date()>> "%DATE_LIST_SCRIPT%"
echo end = datetime.strptime(sys.argv[2], "%%Y-%%m-%%d").date()>> "%DATE_LIST_SCRIPT%"
echo if start ^> end:>> "%DATE_LIST_SCRIPT%"
echo     sys.stderr.write("START_AFTER_END\n")>> "%DATE_LIST_SCRIPT%"
echo     sys.exit(1)>> "%DATE_LIST_SCRIPT%"
echo d = start>> "%DATE_LIST_SCRIPT%"
echo while d ^<= end:>> "%DATE_LIST_SCRIPT%"
echo     print(d.strftime("%%Y-%%m-%%d"))>> "%DATE_LIST_SCRIPT%"
echo     d += timedelta(days=1)>> "%DATE_LIST_SCRIPT%"

set "DATE_LIST_FILE=%TEMP%\_apt_recent_trade_date_list_%RANDOM%.txt"
"%PYTHON_EXE%" "%DATE_LIST_SCRIPT%" %START_DATE% %END_DATE% > "%DATE_LIST_FILE%" 2>"%TEMP%\_apt_recent_trade_dates_err.txt"
set "DATE_LIST_RESULT=%ERRORLEVEL%"
del "%DATE_LIST_SCRIPT%" >nul 2>&1

if not "%DATE_LIST_RESULT%"=="0" (
    echo [ERROR] Invalid date range. Check the input format ^(YYYY-MM-DD^) and that start ^<= end.
    del "%DATE_LIST_FILE%" >nul 2>&1
    del "%TEMP%\_apt_recent_trade_dates_err.txt" >nul 2>&1
    pause
    exit /b 1
)
del "%TEMP%\_apt_recent_trade_dates_err.txt" >nul 2>&1

echo ==========================================================
echo  Starting apartment recent-trade mart (dm_apt_recent_trade) backfill.
echo  Target period (base_date): %START_DATE% ~ %END_DATE%
echo  build_apt_recent_trade_mart.py writes each result to the S3 Lake mart/
echo  path (s3a://LAKE/mart/dm_apt_recent_trade/base_date=YYYY-MM-DD, overwrite
echo  mode), in addition to printing the schema/sample/count to the console.
echo ==========================================================
echo.

REM --- [5] Loop per date (one Spark session per base_date; stop on first failure) ---
set "FAILED_DATE="
for /f "usebackq delims=" %%D in ("%DATE_LIST_FILE%") do (
    if not defined FAILED_DATE (
        echo ---- Running base_date=%%D ... ----
        "%PYTHON_EXE%" "%PIPELINE_SCRIPT%" %%D
        if errorlevel 1 (
            set "FAILED_DATE=%%D"
        )
        echo.
    )
)
del "%DATE_LIST_FILE%" >nul 2>&1

if defined FAILED_DATE (
    echo.
    echo [FAILED] An error occurred while running base_date=%FAILED_DATE% - backfill stopped.
    pause
    exit /b 1
)

echo.
echo [DONE] Apartment recent-trade mart backfill finished: %START_DATE% ~ %END_DATE%
pause
endlocal
