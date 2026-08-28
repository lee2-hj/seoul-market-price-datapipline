@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

REM ==========================================================================
REM  run_dong_mart.bat
REM  Silver(Iceberg: dim_apartment, fact_apt_transactions) -> Gold
REM  Backfill script for the [dong x pyeong] mart family (4 independent scripts,
REM  one per MinIO mart/ folder: dm_dong_pyeong_price_avg, dm_apt_price_avg,
REM  dm_apt_pyeong_price, dm_apt_flr_price). These used to be one shared-SparkSession
REM  script (build_dong_pyeong_mart.py), but that was split into 4 fully independent
REM  scripts so each one starts, finishes, and releases its memory before the next
REM  starts (GCP deployment memory limits) - dong_pyeong_common.py has the shared prep
REM  logic each script calls on its own. Each script only accepts a single BASE_DATE
REM  and aggregates the trailing 90 days from it, so for a given start/end date range
REM  this script loops day by day and re-runs all 4 scripts in sequence per date
REM  (overwriting per base_date=YYYY-MM-DD path).
REM ==========================================================================

REM --- [1] Project paths and venv python executable (relative to scripts\) ---
set "PROJECT_ROOT=%~dp0.."
set "PYTHON_EXE=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "GOLD_DIR=%PROJECT_ROOT%\src\transformation\gold"
set "PIPELINE_SCRIPT_1=%GOLD_DIR%\dm_dong_pyeong_price_avg.py"
set "PIPELINE_SCRIPT_2=%GOLD_DIR%\dm_apt_price_avg.py"
set "PIPELINE_SCRIPT_3=%GOLD_DIR%\dm_apt_pyeong_price.py"
set "PIPELINE_SCRIPT_4=%GOLD_DIR%\dm_apt_flr_price.py"

if not exist "%PYTHON_EXE%" (
    echo [오류] 가상환경 파이썬을 찾을 수 없습니다: %PYTHON_EXE%
    pause
    exit /b 1
)

for %%S in ("%PIPELINE_SCRIPT_1%" "%PIPELINE_SCRIPT_2%" "%PIPELINE_SCRIPT_3%" "%PIPELINE_SCRIPT_4%") do (
    if not exist %%S (
        echo [오류] 대상 스크립트를 찾을 수 없습니다: %%S
        pause
        exit /b 1
    )
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
    echo [준비] winutils.exe를 최초 1회 내려받습니다...
    mkdir "%HADOOP_HOME%\bin" >nul 2>&1
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri 'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe' -OutFile '%HADOOP_HOME%\bin\winutils.exe' -UseBasicParsing; Invoke-WebRequest -Uri 'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll' -OutFile '%HADOOP_HOME%\bin\hadoop.dll' -UseBasicParsing"
)

REM --- [2] MinIO(S3) credentials and bucket name: loaded from env/.env ---
REM     The 4 mart scripts load env/.env themselves too (dong_pyeong_common.py,
REM     override=False), but we populate the process environment here as well so
REM     values are available before the first script even starts.
set "ENV_LOADER_SCRIPT=%TEMP%\_dong_mart_env_%RANDOM%.py"

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
echo [오류] env\.env 에서 S3_END_POINT / S3_ACCESS_KEY / S3_SECRET_KEY / LAKE 값을 모두 확인할 수 없습니다.
echo        %PROJECT_ROOT%\env\.env 파일에 해당 값들이 설정되어 있는지 확인하세요.
pause
exit /b 1

:ENV_LOAD_OK

REM --- [3] Prompt for the backfill target period (base_date range) ---
echo [동 x 평형대] 마트 4종(dm_dong_pyeong_price_avg / dm_apt_price_avg / dm_apt_pyeong_price /
echo dm_apt_flr_price) 백필 대상 기준일(base_date) 범위를 입력하세요.
echo 형식: YYYY-MM-DD (예: 2026-07-01)
echo.

set "START_DATE="
set "END_DATE="
set /p START_DATE=시작일 (예: 2026-07-01):
set /p END_DATE=종료일   (예: 2026-08-12):
echo.

if "%START_DATE%"=="" (
    echo [오류] 시작일을 입력해야 합니다.
    pause
    exit /b 1
)
if "%END_DATE%"=="" (
    echo [오류] 종료일을 입력해야 합니다.
    pause
    exit /b 1
)

REM --- [4] Build the list of dates in the start~end range (computed via Python datetime) ---
set "DATE_LIST_SCRIPT=%TEMP%\_dong_mart_dates_%RANDOM%.py"

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

set "DATE_LIST_FILE=%TEMP%\_dong_mart_date_list_%RANDOM%.txt"
"%PYTHON_EXE%" "%DATE_LIST_SCRIPT%" %START_DATE% %END_DATE% > "%DATE_LIST_FILE%" 2>"%TEMP%\_dong_mart_dates_err.txt"
set "DATE_LIST_RESULT=%ERRORLEVEL%"
del "%DATE_LIST_SCRIPT%" >nul 2>&1

if not "%DATE_LIST_RESULT%"=="0" (
    echo [오류] 날짜 범위가 올바르지 않습니다. 입력값 형식^(YYYY-MM-DD^)과 시작일 ^<= 종료일 여부를 확인하세요.
    del "%DATE_LIST_FILE%" >nul 2>&1
    del "%TEMP%\_dong_mart_dates_err.txt" >nul 2>&1
    pause
    exit /b 1
)
del "%TEMP%\_dong_mart_dates_err.txt" >nul 2>&1

echo ==========================================================
echo  [동 x 평형대] 마트 4종 백필을 시작합니다.
echo  대상 기간(base_date): %START_DATE% ~ %END_DATE%
echo ==========================================================
echo.

REM --- [5] Loop per date, running the 4 mart scripts one at a time in sequence
REM     (each is its own separate Python/Spark process that fully exits and
REM     releases its memory before the next one starts - GCP 메모리 제약 대응,
REM     data_orchestration.py의 순차 태스크 체이닝과 동일한 이유). Stop on first failure.
set "FAILED_DATE="
set "FAILED_SCRIPT="
for /f "usebackq delims=" %%D in ("%DATE_LIST_FILE%") do (
    if not defined FAILED_DATE (
        echo ---- base_date=%%D 실행 중... ----
        for %%S in ("%PIPELINE_SCRIPT_1%" "%PIPELINE_SCRIPT_2%" "%PIPELINE_SCRIPT_3%" "%PIPELINE_SCRIPT_4%") do (
            if not defined FAILED_DATE (
                echo   - %%~nxS 실행 중...
                "%PYTHON_EXE%" %%S %%D
                if errorlevel 1 (
                    set "FAILED_DATE=%%D"
                    set "FAILED_SCRIPT=%%~nxS"
                )
            )
        )
        echo.
    )
)
del "%DATE_LIST_FILE%" >nul 2>&1

if defined FAILED_DATE (
    echo.
    echo [실패] base_date=%FAILED_DATE% 의 %FAILED_SCRIPT% 실행 중 오류가 발생하여 백필을 중단했습니다.
    pause
    exit /b 1
)

echo.
echo [완료] [동 x 평형대] 마트 4종 백필이 끝났습니다: %START_DATE% ~ %END_DATE%
pause
endlocal
