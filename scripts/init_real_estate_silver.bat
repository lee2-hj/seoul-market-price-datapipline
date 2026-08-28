@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

REM ==========================================================================
REM  init_real_estate_silver.bat
REM  MinIO Bronze (S3 JSON) -> Apache Iceberg Silver initial backfill script.
REM  Calls Real_Estate_Transform.py BULK once for a single high-speed bulk
REM  load of the whole source period (no more one-Spark-session-per-day loop).
REM ==========================================================================

REM --- [1] Project paths and venv python executable (relative to scripts\) ---
set "PROJECT_ROOT=%~dp0.."
set "PYTHON_EXE=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "PIPELINE_SCRIPT=%PROJECT_ROOT%\src\transformation\silver\Real_Estate_Transform.py"

if not exist "%PYTHON_EXE%" (
    echo [오류] 가상환경 파이썬을 찾을 수 없습니다: %PYTHON_EXE%
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
    echo [준비] winutils.exe를 최초 1회 내려받습니다...
    mkdir "%HADOOP_HOME%\bin" >nul 2>&1
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri 'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe' -OutFile '%HADOOP_HOME%\bin\winutils.exe' -UseBasicParsing; Invoke-WebRequest -Uri 'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll' -OutFile '%HADOOP_HOME%\bin\hadoop.dll' -UseBasicParsing"
)

REM --- [2] MinIO(S3) credentials & bucket names: loaded from env/.env ---
REM     Hardcoding credentials directly in this script is a security risk
REM     (plaintext secrets checked into the repo), so they are loaded at
REM     runtime from the project's env/.env file via python-dotenv instead
REM     (the same file src/config/paths.py already loads for the rest of
REM     the project). Edit env/.env to change these values, not this file.
set "ENV_LOADER_SCRIPT=%TEMP%\_re_silver_env_%RANDOM%.py"

echo import os> "%ENV_LOADER_SCRIPT%"
echo from dotenv import dotenv_values>> "%ENV_LOADER_SCRIPT%"
echo env_path = os.path.join(os.environ["PROJECT_ROOT"], "env", ".env")>> "%ENV_LOADER_SCRIPT%"
echo values = dotenv_values(env_path)>> "%ENV_LOADER_SCRIPT%"
echo for key in ["S3_END_POINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "RAW", "LAKE"]:>> "%ENV_LOADER_SCRIPT%"
echo     val = values.get(key)>> "%ENV_LOADER_SCRIPT%"
echo     print(key + "=" + (val if val is not None else ""))>> "%ENV_LOADER_SCRIPT%"

set "S3_END_POINT="
set "S3_ACCESS_KEY="
set "S3_SECRET_KEY="
set "RAW="
set "LAKE="
for /f "usebackq tokens=1,* delims==" %%A in (`""%PYTHON_EXE%" "%ENV_LOADER_SCRIPT%""`) do set "%%A=%%B"
del "%ENV_LOADER_SCRIPT%" >nul 2>&1

if not defined S3_END_POINT goto :ENV_LOAD_FAILED
if not defined S3_ACCESS_KEY goto :ENV_LOAD_FAILED
if not defined S3_SECRET_KEY goto :ENV_LOAD_FAILED
if not defined RAW goto :ENV_LOAD_FAILED
if not defined LAKE goto :ENV_LOAD_FAILED
goto :ENV_LOAD_OK

:ENV_LOAD_FAILED
echo [오류] env\.env 에서 S3_END_POINT / S3_ACCESS_KEY / S3_SECRET_KEY / RAW / LAKE 값을 모두 확인할 수 없습니다.
echo        %PROJECT_ROOT%\env\.env 파일에 해당 값들이 설정되어 있는지 확인하세요.
pause
exit /b 1

:ENV_LOAD_OK

REM --- [3] Backfill date range: auto-detected from the Bronze source data ---
REM     Hardcoding a date range would silently miss/skip data whenever the
REM     source range changes, so instead every real_estate/year=*/month=*/
REM     day=*/ partition actually present in MinIO is scanned (via DuckDB's
REM     httpfs S3 support) and the earliest/latest dates found become the
REM     backfill range.
set "DATE_RANGE_SCRIPT=%TEMP%\_re_silver_date_range_%RANDOM%.py"

echo import duckdb, os, re, sys> "%DATE_RANGE_SCRIPT%"
echo con = duckdb.connect()>> "%DATE_RANGE_SCRIPT%"
echo con.execute("INSTALL httpfs; LOAD httpfs;")>> "%DATE_RANGE_SCRIPT%"
echo con.execute("SET s3_endpoint='" + os.environ["S3_END_POINT"] + "';")>> "%DATE_RANGE_SCRIPT%"
echo con.execute("SET s3_access_key_id='" + os.environ["S3_ACCESS_KEY"] + "';")>> "%DATE_RANGE_SCRIPT%"
echo con.execute("SET s3_secret_access_key='" + os.environ["S3_SECRET_KEY"] + "';")>> "%DATE_RANGE_SCRIPT%"
echo con.execute("SET s3_use_ssl=false;")>> "%DATE_RANGE_SCRIPT%"
echo con.execute("SET s3_url_style='path';")>> "%DATE_RANGE_SCRIPT%"
echo pattern = "s3://" + os.environ["RAW"] + "/real_estate/year=*/month=*/day=*/*">> "%DATE_RANGE_SCRIPT%"
echo rows = con.execute("SELECT file FROM glob('" + pattern + "')").fetchall()>> "%DATE_RANGE_SCRIPT%"
echo dates = set()>> "%DATE_RANGE_SCRIPT%"
echo for (path,) in rows:>> "%DATE_RANGE_SCRIPT%"
echo     m = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", path)>> "%DATE_RANGE_SCRIPT%"
echo     if m:>> "%DATE_RANGE_SCRIPT%"
echo         dates.add(m.group(1) + "-" + m.group(2) + "-" + m.group(3))>> "%DATE_RANGE_SCRIPT%"
echo if not dates:>> "%DATE_RANGE_SCRIPT%"
echo     sys.stderr.write("NO_SOURCE_DATA_FOUND\n")>> "%DATE_RANGE_SCRIPT%"
echo     sys.exit(1)>> "%DATE_RANGE_SCRIPT%"
echo dates = sorted(dates)>> "%DATE_RANGE_SCRIPT%"
echo print(dates[0])>> "%DATE_RANGE_SCRIPT%"
echo print(dates[-1])>> "%DATE_RANGE_SCRIPT%"

echo [준비] Bronze 원천 데이터의 실제 존재 기간을 조회합니다...
set "START_DATE="
set "END_DATE="
for /f "usebackq delims=" %%D in (`""%PYTHON_EXE%" "%DATE_RANGE_SCRIPT%""`) do (
    if not defined START_DATE (
        set "START_DATE=%%D"
    ) else (
        set "END_DATE=%%D"
    )
)
del "%DATE_RANGE_SCRIPT%" >nul 2>&1

if not defined START_DATE (
    echo [오류] Bronze 원천 데이터의 날짜 범위를 확인할 수 없습니다. MinIO 연결 상태 또는 원천 데이터 존재 여부를 확인하세요.
    pause
    exit /b 1
)
if not defined END_DATE (
    set "END_DATE=%START_DATE%"
)

echo ==========================================================
echo  [INITIAL LOAD] 부동산 실거래가 Silver 초기 적재를 시작합니다. (BULK 모드)
echo  대상 기간: %START_DATE% ~ %END_DATE%
echo ==========================================================
echo.

REM --- [4] Single bulk-load Spark run (no more one-Spark-session-per-day loop) ---
REM     Real_Estate_Transform.py's BULK mode scans every year/month/day
REM     partition via a single wildcard path, so the whole backfill finishes
REM     in one Spark session instead of restarting Spark for every day.
"%PYTHON_EXE%" "%PIPELINE_SCRIPT%" BULK

set "BACKFILL_RESULT=%ERRORLEVEL%"

REM --- [5] Report final result ---
if not "%BACKFILL_RESULT%"=="0" (
    echo.
    echo [실패] Backfill이 실패했습니다. 위 로그를 확인하세요. ^(ExitCode=%BACKFILL_RESULT%^)
    pause
    exit /b 1
)

echo.
echo [완료] 전체 기간 Backfill이 끝났습니다: %START_DATE% ~ %END_DATE%
pause
endlocal
