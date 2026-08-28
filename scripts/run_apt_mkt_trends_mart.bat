@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

REM ==========================================================================
REM  run_apt_mkt_trends_mart.bat
REM  아파트 시장 동향 Gold 마트(apt_mkt_trends_mart.py) 실행 스크립트.
REM  apt_mkt_trends_mart.py는 AS_OF_DATE(YYYY-MM-DD) 인자 1개만 받아 그 날짜 기준
REM  최근 90일을 다시 훑은 뒤, 실제로 내용이 바뀐 base_date=YYYY-MM-DD 파티션만
REM  Insert/Update하고 변경이 없는 파티션은 Skip한다(run_apt_rtt_mart.bat와 동일한
REM  구조 - Polars+DuckDB 기반이라 PySpark용 SPARK_HOME/HADOOP_HOME 설정은 불필요).
REM  한글 깨짐 방지: 이 파일은 UTF-8로 저장돼 있고, 위 chcp 65001로 콘솔 코드페이지를
REM  UTF-8로 맞춰 출력 시 한글이 깨지지 않도록 한다(run_dong_mart.bat와 동일한 방식).
REM ==========================================================================

REM --- [1] 프로젝트 경로 및 가상환경 파이썬 실행 파일(scripts\ 기준 상대 경로) ---
set "PROJECT_ROOT=%~dp0.."
set "PYTHON_EXE=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "PIPELINE_SCRIPT=%PROJECT_ROOT%\src\transformation\gold\apt_mkt_trends_mart.py"

if not exist "%PYTHON_EXE%" (
    echo [오류] 가상환경 파이썬을 찾을 수 없습니다: %PYTHON_EXE%
    pause
    exit /b 1
)

if not exist "%PIPELINE_SCRIPT%" (
    echo [오류] 대상 스크립트를 찾을 수 없습니다: %PIPELINE_SCRIPT%
    pause
    exit /b 1
)

REM --- [2] MinIO(S3) 접속 정보/버킷명: env\.env 값 존재 여부를 미리 확인 ---
REM     apt_mkt_trends_mart.py 자체가 load_config()에서 env\.env를 직접 로드하므로
REM     (override=False) 이 단계가 실행에 필수는 아니지만, 스크립트가 이미 한참
REM     실행된 뒤에야 실패하면 원인 파악이 오래 걸리므로 여기서 먼저 점검한다.
set "ENV_LOADER_SCRIPT=%TEMP%\_apt_mkt_trends_env_%RANDOM%.py"

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

REM --- [3] 기준일(AS_OF_DATE) 입력 받기 ---
echo 아파트 시장 동향 마트(apt_mkt_trends_mart) 기준일(AS_OF_DATE)을 입력하세요.
echo apt_mkt_trends_mart.py는 이 날짜 기준 최근 90일을 다시 확인해, 실제로 내용이
echo 바뀐 base_date=YYYY-MM-DD 파티션만 Insert/Update하고 변경 없는 파티션은 건너뜁니다.
echo 형식: YYYY-MM-DD (예: 2026-08-20)
echo 아무것도 입력하지 않고 Enter를 누르면 오늘 날짜를 기준일로 사용합니다.
echo.

set "AS_OF_DATE="
set /p AS_OF_DATE=기준일 (예: 2026-08-20):
echo.

if "%AS_OF_DATE%"=="" (
    echo 입력값이 없어 오늘 날짜를 기준일로 실행합니다...
    echo.
    "%PYTHON_EXE%" "%PIPELINE_SCRIPT%"
) else (
    echo 기준일 %AS_OF_DATE% ^(이 날짜로부터 최근 90일 재확인^)로 실행합니다...
    echo.
    "%PYTHON_EXE%" "%PIPELINE_SCRIPT%" %AS_OF_DATE%
)

if errorlevel 1 (
    echo.
    echo [실패] 아파트 시장 동향 마트^(apt_mkt_trends^) 실행 중 오류가 발생했습니다.
) else (
    echo.
    echo [완료] 아파트 시장 동향 마트^(apt_mkt_trends^) 실행이 끝났습니다.
)

pause
endlocal
