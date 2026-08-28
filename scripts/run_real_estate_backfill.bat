@echo off
setlocal

REM ==========================================================================
REM  run_real_estate_backfill.bat
REM  Backfill script for real estate raw ingestion (Real_Estate.py).
REM  Prompts for a start/end anchor-date range and passes it through to
REM  run_real_estate_backfill.py, which treats each date in that range as an
REM  anchor date and re-checks the trailing 90 days from it (same logic the
REM  daily DAG runs for "today"). Leaving both prompts empty runs the default
REM  range (last 7 days ending yesterday).
REM  NOTE: all user-facing messages are in English on purpose (not Korean) -
REM  this project's Windows/cmd.exe environment has shown repeated, inconsistent
REM  parsing corruption with Korean text in .bat files regardless of the file
REM  encoding tried (UTF-8 no BOM, UTF-8+BOM, CP949 native), so plain ASCII is
REM  used here to avoid that class of bug entirely. Do not add non-ASCII text
REM  back into this file.
REM ==========================================================================

REM this batch file's location (scripts folder) to find the project root
set "PROJECT_ROOT=%~dp0.."
set "PYTHON_EXE=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "SCRIPT_PATH=%PROJECT_ROOT%\scripts\run_real_estate_backfill.py"

if not exist "%PYTHON_EXE%" (
    echo [ERROR] Cannot find the virtual environment Python: %PYTHON_EXE%
    pause
    exit /b 1
)

echo Enter the anchor-date range to backfill for real estate raw ingestion.
echo Each date in start~end is used as an anchor date: the trailing 90 days
echo from that date are re-checked ^(same logic the daily DAG runs for "today"^).
echo Dates already stored with unchanged content are skipped automatically -
echo only changed or newly added contract dates are written and upserted.
echo Format: YYYYMMDD (e.g. 20260701) or YYYY-MM-DD (e.g. 2026-07-01) both work.
echo Leave both blank and press Enter to run the default range (last 7 days, through yesterday).
echo.

set "START_DATE="
set "END_DATE="
set /p START_DATE=Start anchor date (e.g. 2026-07-01):
set /p END_DATE=End anchor date   (e.g. 2026-08-12):
echo.

if "%START_DATE%"=="" (
    echo No input given - running with the default range ^(last 7 days, through yesterday^)...
    echo.
    "%PYTHON_EXE%" "%SCRIPT_PATH%"
) else (
    echo Running with anchor-date range %START_DATE% ~ %END_DATE% ^(rechecking last 90 days per anchor^)...
    echo YYYY-MM-DD input is auto-normalized to YYYYMMDD by the script. Invalid formats log an error and exit.
    echo.
    "%PYTHON_EXE%" "%SCRIPT_PATH%" %START_DATE% %END_DATE%
)

if errorlevel 1 (
    echo.
    echo [FAILED] An error occurred during the real estate backfill.
) else (
    echo.
    echo [DONE] Real estate backfill finished.
)

pause
endlocal
