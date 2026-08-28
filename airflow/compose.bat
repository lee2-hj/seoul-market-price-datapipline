@echo off
REM Wrapper for docker compose: points --env-file at the project's single
REM consolidated env file (env/.env) so ${VAR} substitution in this
REM compose file resolves correctly.
REM Usage: compose.bat up -d / compose.bat down / compose.bat config
docker compose --env-file ../env/.env %*
