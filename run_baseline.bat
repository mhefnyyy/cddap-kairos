@echo off
REM Baseline (LAN) scenario — Windows wrapper. See run_baseline.py for options.
cd /d "%~dp0"
python run_baseline.py %*
