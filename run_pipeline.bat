@echo off
title Finance Data Pipeline
echo ==========================================
echo   Finance Data Pipeline (One-Click Runner)
echo ==========================================
echo.

python run_pipeline.py

echo.
echo ------------------------------------------
echo If the report was generated successfully,
echo open: data\output\finance_report.xlsx
echo ------------------------------------------
echo.
pause
