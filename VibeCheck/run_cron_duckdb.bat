@echo off
echo Starting Twitter Vibe Check Update at %date% %time%

:: Use the correct path to your R installation (from Sys.which output)
"C:\Users\TomHun\AppData\Local\Programs\R\R-44~1.2\bin\x64\Rscript.exe" "C:/Users/TomHun/OneDrive - City & Guilds/Documents/Code/R/vibe_check/VibeCheck/inst/extdata/cron_duckdb.R"

if %ERRORLEVEL% NEQ 0 (
    echo Error occurred while running the script!
    echo Error code: %ERRORLEVEL%
) else (
    echo Script completed successfully.
)

echo Process completed at %date% %time%
pause