@echo off
echo Installing Tally XML to MySQL Converter...
echo.

echo Step 1: Installing Node.js dependencies...
call npm install

if %errorlevel% neq 0 (
    echo Error: Failed to install dependencies
    echo Please make sure you have Node.js installed
    pause
    exit /b 1
)

echo.
echo Step 2: Testing setup...
call node test.js

echo.
echo Setup complete!
echo.
echo To run the converter: npm start
echo To test again: npm test
echo.
pause