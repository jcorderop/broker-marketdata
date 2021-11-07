@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

echo "APP_HOME" %APP_HOME%
echo "APP_BASE_NAME" %APP_BASE_NAME%

set DST_DIR=%DIRNAME%src\main\java\
set SRC_DIR=%DIRNAME%proto

echo "DST_DIR %DST_DIR%"
echo "SRC_DIR %SRC_DIR%"
echo "PROTOC_PATH %PROTOC_PATH%"

"%PROTOC_PATH%protoc.exe" -I=%SRC_DIR% --java_out=%DST_DIR% %SRC_DIR%\InternalQuote.proto
