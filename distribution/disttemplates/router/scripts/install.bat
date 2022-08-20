@echo off
setlocal enableDelayedExpansion
set "MISSING_REQUIREMENTS=false"
CALL :check_requirement curl curl
CALL :check_requirement tar tar
IF "%MISSING_REQUIREMENTS%"=="true" (
  echo The above commands are missing on your computer. Please install it and execute this script again.
  exit /b
)
set EXTRACTED=#GRAALVMRELEASE#
set JAVA_HOME=%cd%/../%EXTRACTED%
if exist "%JAVA_HOME%" (
  if "%1" == "-d" (
    rmdir /Q /S "%JAVA_HOME%"
  ) else (
    @echo %EXTRACTED% is already installed.
    exit
  )
)
set DOWNLOADURL=#GRAALVMURL#.zip
if defined proxyhost (
  if defined proxyport (
    echo Using proxy %proxyhost%:%proxyport%
    set CURLPROXY=--proxy %proxyhost%:%proxyport%
    set GUPROXY=--vm.Dhttp.proxyHost=%proxyhost% --vm.Dhttp.proxyPort=%proxyport% --vm.Dhttps.proxyHost=%proxyhost% --vm.Dhttps.proxyPort=%proxyport%
  )
)
@echo Installing %EXTRACTED% ...
curl %CURLPROXY% -L -o graalvm.zip %DOWNLOADURL%
tar xf graalvm.zip --directory ../
del "graalvm.zip"
set EXECUTABLES=%JAVA_HOME%/bin
@echo The following version of GraalVM has been installed for this SwiftMQ Router:
%EXECUTABLES%/java -version
@echo !JAVA_HOME!>.javahome
@echo !EXECUTABLES!>.executables
@echo Installing Graal.js which is necessary to execute SwiftMQ Streams ...
%EXECUTABLES%/gu %GUPROXY% install js
@echo Installation complete.
exit
rem Ensures that the system has a specific program installed on the PATH.
:check_requirement
set "MISSING_REQUIREMENT=true"
where %1 > NUL 2>&1 && set "MISSING_REQUIREMENT=false"

IF "%MISSING_REQUIREMENT%"=="true" (
  echo Please download and install %2
  set "MISSING_REQUIREMENTS=true"
)

exit /b

