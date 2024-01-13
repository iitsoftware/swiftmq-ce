@echo off
if not exist .executables call install.bat -d

set /p EXECUTABLES=<.executables
set /p JAVA_HOME=<.javahome
set OPENS=--module-path=../graalvm --add-modules=org.graalvm.polyglot --add-opens=java.desktop/java.awt.font=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED  --add-opens=java.base/sun.net.dns=ALL-UNNAMED -Dnashorn.args=--no-deprecation-warning

set "PRECONFIG=-Dswiftmq.preconfig="
set "TEMP_FILE=tempfile.txt"

:: Clear the temp file
type nul > %TEMP_FILE%

:: Accumulate file names
for %%F in (..\data\preconfig\*.xml) do (
    echo %%F >> %TEMP_FILE%
)

:: Read the temp file and build the PRECONFIG string
set /p PRECONFIG=%PRECONFIG%<%TEMP_FILE%
for /f "skip=1 delims=" %%A in (%TEMP_FILE%) do (
    set /p =,%PRECONFIG%%%A< nul >> %TEMP_FILE%
    set /p PRECONFIG=<%TEMP_FILE%
)

:: Display the result
echo %PRECONFIG%

:: Clean up the temporary file
del %TEMP_FILE%

IF NOT "%~1"=="" (
  set PRECONFIG=%PRECONFIG%,%~1
)
set ROUTEROPT=%OPENS% %PRECONFIG% -Dswiftmq.directory.autocreate=true -Djavax.net.ssl.keyStore=../certs/server.keystore -Djavax.net.ssl.keyStorePassword=secret -Djavax.net.ssl.trustStore=../certs/client.truststore -Djavax.net.ssl.trustStorePassword=secret

if not exist ../certs/.certimported (
   keytool -importkeystore -srckeystore "%JAVAHOME%/lib/security/cacerts" -srcstorepass changeit -destkeystore ../certs/client.truststore -deststorepass secret > nul 2>nul
   @echo $null > ../certs/.certimported
)

set JVMPARAM=-Xmx2G
IF DEFINED SWIFTMQ_JVMPARAM (
  set JVMPARAM=%SWIFTMQ_JVMPARAM%
)
if defined proxyhost (
  if defined proxyport (
    echo Setting http/s proxy to %proxyhost%:%proxyport%
    set PROXY=-Dhttp.proxyHost=%proxyhost% -Dhttp.proxyPort=%proxyport% -Dhttps.proxyHost=%proxyhost% -Dhttps.proxyPort=%proxyport%
  )
)

echo Starting SwiftMQ with '%JVMPARAM%' with '%JAVA_HOME%'.
echo Please have a look at data/log/stdout.log ...
if "%EXECUTABLES%" == "" (
    java -server %JVMPARAM% %ROUTEROPT% %PROXY% -cp ../jars/swiftmq.jar;../graalvm com.swiftmq.Router ../data/config/routerconfig.xml >../data/log/stdout.log 2>../data/log/stderr.log
) else (
    %EXECUTABLES%/java -server %JVMPARAM% %ROUTEROPT% %PROXY% -cp ../jars/swiftmq.jar;../graalvm com.swiftmq.Router ../data/config/routerconfig.xml >../data/log/stdout.log 2>../data/log/stderr.log
)
