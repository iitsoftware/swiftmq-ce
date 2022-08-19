@echo off
if exist .executables (goto graalinstalled) else (goto graalnotinstalled)

:main
set OPENS=--add-opens=java.desktop/java.awt.font=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED  --add-opens=java.base/sun.net.dns=ALL-UNNAMED -Dnashorn.args=--no-deprecation-warning

set PRECONFIG=-Dswiftmq.preconfig=../data/preconfig/upgrade-to-12.1.0.xml
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
echo Starting SwiftMQ with '%JVMPARAM%' with '%JAVA_HOME%'.
echo Please have a look at data/log/stdout.log ...
if "%EXECUTABLES%" == "" (
    java -server %JVMPARAM% %ROUTEROPT% -cp ../jars/swiftmq.jar com.swiftmq.Router ../data/config/routerconfig.xml >../data/log/stdout.log 2>../data/log/stderr.log
) else (
    %EXECUTABLES%/java -server %JVMPARAM% %ROUTEROPT% -cp ../jars/swiftmq.jar com.swiftmq.Router ../data/config/routerconfig.xml >../data/log/stdout.log 2>../data/log/stderr.log
)
exit

:graalinstalled
set /p EXECUTABLES=<.executables
set /p JAVA_HOME=<.javahome
goto main

:graalnotinstalled
where java >nul 2>nul
if %errorlevel%==1 (
    @echo Please install Java 11 or later before using SwiftMQ! From Java 15 onwards you need GraalVM: https://graalvm.org
    exit
)

for /f tokens^=2-5^ delims^=.-_^" %%j in ('java -fullversion 2^>^&1') do set "jver=%%j"
if %jver% LSS 11 (
    @echo Please install Java 11 or later before using SwiftMQ! From Java 15 onwards you need GraalVM: https://graalvm.org
    exit
)
for /f "tokens=*" %%i in ('java -cp ../jars/swiftmq.jar com.swiftmq.JavaHome') do set JAVAHOME=%%i
echo You are using the default Java installation at '%JAVA_HOME%'!
echo Please consider to use the 'install' script in this folder to install GraalVM!
goto main