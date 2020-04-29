@echo off
where java >nul 2>nul
if %errorlevel%==1 (
    @echo Please install Java before using SwiftMQ!
    exit
)

set OPENS=
for /f "tokens=*" %%i in ('java -cp ../jars/swiftmq.jar com.swiftmq.JavaHome') do set JAVAHOME=%%i
for /f %%i in ('java -cp ../jars/swiftmq.jar com.swiftmq.IsJava8') do set ISJAVA8=%%i
if "%ISJAVA8%" == "false" (
  set OPENS=--add-opens=java.desktop/java.awt.font=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED  --add-opens=java.base/sun.net.dns=ALL-UNNAMED -Dnashorn.args=--no-deprecation-warning
)

set PRECONFIG=-Dswiftmq.preconfig=../data/preconfig/upgrade-to-12.1.0.xml
IF NOT "%1"=="" (
  set PRECONFIG=%PRECONFIG%,%1
)
set ROUTEROPT=%OPENS% %PRECONFIG% -Dswiftmq.directory.autocreate=true -Djavax.net.ssl.keyStore=../certs/server.keystore -Djavax.net.ssl.keyStorePassword=secret -Djavax.net.ssl.trustStore=../certs/client.truststore -Djavax.net.ssl.trustStorePassword=secret

if not exist ../certs/.certimported (
   keytool -importkeystore -srckeystore "%JAVAHOME%/lib/security/cacerts" -srcstorepass changeit -destkeystore ../certs/client.truststore -deststorepass secret > nul 2>nul
   @echo $null > ../certs/.certimported
)

java -server -Xmx2G %ROUTEROPT% -cp ../jars/swiftmq.jar com.swiftmq.Router ../data/config/routerconfig.xml
