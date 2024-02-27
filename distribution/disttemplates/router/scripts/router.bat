@echo off
set /p JAVAHOME=../graalvm-jdk
set /p EXECUTABLES=%JAVAHOME%/bin
set OPENS=--module-path=../graalvm --add-opens=java.desktop/java.awt.font=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED  --add-opens=java.base/sun.net.dns=ALL-UNNAMED -Dnashorn.args=--no-deprecation-warning
%EXECUTABLES%/java -cp ../jars/swiftmq.jar;../graalvm com.swiftmq.SystemPreconfig > temp.txt
set /p PRECONFIG=<temp.txt
del temp.txt

IF NOT "%~1"=="" (
  set PRECONFIG=%PRECONFIG%,%~1
)
set ROUTEROPT=%OPENS% -Dswiftmq.preconfig=%PRECONFIG% -Dswiftmq.directory.autocreate=true -Djavax.net.ssl.keyStore=../certs/server.keystore -Djavax.net.ssl.keyStorePassword=secret -Djavax.net.ssl.trustStore=../certs/client.truststore -Djavax.net.ssl.trustStorePassword=secret

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

echo Starting SwiftMQ with '%JVMPARAM%' heap size on bundled GraalVM CE..
echo Please have a look at data/log/stdout.log ...
%EXECUTABLES%/java -server %JVMPARAM% %ROUTEROPT% %PROXY% -cp ../jars/swiftmq.jar com.swiftmq.Router ../data/config/routerconfig.xml >../data/log/stdout.log 2>../data/log/stderr.log

