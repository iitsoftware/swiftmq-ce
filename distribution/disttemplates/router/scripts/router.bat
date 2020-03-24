set OPENS=
set PRECONFIG=-Dswiftmq.preconfig=../data/preconfig/upgrade-to-12.1.0.xml
IF "%1"=="java9" (
  set OPENS=--add-opens=java.desktop/java.awt.font=ALL-UNNAMED --add-exports=java.desktop/com.sun.awt=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
  shift
)
IF NOT %1=="" (
  set PRECONFIG=%PRECONFIG%,%⁓1
)
java -server -Xmx1024M %OPENS% %PRECONFIG% -Dswiftmq.directory.autocreate=true -Djavax.net.ssl.keyStore=../certs/server.keystore -Djavax.net.ssl.keyStorePassword=secret -Djavax.net.ssl.trustStore=../certs/client.truststore -Djavax.net.ssl.trustStorePassword=secret -cp ../jars/swiftmq.jar -Dnashorn.args="--no-deprecation-warning" com.swiftmq.Router ../data/config/routerconfig.xml
