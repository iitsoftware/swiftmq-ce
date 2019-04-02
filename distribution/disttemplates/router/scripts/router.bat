set OPENS=
IF "%1"=="java9" (
  set OPENS=--add-opens=java.desktop/java.awt.font=ALL-UNNAMED --add-exports=java.desktop/com.sun.awt=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
  shift
)
java -server -Xmx1024M %OPENS% -cp ../jars/swiftmq.jar -Dnashorn.args="--no-deprecation-warning" com.swiftmq.Router ../config/routerconfig.xml %1
