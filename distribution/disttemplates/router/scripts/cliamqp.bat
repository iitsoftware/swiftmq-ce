java -cp ../jars/swiftmq.jar -Djavax.net.ssl.trustStore=../certs/client.truststore -Djavax.net.ssl.trustStorePassword=secret com.swiftmq.amqp.v100.mgmt.CLILauncher amqp://localhost:5672 %1

