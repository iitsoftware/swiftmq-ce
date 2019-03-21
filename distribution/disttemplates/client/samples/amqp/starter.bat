set class=%1
shift
java -cp .;../../jars/swiftmq.jar -Dswiftmq.jsse.anoncipher.enabled=false -Dswiftmq.amqp.frame.debug=false -Dswiftmq.amqp.debug=false %class% %1 %2 %3 %4 %5 %6 %7 %8 %9

