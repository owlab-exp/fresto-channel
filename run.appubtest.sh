export CLASSPATH=./lib/jeromq-0.3.0-SNAPSHOT.jar
export CLASSPATH=$CLASSPATH:./lib/*
export CLASSPATH=$CLASSPATH:./dist/eventhub.jar
export CLASS=TestHttpPublisher
java -cp $CLASSPATH $CLASS
