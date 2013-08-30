export CLASSPATH=./lib/jeromq-0.3.0-SNAPSHOT.jar
export CLASSPATH=$CLASSPATH:./lib/libthrift-0.9.1.jar
export CLASSPATH=$CLASSPATH:./lib/log4j-1.2.16.jar
export CLASSPATH=$CLASSPATH:./lib/slf4j-api-1.6.6.jar
export CLASSPATH=$CLASSPATH:./dist/Forwarder.jar
#export CLASS=psenvpub
export CLASS=Forwarder
java -cp $CLASSPATH $CLASS
