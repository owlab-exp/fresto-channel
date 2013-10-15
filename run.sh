CLASSPATH=./build/classes
CLASSPATH=$CLASSPATH:./lib/*
CLASSPATH=$CLASSPATH:./lib-etc/*
#CLASSPATH=$CLASSPATH:/working/servers/hadoop/lib/*
export CLASSPATH

java -Xmx512m -Xms512m -cp $CLASSPATH $@
