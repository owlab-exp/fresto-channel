CLASSPATH=./dist/*
CLASSPATH=$CLASSPATH:./lib/*
#CLASSPATH=$CLASSPATH:/working/servers/hadoop/lib/*
export CLASSPATH
MAIN_CLASS=fresto.channel.EventStreamer
FRONT_URL=tcp://<host name of event hub>:7001
BACK_URL=tcp://*:7002
#java -Xmx256m -XX:+AggressiveOpts -XX:CompileThreshold=200 -cp $CLASSPATH $@
nohup java -server -Xmx256m -XX:+AggressiveOpts -XX:CompileThreshold=200 -cp $CLASSPATH $MAIN_CLASS $FRONT_URL $BACK_URL < /dev/null > event_streamer.log 2>&1 &
#java -server -Xmx256m -XX:+AggressiveOpts -XX:CompileThreshold=200 -cp $CLASSPATH $MAIN_CLASS $FRONT_URL $BACK_URL
