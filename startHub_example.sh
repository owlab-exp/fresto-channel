CLASSPATH=./dist/*
CLASSPATH=$CLASSPATH:./lib/*
#CLASSPATH=$CLASSPATH:/working/servers/hadoop/lib/*
export CLASSPATH
MAIN_CLASS=fresto.channel.EventHub
FRONT_URL=tcp://*:7000
BACK_URL=tcp://*:7001
#java -Xmx256m -XX:+AggressiveOpts -XX:CompileThreshold=200 -cp $CLASSPATH $@
nohup java -server -Xmx256m -XX:+AggressiveOpts -XX:CompileThreshold=200 -cp $CLASSPATH $MAIN_CLASS $FRONT_URL $BACK_URL < /dev/null > event_hub.log 2>&1 &
#java -server -Xmx256m -XX:+AggressiveOpts -XX:CompileThreshold=200 -cp $CLASSPATH $MAIN_CLASS $FRONT_URL $BACK_URL
