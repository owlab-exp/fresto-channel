CLASSPATH=./build/classes
CLASSPATH=$CLASSPATH:./lib/*
CLASSPATH=$CLASSPATH:./lib-etc/*
#CLASSPATH=$CLASSPATH:/working/servers/hadoop/lib/*
export CLASSPATH

jdb -sourcepath ./src/java:/working/kits/dfs-datastores/dfs-datastores/src/main/java  -classpath $CLASSPATH $@
