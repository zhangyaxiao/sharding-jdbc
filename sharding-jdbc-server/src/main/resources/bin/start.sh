#!/bin/bash
SERVER_NAME=Sharding-JDBC-Server

cd `dirname $0`
cd ..
DEPLOY_DIR=`pwd`

LOGS_DIR=$DEPLOY_DIR/logs
if [ ! -d $LOGS_DIR ]; then
    mkdir $LOGS_DIR
fi

STDOUT_FILE=$LOGS_DIR/stdout.log

PIDS=`ps -ef | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`
if [ -n "$PIDS" ]; then
    echo "ERROR: The $SERVER_NAME already started!"
    echo "PID: $PIDS"
    exit 1
fi

LIB_JARS=$DEPLOY_DIR/lib/*
JAVA_OPTS=" -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true "

JAVA_MEM_OPTS=" -server -Xmx2g -Xms2g -Xmn256m -XX:PermSize=128m -Xss256k -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:LargePageSizeInBytes=128m -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 "

MAIN_CLASS=io.shardingjdbc.server.Bootstrap

if [ $# == 1 ]; then
    MAIN_CLASS=$MAIN_CLASS $1
fi

echo "Starting the $SERVER_NAME ..."
nohup java $JAVA_OPTS $JAVA_MEM_OPTS -classpath .:$LIB_JARS $MAIN_CLASS > $STDOUT_FILE 2>&1 &
echo "Please check the STDOUT file: $STDOUT_FILE"
