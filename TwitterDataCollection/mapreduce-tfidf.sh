#!/bin/bash

java -version
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

cd hadoop-2.9.2

#Set environment variables (These will not persist unless using "source" command!)
export HADOOP_HOME=`pwd`
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_INSTALL/lib/native"
export PATH="$PATH:$HADOOP_HOME/bin"
export CLASSPATH=`hadoop classpath`

cd ..

./hadoop-2.9.2/bin/hadoop dfs -put input /input
./hadoop-2.9.2/bin/hadoop dfs -put config /config

javac TFIDF.java
jar cf TFIDF.jar TFIDF*.class
hadoop jar TFIDF.jar TFIDF input