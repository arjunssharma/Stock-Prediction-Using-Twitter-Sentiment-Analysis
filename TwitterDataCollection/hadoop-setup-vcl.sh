#!/bin/bash

#############################################################################################
#
# FILENAME:    hadoop-setup.sh
#
# USAGE:       "source hadoop-setup.sh"
#
# NOTE:        This script sets multiple environment variables, so you MUST use
#              "source" to run the script. Otherwise, the shell in which you ran
#              this script will not pick up the new environment variables!
#
# DESCRIPTION: This script sets up hadoop on the ARC cluster. It assumes one master node
#              and N slave nodes, where N is the number of nodes you reserved using "srun". 
#              The master node will be the lowest numbered node in your reservation. It
#              sets up the HDFS and also creates a "/user/UNITYID" directory inside the
#              HDFS, which is where any input/output will be read/written to. To make sure
#              this script ran successfully, run "hdfs dfs -ls /user". You should see the
#              "/user/UNITYID" directory that was created. 
# 
# AUTHOR:      Tyler Stocksdale
# DATE:        10/18/2017
#
#############################################################################################

#Java setup, probably redundant. JDK is always present at mentioned path
sudo apt-get update
sudo apt-get install default-jdk -y
java -version
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export JAVA_HOME

#Replacing IP in /etc/hosts needed for ssh to localhost
ip="$(ifconfig | grep -A 1 'eth1' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1)"
sudo sed -i -e "s/127.0.0.1/$ip/g" /etc/hosts
sudo sed -i -e "s/{username}/$USER/g" /home/$USER/Stock-Price-Prediction/TwitterDataCollection/flume/conf/flume-env.sh

#Adding JAVA_HOME path in /etc/environment
echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" | sudo tee -a /etc/environment

#Adding required contents in .bashrc
FLUME_HOME="/home/$USER/Stock-Price-Prediction/TwitterDataCollection/flume"
echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
module load java
#Flume variables start
export FLUME_HOME=$FLUME_HOME
export FLUME_CONF_DIR=\$FLUME_HOME/conf
export FLUME_CLASSPATH=\$FLUME_CONF_DIR
export PATH=\$FLUME_HOME"/bin:\$PATH"
#Flume variables end" >> ~/.bashrc

#Source these files for reload
source /etc/environment
source /etc/hosts
source ~/.bashrc

#Create file core-site.xml
echo "<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>" > hadoop-2.9.2/etc/hadoop/core-site.xml

#Create file hdfs-site.xml
echo "<configuration>
    <property>
		<name>dfs.secondary.http.address</name>
		<value>hdfs://localhost:50090</value>
		<description>SecondaryNameNodeHostname</description>
    </property>
</configuration>" > hadoop-2.9.2/etc/hadoop/hdfs-site.xml

#Not sure if this is needed. But keeping it as it does not harm
echo "localhost" > hadoop-2.9.2/etc/hadoop/masters
echo "localhost" > hadoop-2.9.2/etc/hadoop/slaves

#Password-less ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

#Hadoop setup and making directories
hadoop-2.9.2/bin/hdfs namenode -format
hadoop-2.9.2/sbin/start-dfs.sh
hadoop-2.9.2/bin/hdfs dfs -mkdir /user
hadoop-2.9.2/bin/hdfs dfs -mkdir /user/tweets

#Starting flume and getting 
chmod +x flume/bin/flume-ng
./flume/bin/flume-ng agent -n TwitterAgent -f flume/conf/flume-twitter-vcl.conf -c flume/conf/ -Dflume.root.looger=INFO,console
