# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi
module load java
# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

# User specific aliases and functions

#Flume variables start
export FLUME_HOME=/home/$USER/Stock-Price-Prediction/TwitterDataCollection/flume
export FLUME_CONF_DIR=$FLUME_HOME/conf
export FLUME_CLASSPATH=$FLUME_CONF_DIR
export PATH="$FLUME_HOME/bin:$PATH"
#Flume variables end