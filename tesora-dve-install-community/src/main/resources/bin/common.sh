#! /bin/sh

###
# #%L
# Tesora Inc.
# Database Virtualization Engine
# %%
# Copyright (C) 2011 - 2014 Tesora Inc.
# %%
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# #L%
###
#
# Copyright © ${build.copyright.date} Tesora Corporation.  All Rights Reserved.
#
# All use, reproduction, transfer, publication or disclosure is prohibited
# except as may be expressly permitted in the applicable license agreement.
#

# set -x

_findpid() {
	if [ -z "$1" ]; then
		echo "... must pass parameter to _findpid()" 
        exit 1
    fi
	
    OS=`uname -s`
    if [ "$OS" = "Linux" ]; then
        ps -eo 'pid,cmd' | grep "$1" | grep -v "grep" | grep -v "restarter" | awk '{print $1}'
    elif [ "$OS" = "Darwin" ]; then
        ps -aef | grep "$1" | grep -v "grep" | grep -v "restarter" | awk '{print $2}'
    elif [ "$OS" = "SunOS" ]; then
        /usr/ucb/ps -auxww | grep "$1" | grep -v "grep" | grep -v "restarter" | awk '{print $2}'
    else
        ps | grep java | cut -f 5-7 -d ' '
    fi
}

# Function: _daemon_run
# Starts up a Java class as a daemon with start, stop, status etc control
# Parameters:
#	OPERATION - start|stop|restart|force-reload|status
#	PROC_DESC - description of the process
#   PROC_SHORTDESC - short (i.e. one word) description for process - used in file names
#   PROC_MAINCLASS - fully qualified class name where the Main() method is
#   PROC_JAVA_ARGS - Any process specific JVM args
#
_daemon_run() {
	if [ "$JAVA_VER" -lt 17 ]; then
	    echo "A java VM of version 1.7 or greater is required to run this script" 1>&2
	    exit 1
	fi
	
	if [ $# -lt 5 ]; then
		echo "Not enough parameters ($#) were supplied to _daemon_run()"
		exit 1
	fi
	
	OPERATION=$1
	PROC_DESC=$2 
	PROC_SHORTDESC=$3 
	PROC_MAINCLASS=$4 
	PROC_JAVA_ARGS=$5 
	shift 5
	
	case $OPERATION in
	    start)
	        PID=`_findpid $PROC_MAINCLASS`
	        if [ -n "$PID" ]; then
	            echo "$PROC_DESC is already running (pid $PID)."
	        else
	            echo "Starting $PROC_DESC ... "
	            echo "    Product files = $PROD_DIR"
	            echo "    Log files     = $LOG_DIR"
	            echo "    Config files  = $CONF_DIR"
	
	            JAVA_ARGS="$USER_JAVA_ARGS $PROC_JAVA_ARGS -Dlog4j.configuration=${PROC_SHORTDESC}log4j.properties -Dtesora.dve.log=$LOG_DIR"

	            nohup "$INSTALL_DIR/bin/restarter.sh" "$JAVA_PROG" -classpath "$CLASSPATH" $JAVA_ARGS $PROC_MAINCLASS "$@" >> $LOG_DIR/${PROC_SHORTDESC}-console.log 2>&1 &

	            sleep 10
	
	            PID=`_findpid $PROC_MAINCLASS`
	            if [ -n "$PID" ]; then
	                echo "... $PROC_DESC started (pid $PID)."
	            else
	                echo "... $PROC_DESC failed to start. Check the log files in $LOG_DIR for more information." 
	                exit 1
	            fi
	        fi
	        ;;
	    stop)
	        PID=`_findpid $PROC_MAINCLASS`
	        if [ -n "$PID" ]; then
	            echo "Stopping $PROC_DESC (pid $PID) ... "
	            kill $PID
	            sleep 5
	            PID=`_findpid $PROC_MAINCLASS`
	            if [ -n "$PID" ]; then
	                echo "... $PROC_DESC is still running, killing process $PID ... "
	                kill -9 $PID
	            fi
	            echo "... $PROC_DESC stopped."
	        else
	            echo "$PROC_DESC is not running."
	        fi
	        ;;
	    restart|force-reload)
	        echo "Restarting $PROC_DESC ... "
	        $0 stop
	        $0 start
	          ;;
	    status)
	        PID=`_findpid $PROC_MAINCLASS`
	        if [ -n "$PID" ]; then
	            echo "$PROC_DESC (pid $PID) is running."
	        else
	            echo "$PROC_DESC is not running."
	            exit 3
	        fi
	        ;;
	    *) 	
	        echo "Usage: $0 <start|stop|restart|force-reload|status>" >&2
	        exit 2
	        ;;
	esac
	
	exit 0
}

# INSTALL_DIR is resolved to the parent directory for this script
INSTALL_DIR=${INSTALL_DIR:-$(cd $(dirname "$0")/..; pwd)}

# Determine the location of java executable
if [ -z "$JAVA_HOME" ]; then
    JAVA_PROG=`which java`
else
    JAVA_PROG=$JAVA_HOME/bin/java
fi

if [ ! -f "$JAVA_PROG" ]; then
    echo "A java VM is required to run this script" 1>&2
    exit 1
fi

# Determine Java version on this machine
JAVA_VER=$($JAVA_PROG -version 2>&1 | sed 's/java version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

# Determine where config files and log files live go based on how we are installed
if [ ! -e "/etc/tesora/dve/com" ]; then
    LOG_DIR=$INSTALL_DIR/logs
    CONF_DIR=$INSTALL_DIR/config
    PROD_DIR=$INSTALL_DIR
else
    LOG_DIR=/var/log/tesora/dve
    CONF_DIR=/etc/tesora/dve
    PROD_DIR=/opt/tesora/dve/server
fi	

# Force LANG to be set to UTF-8.
# For different LANG, set in user-config.conf.
export LANG="en_US.UTF-8"

if [ -z "$CONFIG_FILE" ]; then
	CONFIG_FILE="$CONF_DIR/user-config.conf"
fi
	
if [ -f $CONFIG_FILE ]; then
	. $CONFIG_FILE
fi

CLASSPATH="$DVE_CLASSPATH:$CONF_DIR:$INSTALL_DIR/lib/tesora-dve-core-priv.jar:$INSTALL_DIR/lib/tesora-dve-priv-int.jar:$INSTALL_DIR/lib/*:$INSTALL_DIR/lib/ext/*:$INSTALL_DIR/lib/thirdparty/*"
