#!/usr/bin/env bash

###################################################################
# Launches the visualizer.
#
# Required environment parameters:
#    HADOOP_HOME
#    JAVAFX_HOME
#
# Author: Herodotos Herodotou
# Date:   May 19, 2011
###################################################################


# Get the visualizer directory
CURRENT_DIR=`pwd`
VIS_DIR=`dirname "$0"`
VIS_DIR=`cd "$VIS_DIR"; pwd`
cd $CURRENT_DIR;

# Get and check the visualizer jar
VISUALIZER=`ls $VIS_DIR/starfish-*-visualizer.jar`

if [ "${VISUALIZER}" = "" ]; then
   echo "ERROR: starfish-*-visualizer.jar does not exist."
   echo "       Use 'ant' to build the project first"
   exit -1
fi

# Ensure the user has set the necessary environment parameters
if [ "${HADOOP_HOME}" = "" ]; then
  echo "Error: HADOOP_HOME is not set."
  exit -1
fi

if [ "${JAVAFX_HOME}" = "" ]; then
  echo "Error: JAVAFX_HOME is not set."
  exit -1
fi

# Add Hadoop jars and libs to CLASSPATH
for f in $HADOOP_HOME/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
for f in $HADOOP_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# Execute the visualizer
${JAVAFX_HOME}/bin/javafx -jar $VISUALIZER -classpath $CLASSPATH
