#!/usr/bin/env bash

###################################################################
# A script that loads the configuration settings from config.sh
# and performs various initializations and error checks.
#
# The following parameters are created:
#   CURRENT_DIR
#   BIN_DIR
#   BASE_DIR
#   MASTER_BTRACE_DIR
#   HADOOP_OPTS
#
# The user should not need to modify this script. All user-defined
# parameters can be set in 'config.sh'.
#
# Author: Herodotos Herodotou
# Date:   May 11, 2011
###################################################################

# Get the current, bin, and base directories
CURRENT_DIR=`pwd`
BIN_DIR=`dirname "$0"`
BIN_DIR=`cd "$BIN_DIR"; pwd`
BASE_DIR=`cd "$BIN_DIR/../"; pwd`
cd $CURRENT_DIR;

MASTER_BTRACE_DIR=$BASE_DIR/hadoop-btrace

# Get the user-defined parameters
. "$BIN_DIR"/config.sh

# Ensure the user has set the BTrace directory on the slaves
if [ "$SLAVES_BTRACE_DIR" = "" ]; then
  echo "Error: SLAVES_BTRACE_DIR is not set."
  echo "       Please set it in $BIN_DIR/config.sh "
  echo "       and run $BIN_DIR/install-btrace.sh"
  exit -1
fi
HADOOP_OPTS="${HADOOP_OPTS} -Dbtrace.profile.dir=${SLAVES_BTRACE_DIR}"

# Ensure the user has set the cluster name
if [ "$CLUSTER_NAME" = "" ]; then
  echo "Error: CLUSTER_NAME is not set."
  echo "       Please set it in $BIN_DIR/config.sh"
  exit -1
fi
HADOOP_OPTS="${HADOOP_OPTS} -Dstarfish.profiler.cluster.name=${CLUSTER_NAME}"

