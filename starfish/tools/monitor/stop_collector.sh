#!/usr/bin/env bash

#####################################################################
# Stop iostat and vmstat on all slave hosts and gather the outputs
#
# Usage: ./stop_collector.sh <slaves_file> <monitor_dir> <collect_dir> [rm]
#   slaves_file = file with slave nodes
#   monitor_dir = directory on slaves with monitored data
#   collect_dir = local directory to collect the monitoring files
#   rm          = optional flag to remove the files from the slaves
#
# Author: Herodotos Herodotou
# Date:   Nov 6, 2010
#####################################################################

# if no args specified, show usage
if [ $# -le 2 ]; then
  echo "Usage: $0 <slaves_file> <monitor_dir> <collect_dir> [rm]"
  echo "  slaves_file = file with slave nodes"
  echo "  monitor_dir = directory on slaves with monitored data"
  echo "  collect_dir = local directory to collect the monitoring files"
  echo "  rm          = optional flag to remove the files from the slaves"
  exit 1
fi


# Get input args
declare HADOOP_SLAVES=$1
declare SAVE_PATH=$2
declare LOCAL_PATH=$3
declare REMOVE=$4

# Stop collection and get logs
mkdir -p ${LOCAL_PATH}
for slave in `cat "$HADOOP_SLAVES"|sed  "s/#.*$//;/^$/d"`; 
do
  echo $slave
  ssh $slave 'killall iostat';
  ssh $slave 'killall vmstat';
  scp $slave:${SAVE_PATH}'/iostat_output-'${slave} ${LOCAL_PATH}
  scp $slave:${SAVE_PATH}'/vmstat_output-'${slave} ${LOCAL_PATH}
  
  if [ $REMOVE ] && [ $REMOVE == "rm" ]
  then
     ssh $slave "rm ${SAVE_PATH}/iostat_output-${slave}";
     ssh $slave "rm ${SAVE_PATH}/vmstat_output-${slave}";
  fi
done

