#!/usr/bin/env bash

#####################################################################
# Start iostat and vmstat on all slave hosts.
#
# Usage: ./start_collector.sh <slaves_file> <monitor_dir>
#   slaves_file = file with slave nodes
#   monitor_dir = directory on slaves to store the monitored data
#                 Specify full path! Created if doesn't exist.
#
# Author: Herodotos Herodotou
# Date:   Nov 6, 2010
#####################################################################

# if no args specified, show usage
if [ $# -le 1 ]; then
   echo "Usage: $0 <slaves_file> <monitor_dir>"
   echo "  slaves_file = File with slave nodes"
   echo "  monitor_dir = Directory on slaves to store the monitored data"
   echo "                Specify full path! Created if doesn't exist."
   exit -1
fi

# Get input args
declare HADOOP_SLAVES=$1
declare SAVE_PATH=$2

# Start the collection
for slave in `cat "$HADOOP_SLAVES"|sed  "s/#.*$//;/^$/d"`; 
do
   echo $slave
   ssh $slave "mkdir -p ${SAVE_PATH}"
   ssh $slave "nohup iostat -m 3 | perl -e 'while (<>) { print time() . \" \$_\"; }' > ${SAVE_PATH}/iostat_output-${slave} &" 
   ssh $slave "nohup vmstat 3 | perl -e 'while (<>) { print time() . \" \$_\"; }' > ${SAVE_PATH}/vmstat_output-${slave} &" 
done

