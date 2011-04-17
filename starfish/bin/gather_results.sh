#!/usr/bin/env bash

###############################################################################
# This script is used to gather the history, profile, and transfer files 
# after a job is profiled.
#
# Usage:
#  ./gather_results.sh <job_id> <results_dir> [<slaves_file>]
#  
#  where:
#    job_id      = The job id of interest
#    results_dir = Directory to place the result files
#    slaves_file = File with slave names (to gather data transfers) - optional
#
# This script will create three directories under <results_dir>, namely,
# history, profiles, and transfers, to place in the corresponding files.
#
# Warning: You must run this script from the same directory you run the
#          hadoop jar command.
#
# Examples:
#  ./gather_results.sh job_201001052153_0021 ./results
#  ./gather_results.sh job_201001052153_0021 ./results ~/slaves.txt
#
# Author: Herodotos Herodotou
# Date: November 5, 2010
##############################################################################


# Make sure we have all the arguments
if [ $# -ne 2 ] && [ $# -ne 3 ]; then
   printf "Usage: $0 <job_id> <results_dir> [<slaves_file>]\n"
   printf "  job_id      = The job id of interest\n"
   printf "  results_dir = Directory to place the result files\n"
   printf "  slaves_file = File with slave names (to gather data transfers) - optional\n"
   exit -1
fi

# Get the input data
declare JOB_ID=$1;
declare RESULTS_DIR=$2;
declare SLAVES_FILE=$3

# Error checking
if test ! -d $RESULTS_DIR; then
   printf "ERROR: The directory '$RESULTS_DIR' does not exist. Exiting\n"
   exit -1
fi

if [ $# -eq 3 ] && [ ! -e $SLAVES_FILE ]; then
   printf "ERROR: The file '$SLAVES_FILE' does not exist. Exiting\n"
   exit -1
fi

# Execute the hadoop-env.sh script for environmental variable definitions
declare HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"
if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
   . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# Find the hadoop log directories
declare HADOOP_LOG_DIR="${HADOOP_LOG_DIR:-$HADOOP_HOME/logs}"
declare MR_USERLOG_DIR="${HADOOP_LOG_DIR}/userlogs"


# Echo the input
printf "Input Parameters:\n"
printf "  Job id: $JOB_ID\n"
printf "  Results directory: $RESULTS_DIR\n\n"

# Get the history files
mkdir -p $RESULTS_DIR/history
cp $HADOOP_LOG_DIR/history/*$JOB_ID* $RESULTS_DIR/history/.

# Get the profiles
mkdir -p $RESULTS_DIR/task_profiles;
declare profile_name="attempt_${JOB_ID/job_/}"
mv $profile_name*.profile $RESULTS_DIR/task_profiles/. &> /dev/null
[ "$(ls -A $RESULTS_DIR/task_profiles)" ] || rmdir $RESULTS_DIR/task_profiles

if [ "$SLAVES_FILE" == "" ]; then
   printf "Done!\n"
   exit 0
fi


# Get the data transfers
mkdir -p $RESULTS_DIR/transfers;
declare attempt_dirname_pattern="attempt_${JOB_ID/job_/}_r_"
declare -a reduce_attempt_array
declare reduce_attempt_str
declare reduce_attempt

#  For each slave host
for slave in `cat "$SLAVES_FILE"`; do
{
#echo "$slave ls -1t ${MR_USERLOG_DIR} | grep '${attempt_dirname_pattern}'\n"
    reduce_attempt_str=`ssh $HADOOP_SSH_OPTS $slave "ls -1t ${MR_USERLOG_DIR} | grep '${attempt_dirname_pattern}'"`
    reduce_attempt_array=(`echo $reduce_attempt_str | tr '\n' ' '`)
    
    for reduce_attempt in ${reduce_attempt_array[@]}; do
    {
#echo "$reduce_attempt\n"
        if [ "$reduce_attempt" != "" ]; then
            # Grep the data transfers from the syslog into a file and move it locally
            ssh $HADOOP_SSH_OPTS $slave "egrep 'Shuffling|Read|Failed' ${MR_USERLOG_DIR}/${reduce_attempt}/syslog > /tmp/transfers_${reduce_attempt}"
            scp $HADOOP_SSH_OPTS $slave:/tmp/transfers_${reduce_attempt} "$RESULTS_DIR/transfers/."
            ssh $HADOOP_SSH_OPTS $slave "rm -f /tmp/transfers_${reduce_attempt}"
        fi
    }
    done

    if [ "$HADOOP_SLAVE_SLEEP" != "" ]; then
        sleep $HADOOP_SLAVE_SLEEP
    fi
} &
done
wait

# Done
printf "Done!\n"

