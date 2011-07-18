#!/usr/bin/env bash

###################################################################
# The gather_results command script
#
# This script is used to gather the history, profile, and transfer 
# files after a job completes execution. This script is useful
# when a job is not profiled, or when the job is profiled
# without the bin/profile script.
#
# The user should not need to modify this script. All user-defined
# parameters can be set in 'config.sh'.
#
# Author: Herodotos Herodotou
# Date:   July 11, 2011
###################################################################


# if no args specified, show usage
if [ $# = 0 ]; then
  echo "Usage:"
  echo "    $0 job_id"
  echo ""
  echo "General parameters are set in bin/config.sh"
  echo ""
  exit 1
fi

# Perform common tasks like Load configurations and initializations
bin=`dirname "$0"`
. "$bin"/common.sh


# Default output directory is the working directory
if [ "$PROFILER_OUTPUT_DIR" = "" ]; then
  PROFILER_OUTPUT_DIR=$CURRENT_DIR
fi
HADOOP_OPTS="${HADOOP_OPTS} -Dstarfish.profiler.output.dir=${PROFILER_OUTPUT_DIR}"

# Flag for retaining the task profiles
if [ "$RETAIN_TASK_PROFILES" = "" ]; then
  RETAIN_TASK_PROFILES=true
fi
HADOOP_OPTS="${HADOOP_OPTS} -Dstarfish.profiler.retain.task.profiles=${RETAIN_TASK_PROFILES}"

# Flag for collecting the data transfers
if [ "$COLLECT_DATA_TRANSFERS" = "" ]; then
  COLLECT_DATA_TRANSFERS=false
fi
HADOOP_OPTS="${HADOOP_OPTS} -Dstarfish.profiler.collect.data.transfers=${COLLECT_DATA_TRANSFERS}"

# Flag to enable profiling sampling
if [ "$SAMPLING_MODE" = "" ]; then
  SAMPLING_MODE="off"
fi
HADOOP_OPTS="${HADOOP_OPTS} -Dstarfish.profiler.sampling.mode=${SAMPLING_MODE}"

# The sampling fraction
if [ "$SAMPLING_FRACTION" = "" ]; then
  SAMPLING_FRACTION="0.1"
fi
HADOOP_OPTS="${HADOOP_OPTS} -Dstarfish.profiler.sampling.fraction=${SAMPLING_FRACTION}"


# Add the profiler jar to the classpath
HADOOP_CLASSPATH_OLD=$HADOOP_CLASSPATH
HADOOP_CLASSPATH=`ls $BASE_DIR/starfish-*-profiler.jar`
if [ "$HADOOP_CLASSPATH_OLD" != "" ]; then
  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${HADOOP_CLASSPATH_OLD}
fi

# Add any user-defined jars to the classpath
OLD_IFS="$IFS"
IFS=" "
args=( $@ )

size=${#@};
for (( i = 0 ; i < size ; i++ ));
do
  if [ "${args[${i}]}" = "-libjars" ]; then
     IFS=","
     for userjar in ${args[${i}+1]}
     do
        HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:$userjar"
     done
  fi
done
IFS="$OLD_IFS"

# Export the required hadoop parameters
export HADOOP_OPTS
export HADOOP_CLASSPATH

# Execute the hadoop command
${HADOOP_HOME}/bin/hadoop edu.duke.starfish.profile.utils.GatherResults "$1"
