#!/usr/bin/env bash

###############################################################################
# This script can be used to install btrace to all the slaves.
#
# The user must specify the SLAVES_BTRACE_DIR parameter in profile-config.sh
#
# Usage:
#  ./install_btrace.sh <slaves_file>
#  
#  where:
#    slaves_file = File containing a list of slave machines
#
# Example:
#  ./install_btrace.sh /root/SLAVE_NAMES.txt
#
# Author: Herodotos Herodotou
# Date:   Feb 11, 2011
##############################################################################


# Make sure we have all the arguments
if [ $# -ne 1 ]; then
   printf "Usage: $0 <slaves_file>\n"
   printf "  slaves_file = File containing a list of slave machines\n"
   printf "\n"
   printf "Note: In profile-config.sh set SLAVES_BTRACE_DIR,\n"
   printf "      the directory to install btrace to in the slave machines\n"
   exit -1
fi

# Get the slaves file
declare SLAVES_FILE=$1;
if test ! -e $SLAVES_FILE; then
   printf "ERROR: The file '$SLAVES_FILE' does not exist. Exiting\n"
   exit -1
fi

# Get the build and bin directories
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
build=`cd "$bin/../"; pwd`

# Ensure the user has set the BTrace directory
. "$bin"/profile-config.sh
if [ "$SLAVES_BTRACE_DIR" = "" ]; then
  echo "Error: SLAVES_BTRACE_DIR is not set."
  exit 1
fi


# Execute the hadoop-env.sh script for environmental variable definitions
declare HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"
if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
   . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# Echo the input
printf "Starting installation at: "
date
printf "Input Parameters:\n"
printf "  File with slaves: $SLAVES_FILE\n"
printf "  Installation directory: $SLAVES_BTRACE_DIR\n\n"

# Get the source directory
hadoop_btrace=$build/hadoop-btrace

# Connect to each host and copy the files
for slave in `cat "$SLAVES_FILE"`; do
{
   printf "Installing on host: $slave\n"
   ssh $HADOOP_SSH_OPTS $slave "mkdir -p $SLAVES_BTRACE_DIR"
   scp ${hadoop_btrace}/btrace-agent.jar $slave:$SLAVES_BTRACE_DIR/.
   scp ${hadoop_btrace}/btrace-boot.jar $slave:$SLAVES_BTRACE_DIR/.
   scp ${hadoop_btrace}/HadoopBTrace.class $slave:$SLAVES_BTRACE_DIR/.
   scp ${hadoop_btrace}/HadoopBTraceMem.class $slave:$SLAVES_BTRACE_DIR/.
}
done

# Done
printf "\nInstallation completed at: "
date

