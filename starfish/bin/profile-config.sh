#!/usr/bin/env bash

###################################################################
# The Profile configuration script
#
# Used to set the user-defined parameters to profile the execution
# of a MapReduce job.
#
# Author: Herodotos Herodotou
# Date:   February 10, 2011
###################################################################


# The btrace install directory on the slave machines
# Specify a FULL path! This setting is required!
# Example: SLAVES_BTRACE_DIR=/root/btrace
SLAVES_BTRACE_DIR=

# The local directory to place the output files
# This setting is optional, it defaults to the working directory
# Overwritten by the Hadoop parameter starfish.profiler.output.dir
PROFILER_OUTPUT_DIR=results

# Whether to retain the task profiles or not. Default is true
# Overwritten by the Hadoop parameter starfish.profiler.retain.task.profiles
RETAIN_TASK_PROFILES=true

# Whether to collect the data transfers among the tasks. Default is false
# Overwritten by the Hadoop parameter starfish.profiler.collect.data.transfers
COLLECT_DATA_TRANSFERS=false


# The sampling mode. Possible values: off, profiles, tasks
#   - off:      No sampling is done. All tasks are run and profiled. (Default)
#   - profiles: All tasks are run but only a fraction is profiled
#   - tasks:    A fraction of the map tasks are run and profiled
# Overwritten by the Hadoop parameter starfish.profiler.sampling.mode
SAMPLING_MODE=off

# The fraction of tasks to profile or run when the sampling mode is not 'off'.
# Warning: It must be a value between 0 and 1. Default is 0.1
# Overwritten by the Hadoop parameter starfish.profiler.sampling.fraction
SAMPLING_FRACTION=0.1


