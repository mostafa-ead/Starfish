#!/usr/bin/env bash

###################################################################
# The Starfish configuration parameters
#
# Used to set the user-defined parameters in Starfish.
#
# Author: Herodotos Herodotou
# Date:   May 11, 2011
###################################################################

###################################################################
# GLOBAL PARAMETERS (REQUIRED)
###################################################################

# The btrace install directory on the slave machines
# Specify a FULL path! This setting is required!
# Example: SLAVES_BTRACE_DIR=/root/btrace
SLAVES_BTRACE_DIR=

# A descriptive name for the cluster, like test, production, etc.
# No spaces or special characters in the name. This setting is required!
CLUSTER_NAME=


###################################################################
# PROFILING PARAMETERS
###################################################################

# The local directory to place the output files
# This setting is optional, it defaults to the working directory
# Overwritten by the Hadoop parameter starfish.profiler.output.dir
PROFILER_OUTPUT_DIR=results

# Whether to retain the task profiles or not. Default is true
# Overwritten by the Hadoop parameter starfish.profiler.retain.task.profiles
RETAIN_TASK_PROFILES=false

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


###################################################################
# OPTIMIZATION PARAMETERS
###################################################################

# A comma-separated list of parameters to exclude from optimization (no spaces)
# Empty is the default and it means to exclude nothing
# Overwritten by the Hadoop parameter starfish.job.optimizer.exclude.parameters
EXCLUDE_PARAMETERS=""

# Where to print out the best configuration (only for mode: recommend)
# The options are: stdout, stderr, file_path (stdout is default)
# Overwritten by the Hadoop parameter starfish.job.optimizer.output
OUTPUT_LOCATION=stdout


###################################################################
# EXPERIMENTAL PARAMETERS - DON'T WORRY ABOUT THEM!!
###################################################################

# The job optimizer to use.
# The options are: full, smart_full, rrs, smart_rrs (smart_rrs is default)
# Overwritten by the Hadoop parameter starfish.job.optimizer.type
JOB_OPTIMIZER_TYPE=smart_rrs

# The task scheduler to use by the optimizer
# The options are: basic, advanced (advanced is default)
# Overwritten by the Hadoop parameter starfish.whatif.task.scheduler
TASK_SCHEDULER=advanced

# The number of values to consider per parameter
# NOTE: This option is only used by optimizers: full, smart_full
# Overwritten by the Hadoop parameter starfish.job.optimizer.num.values.per.param
JOB_OPTIMIZER_NUM_VALUES_PER_PARAM=2

# Whether the optimizer to use random or equi-distance values per parameter
# NOTE: This option is only used by optimizers: full, smart_full
# Overwritten by the Hadoop parameter starfish.job.optimizer.use.random.values
JOB_OPTIMIZER_USE_RANDOM_VALUES=false

