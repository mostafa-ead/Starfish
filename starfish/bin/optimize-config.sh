#!/usr/bin/env bash

###################################################################
# The Job Optimizer configuration script
#
# Used to set the user-defined parameters to optimize the execution
# of a MapReduce job.
#
# Author: Herodotos Herodotou
# Date:   March 07, 2011
###################################################################

# A comma-separated list of parameters to exclude from optimization (no spaces)
# Empty is the default and it means to exclude nothing
# Overwritten by the Hadoop parameter starfish.job.optimizer.exclude.parameters
EXCLUDE_PARAMETERS=""

# Where to print out the best configuration (only for mode: recommend)
# The options are: stdout, stderr, file_path (stdout is default)
# Overwritten by the Hadoop parameter starfish.job.optimizer.output
OUTPUT_LOCATION=stdout

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

