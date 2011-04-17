package edu.duke.starfish.whatif;

/**
 * Contains a set of constants to be used by the What-if Engine
 * 
 * @author hero
 */
public class Constants {

	// Constants for Hadoop parameters
	public static final String MR_JAVA_OPTS = "mapred.child.java.opts";
	public static final String MR_MAX_MAP_TASKS = "mapred.tasktracker.map.tasks.max";
	public static final String MR_MAX_RED_TASKS = "mapred.tasktracker.reduce.tasks.max";

	public static final String MR_MAP_TASKS = "mapred.map.tasks";
	public static final String MR_SORT_MB = "io.sort.mb";
	public static final String MR_SPILL_PERC = "io.sort.spill.percent";
	public static final String MR_SORT_REC_PERC = "io.sort.record.percent";
	public static final String MR_SORT_FACTOR = "io.sort.factor";
	public static final String MR_NUM_SPILLS_COMBINE = "min.num.spills.for.combine";

	public static final String MR_RED_TASKS = "mapred.reduce.tasks";
	public static final String MR_INMEM_MERGE = "mapred.inmem.merge.threshold";
	public static final String MR_SHUFFLE_IN_BUFF_PERC = "mapred.job.shuffle.input.buffer.percent";
	public static final String MR_SHUFFLE_MERGE_PERC = "mapred.job.shuffle.merge.percent";
	public static final String MR_RED_IN_BUFF_PERC = "mapred.job.reduce.input.buffer.percent";
	public static final String MR_RED_SLOWSTART_MAPS = "mapred.reduce.slowstart.completed.maps";

	public static final String MR_COMBINE_CLASS = "mapreduce.combine.class";
	public static final String MR_COMPRESS_MAP_OUT = "mapred.compress.map.output";
	public static final String MR_COMPRESS_OUT = "mapred.output.compress";

	public static final String MR_INPUT_FORMAT_CLASS = "mapreduce.inputformat.class";
	public static final String MR_INPUT_DIR = "mapred.input.dir";

	// Default values for Hadoop parameters
	public static final long DEF_TASK_MEM = 200l * 1024 * 1024;
	public static final int DEF_MAX_MAP_TASKS = 2;
	public static final int DEF_MAX_RED_TASKS = 2;

	public static final int DEF_SORT_MB = 100;
	public static final float DEF_SPILL_PERC = 0.8f;
	public static final float DEF_SORT_REC_PERC = 0.05f;
	public static final int DEF_SORT_FACTOR = 10;
	public static final int DEF_NUM_SPILLS_FOR_COMB = 3;

	public static final int DEF_INMEM_MERGE = 1000;
	public static final float DEF_SHUFFLE_IN_BUFF_PERC = 0.7f;
	public static final float DEF_SHUFFLE_MERGE_PERC = 0.66f;
	public static final float DEF_RED_IN_BUFF_PERC = 0f;
	public static final float DEF_RED_SLOWSTART_MAPS = 0.05f;

	// Default values for Counters
	public static final long DEF_SPLIT_SIZE = 64l * 1024 * 1024;
	public static final long DEF_MAX_UNIQUE_GROUPS = 1l;

	// Default values for Statistics
	public static final double DEF_PAIR_WIDTH = 100d;
	public static final double DEF_RED_PAIRS_PER_GROUP = 1d;
	public static final double DEF_SEL_ONE = 1d;
	public static final double DEF_COMPRESS_RATIO = 0.3d;
	public static final double DEF_MEM = 10d * 1024 * 1024;
	public static final double DEF_MEM_PER_REC = 100d;

	// Default values for Cost Factors
	public static final double DEF_COST_CPU_UNCOMPRESS = 100d;
	public static final double DEF_COST_CPU_COMPRESS = 150d;
	public static final double DEF_COST_CPU_COMBINE = 4000d;

	// Other constants
	public static final String STARFISH_USE_COMBINER = "starfish.use.combiner";
	public static final double NS_PER_MS = 1000000d;

}
