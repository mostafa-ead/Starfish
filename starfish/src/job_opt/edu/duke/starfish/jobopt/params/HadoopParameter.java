package edu.duke.starfish.jobopt.params;

/**
 * This enum lists a subset of the Hadoop configuration parameters that the Job
 * Optimizer can consider.
 * 
 * @author hero
 */
public enum HadoopParameter {

	SORT_MB, // "io.sort.mb"
	SPILL_PERC, // "io.sort.spill.percent"
	SORT_REC_PERC, // "io.sort.record.percent"
	SORT_FACTOR, // "io.sort.factor"
	NUM_SPILLS_COMBINE, // "min.num.spills.for.combine"

	RED_TASKS, // "mapred.reduce.tasks"
	INMEM_MERGE, // "mapred.inmem.merge.threshold"
	SHUFFLE_IN_BUFF_PERC, // "mapred.job.shuffle.input.buffer.percent"
	SHUFFLE_MERGE_PERC, // "mapred.job.shuffle.merge.percent"
	RED_IN_BUFF_PERC, // "mapred.job.reduce.input.buffer.percent"
	RED_SLOWSTART_MAPS, // "mapred.reduce.slowstart.completed.maps"

	COMBINE, // "starfish.use.combiner"
	COMPRESS_MAP_OUT, // "mapred.compress.map.output"
	COMPRESS_OUT; // "mapred.output.compress"

	@Override
	public String toString() {
		switch (this) {
		case SORT_MB:
			return "io.sort.mb";
		case SPILL_PERC:
			return "io.sort.spill.percent";
		case SORT_REC_PERC:
			return "io.sort.record.percent";
		case SORT_FACTOR:
			return "io.sort.factor";
		case NUM_SPILLS_COMBINE:
			return "min.num.spills.for.combine";

		case RED_TASKS:
			return "mapred.reduce.tasks";
		case INMEM_MERGE:
			return "mapred.inmem.merge.threshold";
		case SHUFFLE_IN_BUFF_PERC:
			return "mapred.job.shuffle.input.buffer.percent";
		case SHUFFLE_MERGE_PERC:
			return "mapred.job.shuffle.merge.percent";
		case RED_IN_BUFF_PERC:
			return "mapred.job.reduce.input.buffer.percent";
		case RED_SLOWSTART_MAPS:
			return "mapred.reduce.slowstart.completed.maps";

		case COMBINE:
			return "starfish.use.combiner";
		case COMPRESS_MAP_OUT:
			return "mapred.compress.map.output";
		case COMPRESS_OUT:
			return "mapred.output.compress";

		}

		return super.toString();
	}

	/**
	 * @return a description of the Hadoop parameter
	 */
	public String getDescription() {
		switch (this) {
		case SORT_MB:
			return "Size (MB) of map-side buffer for storing and sorting key-value pairs produced by the map function";
		case SPILL_PERC:
			return "Usage threshold of map-side memory buffer to trigger a sort and spill of the stored key-value pairs";
		case SORT_REC_PERC:
			return "Fraction of io.sort.mb for storing metadata for every key-value pair stored in the map-side buffer";
		case SORT_FACTOR:
			return "Number of sorted streams to merge at once during multiphase external sorting";
		case NUM_SPILLS_COMBINE:
			return "Minimum number of spill files to trigger the use of Combiner during the merging of map output data";

		case RED_TASKS:
			return "Number of reduce tasks";
		case INMEM_MERGE:
			return "Threshold on the number of copied map outputs to trigger reduce-side merging during the shuffle";
		case SHUFFLE_IN_BUFF_PERC:
			return "Percent of reduce task's heap memory used to buffer output data copied from map tasks during the shuffle";
		case SHUFFLE_MERGE_PERC:
			return "Usage threshold of reduce-side memory buffer to trigger reduce-side merging during the shuffle";
		case RED_IN_BUFF_PERC:
			return "Percent of reduce task's heap memory used to buffer map output data while applying the reduce function";
		case RED_SLOWSTART_MAPS:
			return "Proportion of map tasks that need to be completed before any reduce tasks are scheduled";

		case COMBINE:
			return "Flag to use Combiner function to preaggregate map outputs before transfer to reduce tasks";
		case COMPRESS_MAP_OUT:
			return "Boolean flag to turn on the compression of map output data";
		case COMPRESS_OUT:
			return "Boolean flag to turn on the compression of the job's output";

		default:
			return toString();
		}
	}
}
