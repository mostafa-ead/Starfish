package edu.duke.starfish.profile.profileinfo.execution.profile.enums;

/**
 * Enumerates all the cost factors used in the performance model
 * 
 * @author hero
 */
public enum MRCostFactors {

	READ_HDFS_IO_COST, // I/O cost for reading from HDFS
	WRITE_HDFS_IO_COST, // I/O cost for writing to HDFS
	READ_LOCAL_IO_COST, // I/O cost for reading from local disk
	WRITE_LOCAL_IO_COST, // I/O cost for writing to local disk

	NETWORK_COST, // Cost for network transfers

	MAP_CPU_COST, // CPU cost for executing the Mapper
	REDUCE_CPU_COST, // CPU cost for executing the Reducer
	COMBINE_CPU_COST, // CPU cost for executing the Combiner

	PARTITION_CPU_COST, // CPU cost for partitioning
	SERDE_CPU_COST, // CPU cost for serializing/deserializing
	SORT_CPU_COST, // CPU cost for sorting
	MERGE_CPU_COST, // CPU cost for merging

	INPUT_UNCOMPRESS_CPU_COST, // CPU cost for uncompressing the input
	INTERM_UNCOMPRESS_CPU_COST, // CPU cost for uncompressing map output
	INTERM_COMPRESS_CPU_COST, // CPU cost for compressing map output
	OUTPUT_COMPRESS_CPU_COST, // CPU cost for compressing the output

	SETUP_CPU_COST, // CPU cost for task setup
	CLEANUP_CPU_COST; // CPU cost for task cleanup

	/**
	 * @return a description for the cost factor
	 */
	public String getDescription() {

		switch (this) {
		case READ_HDFS_IO_COST:
			return "I/O cost for reading from HDFS";
		case WRITE_HDFS_IO_COST:
			return "I/O cost for writing to HDFS";
		case READ_LOCAL_IO_COST:
			return "I/O cost for reading from local disk";
		case WRITE_LOCAL_IO_COST:
			return "I/O cost for writing to local disk";
		case NETWORK_COST:
			return "Cost for network transfers";
		case MAP_CPU_COST:
			return "CPU cost for executing the Mapper";
		case REDUCE_CPU_COST:
			return "CPU cost for executing the Reducer";
		case COMBINE_CPU_COST:
			return "CPU cost for executing the Combiner";
		case PARTITION_CPU_COST:
			return "CPU cost for partitioning";
		case SERDE_CPU_COST:
			return "CPU cost for serializing/deserializing";
		case SORT_CPU_COST:
			return "CPU cost for sorting";
		case MERGE_CPU_COST:
			return "CPU cost for merging";
		case INPUT_UNCOMPRESS_CPU_COST:
			return "CPU cost for uncompressing the input";
		case INTERM_UNCOMPRESS_CPU_COST:
			return "CPU cost for uncompressing map output";
		case INTERM_COMPRESS_CPU_COST:
			return "CPU cost for compressing map output";
		case OUTPUT_COMPRESS_CPU_COST:
			return "CPU cost for compressing the output";
		case SETUP_CPU_COST:
			return "CPU cost for task setup";
		case CLEANUP_CPU_COST:
			return "CPU cost for task cleanup";
		default:
			return toString();
		}
	}
}
