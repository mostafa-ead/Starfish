package edu.duke.starfish.profile.profileinfo.execution.profile.enums;

/**
 * Enumerates the possible execution counters
 * 
 * @author hero
 */
public enum MRCounter {

	MAP_TASKS, // Number of map tasks in the job
	REDUCE_TASKS, // Number of reduce tasks in the job

	MAP_INPUT_RECORDS, // Map input records
	MAP_INPUT_BYTES, // Map input bytes
	MAP_OUTPUT_RECORDS, // Map output records
	MAP_OUTPUT_BYTES, // Map output bytes
	MAP_SKIPPED_RECORDS, // Map skipped records
	MAP_NUM_SPILLS, // Number of spills
	MAP_NUM_SPILL_MERGES, // Number of merge rounds
	MAP_RECS_PER_BUFF_SPILL, // Number of records in buffer per spill
	MAP_BUFF_SPILL_SIZE, // Buffer size (bytes) per spill
	MAP_RECORDS_PER_SPILL, // Number of records in spill file
	MAP_SPILL_SIZE, // Spill file size (bytes)
	MAP_MAX_UNIQUE_GROUPS, // Maximum number of unique groups

	REDUCE_SHUFFLE_BYTES, // Shuffle size (bytes)
	REDUCE_INPUT_GROUPS, // Reduce input groups (unique keys)
	REDUCE_INPUT_RECORDS, // Reduce input records
	REDUCE_INPUT_BYTES, // Reduce input bytes
	REDUCE_OUTPUT_RECORDS, // Reduce output records
	REDUCE_OUTPUT_BYTES, // Reduce output bytes
	REDUCE_SKIPPED_RECORDS, // Reduce skipped records
	REDUCE_SKIPPED_GROUPS, // Reduce skipped groups

	COMBINE_INPUT_RECORDS, // Combine input records
	COMBINE_OUTPUT_RECORDS, // Combine output records

	SPILLED_RECORDS, // Total spilled records

	FILE_BYTES_READ, // Bytes read from local file system
	FILE_BYTES_WRITTEN, // Bytes written to local file system
	HDFS_BYTES_READ, // Bytes read from HDFS
	HDFS_BYTES_WRITTEN; // Bytes written to HDFS

	/**
	 * @return a description for the counter
	 */
	public String getDescription() {

		switch (this) {
		case MAP_TASKS:
			return "Number of map tasks in the job";
		case REDUCE_TASKS:
			return "Number of reduce tasks in the job";

		case MAP_INPUT_RECORDS:
			return "Map input records";
		case MAP_INPUT_BYTES:
			return "Map input bytes";
		case MAP_OUTPUT_RECORDS:
			return "Map output records";
		case MAP_OUTPUT_BYTES:
			return "Map output bytes";
		case MAP_SKIPPED_RECORDS:
			return "Map skipped records";
		case MAP_NUM_SPILLS:
			return "Number of spills";
		case MAP_NUM_SPILL_MERGES:
			return "Number of merge rounds";
		case MAP_RECS_PER_BUFF_SPILL:
			return "Number of records in buffer per spill";
		case MAP_BUFF_SPILL_SIZE:
			return "Buffer size (bytes) per spill";
		case MAP_RECORDS_PER_SPILL:
			return "Number of records in spill file";
		case MAP_SPILL_SIZE:
			return "Spill file size (bytes)";
		case MAP_MAX_UNIQUE_GROUPS:
			return "Maximum number of unique groups";

		case REDUCE_SHUFFLE_BYTES:
			return "Shuffle size (bytes)";
		case REDUCE_INPUT_GROUPS:
			return "Reduce input groups (unique keys)";
		case REDUCE_INPUT_RECORDS:
			return "Reduce input records";
		case REDUCE_INPUT_BYTES:
			return "Reduce input bytes";
		case REDUCE_OUTPUT_RECORDS:
			return "Reduce output records";
		case REDUCE_OUTPUT_BYTES:
			return "Reduce output bytes";
		case REDUCE_SKIPPED_RECORDS:
			return "Reduce skipped records";
		case REDUCE_SKIPPED_GROUPS:
			return "Reduce skipped groups";

		case COMBINE_INPUT_RECORDS:
			return "Combine input records";
		case COMBINE_OUTPUT_RECORDS:
			return "Combine output records";
		case SPILLED_RECORDS:
			return "Total spilled records";

		case FILE_BYTES_READ:
			return "Bytes read from local file system";
		case FILE_BYTES_WRITTEN:
			return "Bytes written to local file system";
		case HDFS_BYTES_READ:
			return "Bytes read from HDFS";
		case HDFS_BYTES_WRITTEN:
			return "Bytes written to HDFS";
		default:
			return toString();
		}
	}
}
