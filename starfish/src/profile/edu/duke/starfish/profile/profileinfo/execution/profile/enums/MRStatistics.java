package edu.duke.starfish.profile.profileinfo.execution.profile.enums;

/**
 * Enumerates several job statistics used by the performance model
 * 
 * @author hero
 */
public enum MRStatistics {

	INPUT_PAIR_WIDTH, // Average width of input key-value pairs
	REDUCE_PAIRS_PER_GROUP, // Number of records per reducer's group

	MAP_SIZE_SEL, // Map selectivity in terms of size
	MAP_PAIRS_SEL, // Map selectivity in terms of records
	REDUCE_SIZE_SEL, // Reducer selectivity in terms of size
	REDUCE_PAIRS_SEL, // Reducer selectivity in terms of records
	COMBINE_SIZE_SEL, // Combiner selectivity in terms of size
	COMBINE_PAIRS_SEL, // Combiner selectivity in terms of records

	INPUT_COMPRESS_RATIO, // Input data compression ratio
	INTERM_COMPRESS_RATIO, // Map output data compression ratio
	OUT_COMPRESS_RATIO, // Output data compression ratio

	STARTUP_MEM, // Startup memory per task
	SETUP_MEM, // Setup memory per task
	MAP_MEM_PER_RECORD, // Memory per map's record
	REDUCE_MEM_PER_RECORD, // Memory per reducer's record
	CLEANUP_MEM; // Cleanup memory per task

	/**
	 * @return a description for the statistic
	 */
	public String getDescription() {

		switch (this) {
		case INPUT_PAIR_WIDTH:
			return "Average width of input key-value pairs";
		case REDUCE_PAIRS_PER_GROUP:
			return "Number of records per reducer's group";
		case MAP_SIZE_SEL:
			return "Map selectivity in terms of size";
		case MAP_PAIRS_SEL:
			return "Map selectivity in terms of records";
		case REDUCE_SIZE_SEL:
			return "Reducer selectivity in terms of size";
		case REDUCE_PAIRS_SEL:
			return "Reducer selectivity in terms of records";
		case COMBINE_SIZE_SEL:
			return "Combiner selectivity in terms of size";
		case COMBINE_PAIRS_SEL:
			return "Combiner selectivity in terms of records";
		case INPUT_COMPRESS_RATIO:
			return "Input data compression ratio";
		case INTERM_COMPRESS_RATIO:
			return "Map output data compression ratio";
		case OUT_COMPRESS_RATIO:
			return "Output data compression ratio";
		case STARTUP_MEM:
			return "Startup memory per task";
		case SETUP_MEM:
			return "Setup memory per task";
		case MAP_MEM_PER_RECORD:
			return "Memory per map's record";
		case REDUCE_MEM_PER_RECORD:
			return "Memory per reducer's record";
		case CLEANUP_MEM:
			return "Cleanup memory per task";
		default:
			return toString();
		}
	}
}
