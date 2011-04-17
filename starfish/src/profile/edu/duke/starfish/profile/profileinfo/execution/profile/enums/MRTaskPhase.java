package edu.duke.starfish.profile.profileinfo.execution.profile.enums;

/**
 * Enumerates the different phases that MR tasks go through (eg. map, spill,
 * merge)
 * 
 * @author hero
 */
public enum MRTaskPhase {

	SHUFFLE, // Shuffle phase time in the reduce task
	SORT, // Merge phase time in the reduce task
	SETUP, // Setup phase time in the task
	READ, // Read phase time in the map task
	MAP, // Map phase time in the map task
	REDUCE, // Reduce phase time in the reduce task
	COLLECT, // Collect phase time in the map task
	WRITE, // write phase time in the reduce task
	SPILL, // Spill phase time in the map task
	MERGE, // Merge phase time in the map task
	CLEANUP; // Cleanup phase time in the task

	/**
	 * @return a name for the task phase
	 */
	public String getName() {

		switch (this) {
		case SHUFFLE:
			return "SHUFFLE";
		case SORT:
			return "MERGE";
		case SETUP:
			return "SETUP";
		case READ:
			return "READ";
		case MAP:
			return "MAP";
		case REDUCE:
			return "REDUCE";
		case COLLECT:
			return "COLLECT";
		case WRITE:
			return "WRITE";
		case CLEANUP:
			return "CLEANUP";
		case SPILL:
			return "SPILL";
		case MERGE:
			return "MERGE";
		default:
			return toString();
		}
	}

	
	/**
	 * @return a description for the task phase
	 */
	public String getDescription() {

		switch (this) {
		case SHUFFLE:
			return "Shuffle phase: Transferring map output data to reduce tasks, with decompression if needed";
		case SORT:
			return "Merge phase: Merging sorted map outputs";
		case SETUP:
			return "Setup phase: Executing the user-defined setup function";
		case READ:
			return "Read phase: Reading the job input data from the distributed filesystem";
		case MAP:
			return "Map phase: Executing the user-defined map function";
		case REDUCE:
			return "Reduce phase: Executing the user-defined reduce function";
		case COLLECT:
			return "Collect phase: Partitioning and serializing the map output data to buffer before spilling";
		case WRITE:
			return "Write phase: Writing the job output data to the distributed filesystem";
		case CLEANUP:
			return "Cleanup phase: Executing the user-defined task cleanup function";
		case SPILL:
			return "Spill phase: Sorting, combining, compressing, and writing map output data to local disk";
		case MERGE:
			return "Merge phase: Merging sorted spill files";
		default:
			return toString();
		}
	}
}
