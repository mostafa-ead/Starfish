package edu.duke.starfish.profile.profileinfo.execution;

/**
 * Enumerates the possible execution status values (success, failed, killed,
 * prep, running).
 * 
 * @author hero
 * 
 */
public enum MRExecutionStatus {
	SUCCESS, // The execution was successful
	FAILED, // The execution failed
	KILLED, // The execution was killed
	PREP, // The execution is in the preparation state
	RUNNING; // The execution is in a running state

	/**
	 * @return a human-readable description
	 */
	public String getDescription() {
		switch (this) {
		case SUCCESS:
			return "Success";
		case FAILED:
			return "Failed";
		case KILLED:
			return "Killed";
		case PREP:
			return "In Prep";
		case RUNNING:
			return "Running";
		default:
			return toString();
		}
	}
}
