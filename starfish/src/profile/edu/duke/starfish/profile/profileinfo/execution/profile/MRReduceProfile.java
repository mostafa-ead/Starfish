package edu.duke.starfish.profile.profileinfo.execution.profile;

import java.io.PrintStream;

/**
 * A task profile specific to reduce tasks.
 * 
 * @author hero
 */
public class MRReduceProfile extends MRTaskProfile {

	/**
	 * Constructor
	 * 
	 * @param taskId
	 *            the reduce task id
	 */
	public MRReduceProfile(String taskId) {
		super(taskId);
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 *            the reduce profile to copy from
	 */
	public MRReduceProfile(MRReduceProfile other) {
		super(other);
	}

	/**
	 * Prints out all the execution profiling information
	 * 
	 * @param out
	 *            The print stream to print to
	 */
	@Override
	public void printProfile(PrintStream out) {
		out.println("REDUCE PROFILE:\n\t" + getTaskId());
		super.printProfile(out);
	}

}
