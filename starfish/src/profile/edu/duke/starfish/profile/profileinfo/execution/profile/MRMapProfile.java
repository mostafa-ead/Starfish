package edu.duke.starfish.profile.profileinfo.execution.profile;

import java.io.PrintStream;

/**
 * A task profile specific to map tasks. In addition to all the profile
 * information from the task's profile, it also contains the input location.
 * 
 * @author hero
 */
public class MRMapProfile extends MRTaskProfile {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private int inputIndex;

	/**
	 * Constructor
	 * 
	 * @param taskId
	 *            the task id of the map task
	 */
	public MRMapProfile(String taskId) {
		super(taskId);
		this.inputIndex = 0;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 *            a map profile to copy from
	 */
	public MRMapProfile(MRMapProfile other) {
		super(other);
		this.inputIndex = other.inputIndex;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the input index in the job input list
	 */
	public int getInputIndex() {
		return inputIndex;
	}

	/**
	 * @param inputIndex
	 *            the input index to set
	 */
	public void setInputIndex(int inputIndex) {
		this.inputIndex = inputIndex;
	}

	/**
	 * Prints out all the execution profiling information
	 * 
	 * @param out
	 *            The print stream to print to
	 */
	@Override
	public void printProfile(PrintStream out) {
		out.println("MAP PROFILE:\n\t" + getTaskId());
		out.println("Input Path Index:\n\t" + inputIndex);

		super.printProfile(out);
	}

}
