package edu.duke.starfish.profile.profileinfo.setup;

import edu.duke.starfish.profile.profileinfo.utils.Constants;

/**
 * Represents the information about a Task Tracker
 * 
 * @author hero
 * 
 */
public class TaskTrackerInfo extends TrackerInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private int numMapSlots; // The number of map slots
	private int numReduceSlots; // The number of reduce slots
	private long maxSlotMemory; // The max memory per slot (in bytes)

	/**
	 * Default Constructor
	 */
	public TaskTrackerInfo() {
		super();
		// Use Hadoop defaults to initialize
		this.numMapSlots = Constants.DEF_MAX_MAP_TASKS;
		this.numReduceSlots = Constants.DEF_MAX_RED_TASKS;
		this.maxSlotMemory = Constants.DEF_TASK_MEM;
	}

	/**
	 * @param internalId
	 *            an internal id
	 * @param name
	 *            the name of the task tracker
	 * @param hostName
	 *            the host machine on which the tracker is running
	 * @param port
	 *            the port of the tracker
	 * @param numMapSlots
	 *            the number of map slots
	 * @param numReduceSlots
	 *            the number of reduce slots
	 * @param maxSlotMemory
	 *            the max memory per slot in bytes
	 */
	public TaskTrackerInfo(long internalId, String name, String hostName,
			int port, int numMapSlots, int numReduceSlots, long maxSlotMemory) {
		super(internalId, name, hostName, port);
		this.numMapSlots = numMapSlots;
		this.numReduceSlots = numReduceSlots;
		this.maxSlotMemory = maxSlotMemory;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public TaskTrackerInfo(TaskTrackerInfo other) {
		super(other);
		this.numMapSlots = other.numMapSlots;
		this.numReduceSlots = other.numReduceSlots;
		this.maxSlotMemory = other.maxSlotMemory;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the number of map slots
	 */
	public int getNumMapSlots() {
		return numMapSlots;
	}

	/**
	 * @return the number of reduce slots
	 */
	public int getNumReduceSlots() {
		return numReduceSlots;
	}

	/**
	 * @return the max memory per slot in bytes
	 */
	public long getMaxTaskMemory() {
		return maxSlotMemory;
	}

	/**
	 * @param numMapSlots
	 *            the numMapSlots to set
	 */
	public void setNumMapSlots(int numMapSlots) {
		this.hash = -1;
		this.numMapSlots = numMapSlots;
	}

	/**
	 * @param numReduceSlots
	 *            the numReduceSlots to set
	 */
	public void setNumReduceSlots(int numReduceSlots) {
		this.hash = -1;
		this.numReduceSlots = numReduceSlots;
	}

	/**
	 * @param maxSlotMemory
	 *            the max memory per slot (in bytes) to set
	 */
	public void setMaxSlotMemory(long maxSlotMemory) {
		this.hash = -1;
		this.maxSlotMemory = maxSlotMemory;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == -1) {
			hash = super.hashCode();
			hash = 31 * hash + numMapSlots;
			hash = 37 * hash + numReduceSlots;
			hash = 41 * hash + (int) (maxSlotMemory ^ (maxSlotMemory >>> 32));
		}
		return hash;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof TaskTrackerInfo))
			return false;
		TaskTrackerInfo other = (TaskTrackerInfo) obj;
		if (maxSlotMemory != other.maxSlotMemory)
			return false;
		if (numMapSlots != other.numMapSlots)
			return false;
		if (numReduceSlots != other.numReduceSlots)
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TaskTrackerInfo [Name=" + getName() + ", Host=" + getHostName()
				+ ", Port=" + getPort() + "]";
	}

}
