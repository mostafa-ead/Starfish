package edu.duke.starfish.profile.profileinfo.setup;

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

	/**
	 * Default Constructor
	 */
	public TaskTrackerInfo() {
		super();
		this.numMapSlots = 0;
		this.numReduceSlots = 0;
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
	 */
	public TaskTrackerInfo(long internalId, String name, String hostName,
			int port, int numMapSlots, int numReduceSlots) {
		super(internalId, name, hostName, port);
		this.numMapSlots = numMapSlots;
		this.numReduceSlots = numReduceSlots;
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
