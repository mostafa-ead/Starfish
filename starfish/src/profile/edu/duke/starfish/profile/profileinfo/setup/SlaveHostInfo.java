package edu.duke.starfish.profile.profileinfo.setup;

/**
 * Represents the information about a Slave Host.
 * 
 * @author hero
 * 
 */
public class SlaveHostInfo extends HostInfo {

	private TaskTrackerInfo taskTracker; // A task tracker

	/**
	 * Default Constructor
	 */
	public SlaveHostInfo() {
		super();
		this.taskTracker = null;
	}

	/**
	 * @param internalId
	 *            an internal id
	 * @param name
	 *            the name of the slave host
	 * @param ipAddress
	 *            the IP address of the host
	 * @param rackName
	 *            the rack on which the host is located
	 */
	public SlaveHostInfo(long internalId, String name, String ipAddress,
			String rackName) {
		super(internalId, name, ipAddress, rackName);
		this.taskTracker = null;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public SlaveHostInfo(SlaveHostInfo other) {
		super(other);
		this.taskTracker = null;
		if (other.taskTracker != null)
			setTaskTracker(new TaskTrackerInfo(other.taskTracker));
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the taskTracker
	 */
	public TaskTrackerInfo getTaskTracker() {
		return taskTracker;
	}

	/**
	 * @param taskTracker
	 *            the taskTracker to set
	 */
	public void setTaskTracker(TaskTrackerInfo taskTracker) {
		this.taskTracker = taskTracker;
		taskTracker.setHostName(getName());
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
		int result = super.hashCode();
		result = 31 * result
				+ ((taskTracker == null) ? 0 : taskTracker.hashCode());
		return result;
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
		if (!(obj instanceof SlaveHostInfo))
			return false;
		SlaveHostInfo other = (SlaveHostInfo) obj;
		if (taskTracker == null) {
			if (other.taskTracker != null)
				return false;
		} else if (!taskTracker.equals(other.taskTracker))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SlaveHostInfo [Name=" + getName() + ", IPAddress="
				+ getIpAddress() + ", Rack=" + getRackName() + "]";
	}

}
