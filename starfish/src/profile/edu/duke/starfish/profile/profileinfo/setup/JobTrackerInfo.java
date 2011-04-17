package edu.duke.starfish.profile.profileinfo.setup;

/**
 * Represents the information about a Job Tracker
 * 
 * @author hero
 * 
 */
public class JobTrackerInfo extends TrackerInfo {

	/**
	 * Default Constructor
	 */
	public JobTrackerInfo() {
		super();
	}

	/**
	 * @param internalId
	 *            an internal id
	 * @param name
	 *            the name of the job tracker
	 * @param hostName
	 *            the host machine on which the tracker is running
	 * @param port
	 *            the port of the tracker
	 */
	public JobTrackerInfo(long internalId, String name, String hostName,
			int port) {
		super(internalId, name, hostName, port);
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public JobTrackerInfo(JobTrackerInfo other) {
		super(other);
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
		return super.hashCode();
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
		if (!(obj instanceof JobTrackerInfo))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "JobTrackerInfo [Name=" + getName() + ", Host=" + getHostName()
				+ ", Port=" + getPort() + "]";
	}

}
