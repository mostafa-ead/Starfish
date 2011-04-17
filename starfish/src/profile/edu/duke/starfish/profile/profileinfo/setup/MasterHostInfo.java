package edu.duke.starfish.profile.profileinfo.setup;

/**
 * Represents the information about a Master Host.
 * 
 * @author hero
 * 
 */
public class MasterHostInfo extends HostInfo {

	private JobTrackerInfo jobTracker; // A job tracker

	/**
	 * Default Constructor
	 */
	public MasterHostInfo() {
		super();
		this.jobTracker = null;
	}

	/**
	 * @param internalId
	 *            an internal id
	 * @param name
	 *            the name of the Master
	 * @param ipAddress
	 *            the IP address of the host
	 * @param rackName
	 *            the rack on which the host is located
	 */
	public MasterHostInfo(long internalId, String name, String ipAddress,
			String rackName) {
		super(internalId, name, ipAddress, rackName);
		this.jobTracker = null;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MasterHostInfo(MasterHostInfo other) {
		super(other);
		this.jobTracker = null;
		if (other.jobTracker != null)
			setJobTracker(new JobTrackerInfo(other.jobTracker));
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the jobTracker
	 */
	public JobTrackerInfo getJobTracker() {
		return jobTracker;
	}

	/**
	 * @param jobTracker
	 *            the jobTracker to set
	 */
	public void setJobTracker(JobTrackerInfo jobTracker) {
		this.jobTracker = jobTracker;
		jobTracker.setHostName(getName());
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
				+ ((jobTracker == null) ? 0 : jobTracker.hashCode());
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
		if (!(obj instanceof MasterHostInfo))
			return false;
		MasterHostInfo other = (MasterHostInfo) obj;
		if (jobTracker == null) {
			if (other.jobTracker != null)
				return false;
		} else if (!jobTracker.equals(other.jobTracker))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MasterHostInfo [Name=" + getName() + ", IPAddress="
				+ getIpAddress() + ", Rack=" + getRackName() + "]";
	}

}
