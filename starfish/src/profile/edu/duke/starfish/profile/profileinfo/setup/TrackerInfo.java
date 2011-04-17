package edu.duke.starfish.profile.profileinfo.setup;

/**
 * Represents the information about a Tracker (Job or Task Tracker)
 * 
 * @author hero
 * 
 */
public abstract class TrackerInfo extends ClusterSetupInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private String hostName; // The host name this tracker is on
	private int port; // The HTTP port the tracker is listening to

	/**
	 * Default Constructor
	 */
	public TrackerInfo() {
		super();
		this.hostName = null;
		this.port = 0;
	}

	/**
	 * @param internalId
	 *            an internal id
	 * @param name
	 *            the name of the tracker
	 * @param hostName
	 *            the host name this tracker is on
	 * @param port
	 *            the HTTP port the tracker is listening to
	 */
	public TrackerInfo(long internalId, String name, String hostName, int port) {
		super(internalId, name);
		this.hostName = hostName;
		this.port = port;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public TrackerInfo(TrackerInfo other) {
		super(other);
		this.hostName = other.hostName;
		this.port = other.port;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the HTTP port the tracker is listening to
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @return the hostName
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * @param port
	 *            the port to set
	 */
	public void setPort(int port) {
		this.hash = -1;
		this.port = port;
	}

	/**
	 * @param hostName
	 *            the hostName to set
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
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
		result = 31 * result + ((hostName == null) ? 0 : hostName.hashCode());
		result = 37 * result + port;
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
		if (!(obj instanceof TrackerInfo))
			return false;
		TrackerInfo other = (TrackerInfo) obj;
		if (hostName == null) {
			if (other.hostName != null)
				return false;
		} else if (!hostName.equals(other.hostName))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

}
