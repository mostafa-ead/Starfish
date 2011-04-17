package edu.duke.starfish.profile.profileinfo.setup;

/**
 * Represents the information about a host machine
 * 
 * @author hero
 * 
 */
public abstract class HostInfo extends ClusterSetupInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private String ipAddress; // The IP address of the host
	private String rackName; // The rack on which the host is located

	/**
	 * Default Constructor
	 */
	public HostInfo() {
		super();
		this.ipAddress = null;
		this.rackName = null;
	}

	/**
	 * Constructor
	 * 
	 * Note: the new host is added to this rack
	 * 
	 * @param internalId
	 *            an internal id
	 * @param name
	 *            the name of this host machine
	 * @param ipAddress
	 *            the IP address of the host
	 * @param rackName
	 *            the rack on which the host is located
	 */
	public HostInfo(long internalId, String name, String ipAddress,
			String rackName) {
		super(internalId, name);
		this.ipAddress = ipAddress;
		this.rackName = rackName;
	}

	/**
	 * Copy constructor
	 * 
	 * @param other
	 */
	public HostInfo(HostInfo other) {
		super(other);
		this.ipAddress = other.ipAddress;
		this.rackName = other.rackName;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the ipAddress the IP address of the host
	 */
	public String getIpAddress() {
		return ipAddress;
	}

	/**
	 * @return the rack on which this host is located on
	 */
	public String getRackName() {
		return rackName;
	}

	/**
	 * @param ipAddress
	 *            the ip address to set
	 */
	public void setIpAddress(String ipAddress) {
		this.hash = -1;
		this.ipAddress = ipAddress;
	}

	/**
	 * Set the rack. This method also ensures that his host is in the rack.
	 * 
	 * @param rackName
	 *            the rack to set
	 */
	public void setRackName(String rackName) {
		this.hash = -1;
		this.rackName = rackName;
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
			hash = 31 * hash + ((ipAddress == null) ? 0 : ipAddress.hashCode());
			hash = 37 * hash + ((rackName == null) ? 0 : rackName.hashCode());
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
		if (!(obj instanceof HostInfo))
			return false;
		HostInfo other = (HostInfo) obj;
		if (ipAddress == null) {
			if (other.ipAddress != null)
				return false;
		} else if (!ipAddress.equals(other.ipAddress))
			return false;

		// NOTE: the rack object contains hosts => don't use the rack object
		if (rackName == null) {
			if (other.rackName != null)
				return false;
		} else if (!rackName.equals(other.rackName))
			return false;

		return true;
	}

}
