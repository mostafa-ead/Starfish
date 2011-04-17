package edu.duke.starfish.profile.profileinfo;

/**
 * This is the base class for all information gathered in a Cluster.
 * 
 * @author hero
 * 
 */
public abstract class ClusterInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private long internalId; // The internal id for the object

	protected int hash; // The hash value for this object

	/**
	 * Default constructor
	 */
	public ClusterInfo() {
		this.internalId = -1;
		this.hash = -1;
	}

	/**
	 * @param internalId
	 *            an internal id
	 */
	public ClusterInfo(long internalId) {
		this.internalId = internalId;
		this.hash = -1;
	}

	/**
	 * Copy constructor
	 * 
	 * @param other
	 */
	public ClusterInfo(ClusterInfo other) {
		this.internalId = other.internalId;
		this.hash = other.hash;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return internalId the internal id
	 */
	public long getInternalId() {
		return internalId;
	}

	/**
	 * @param internalId
	 *            the internal id to set
	 */
	public void setInternalId(long internalId) {
		this.hash = -1;
		this.internalId = internalId;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == -1)
			hash = 31 + (int) (internalId ^ (internalId >>> 32));
		return hash;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ClusterInfo))
			return false;
		ClusterInfo other = (ClusterInfo) obj;
		if (internalId != other.internalId)
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public abstract String toString();

}
