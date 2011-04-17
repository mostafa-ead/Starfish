package edu.duke.starfish.profile.profileinfo.setup;

import edu.duke.starfish.profile.profileinfo.ClusterInfo;

/**
 * This is the base class for all information in the cluster related to the
 * setup of the cluster (e.g. hosts, trackers)
 * 
 * @author hero
 * 
 */
public abstract class ClusterSetupInfo extends ClusterInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private String name; // The name of host, tracker etc.

	/**
	 * Default constructor
	 */
	public ClusterSetupInfo() {
		super();
		this.name = null;
	}

	/**
	 * @param internalId
	 *            an internal id
	 * @param name
	 *            the name of this entity
	 */
	public ClusterSetupInfo(long internalId, String name) {
		super(internalId);
		this.name = name;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public ClusterSetupInfo(ClusterSetupInfo other) {
		super(other);
		this.name = other.name;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the name the name of this entity
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.hash = -1;
		this.name = name;
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
			hash = 31 * hash + ((name == null) ? 0 : name.hashCode());
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
		if (!(obj instanceof ClusterSetupInfo))
			return false;
		ClusterSetupInfo other = (ClusterSetupInfo) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}
