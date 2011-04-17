package edu.duke.starfish.profile.profileinfo.setup;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A class to represent a rack of host machines.
 * 
 * @author hero
 * 
 */
public class RackInfo extends ClusterSetupInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private MasterHostInfo masterHost; // A master host on the rack
	private Map<String, SlaveHostInfo> slaveHosts; // The host machines

	/**
	 * Default Constructor
	 */
	public RackInfo() {
		super();
		this.masterHost = null;
		this.slaveHosts = new HashMap<String, SlaveHostInfo>();
	}

	/**
	 * @param internalId
	 *            an internal id
	 * @param name
	 *            the name of the rack
	 */
	public RackInfo(long internalId, String name) {
		super(internalId, name);
		this.masterHost = null;
		this.slaveHosts = new HashMap<String, SlaveHostInfo>();
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public RackInfo(RackInfo other) {
		super(other);
		this.masterHost = null;
		if (other.masterHost != null)
			setMasterHost(new MasterHostInfo(other.masterHost));
		this.slaveHosts = new HashMap<String, SlaveHostInfo>(other.slaveHosts
				.size());
		for (SlaveHostInfo slave : other.slaveHosts.values())
			addSlaveHost(new SlaveHostInfo(slave));
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the master host on this rack
	 */
	public MasterHostInfo getMasterHost() {
		return masterHost;
	}

	/**
	 * @param slaveName
	 *            the slave's name
	 * @return the slave host
	 */
	public SlaveHostInfo getSlaveHost(String slaveName) {
		return slaveHosts.get(slaveName);
	}

	/**
	 * @return the slave hosts on this rack
	 */
	public Collection<SlaveHostInfo> getSlaveHosts() {
		return slaveHosts.values();
	}

	/**
	 * @param host
	 *            the master host to set to this rack
	 */
	public void setMasterHost(MasterHostInfo host) {
		this.masterHost = host;
		host.setRackName(getName());
		hash = -1;
	}

	/**
	 * Adds a slave host to this rack. This method also ensures that the host
	 * points to this rack.
	 * 
	 * @param host
	 *            the slave host to add to this rack
	 */
	public void addSlaveHost(SlaveHostInfo host) {
		this.slaveHosts.put(host.getName(), host);
		host.setRackName(getName());
		hash = -1;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * *************************************************************
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == -1) {
			hash = super.hashCode();
			hash = 31 * hash
					+ ((masterHost == null) ? 0 : masterHost.hashCode());
			hash = 37 * hash
					+ ((slaveHosts == null) ? 0 : slaveHosts.hashCode());
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
		if (!(obj instanceof RackInfo))
			return false;
		RackInfo other = (RackInfo) obj;
		if (masterHost == null) {
			if (other.masterHost != null)
				return false;
		} else if (!masterHost.equals(other.masterHost))
			return false;
		if (slaveHosts == null) {
			if (other.slaveHosts != null)
				return false;
		} else if (!slaveHosts.equals(other.slaveHosts))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RackInfo [Name=" + getName() + ", MasterHost="
				+ ((masterHost == null) ? "NONE" : masterHost.getName())
				+ ", Hosts=" + slaveHosts.keySet() + "]";
	}

}
