package edu.duke.starfish.profile.profileinfo.execution;

/**
 * Enumerates the possible locality values of the data access (data local, rack
 * local, non local)
 * 
 * @author hero
 * 
 */
public enum DataLocality {
	DATA_LOCAL, // The data accessed are local
	RACK_LOCAL, // The data accessed are in the same rack
	NON_LOCAL; // The data accessed are not local

	/**
	 * @return a human-readable description
	 */
	public String getDescription() {
		switch (this) {
		case DATA_LOCAL:
			return "Data Local";
		case RACK_LOCAL:
			return "Rack Local";
		case NON_LOCAL:
			return "Non Local";
		default:
			return toString();
		}
	}
}
