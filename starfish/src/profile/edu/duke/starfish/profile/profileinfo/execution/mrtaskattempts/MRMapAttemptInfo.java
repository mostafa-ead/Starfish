package edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts;

import java.util.Date;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * Represents an execution attempt for a Map Task in a Map-Reduce job
 * 
 * @author hero
 * 
 */
public class MRMapAttemptInfo extends MRTaskAttemptInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private DataLocality dataLocality; // The locality of the data access
	private MRMapProfile profile; // The Map profile

	/**
	 * Default Constructor
	 */
	public MRMapAttemptInfo() {
		super();
		this.dataLocality = null;
		this.profile = null;
	}

	/**
	 * @param internalId
	 *            an internal id
	 * @param execId
	 *            the execution id
	 * @param startTime
	 *            the start time of the execution
	 * @param endTime
	 *            the end time of the execution
	 * @param status
	 *            the execution status
	 * @param errorMsg
	 *            the error message of a failed execution
	 * @param taskTracker
	 *            the task tracker running the attempt
	 * @param dataLocality
	 */
	public MRMapAttemptInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			TaskTrackerInfo taskTracker, DataLocality dataLocality) {
		super(internalId, execId, startTime, endTime, status, errorMsg,
				taskTracker);
		this.dataLocality = dataLocality;
		this.profile = null;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MRMapAttemptInfo(MRMapAttemptInfo other) {
		super(other);
		this.dataLocality = other.dataLocality;
		this.profile = other.profile == null ? null : new MRMapProfile(
				other.profile);
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the data Locality
	 */
	public DataLocality getDataLocality() {
		return dataLocality;
	}

	/**
	 * @param dataLocality
	 *            the data Locality to set
	 */
	public void setDataLocality(DataLocality dataLocality) {
		this.hash = -1;
		this.dataLocality = dataLocality;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the map profile
	 */
	@Override
	public MRMapProfile getProfile() {
		if (profile == null)
			profile = new MRMapProfile(this.getExecId());
		return profile;
	}

	@Override
	public void setProfile(MRTaskProfile profile) {
		this.profile = (MRMapProfile) profile;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == -1) {
			hash = super.hashCode();
			hash = 31 * hash
					+ ((dataLocality == null) ? 0 : dataLocality.hashCode());
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
		if (!(obj instanceof MRMapAttemptInfo))
			return false;
		MRMapAttemptInfo other = (MRMapAttemptInfo) obj;
		if (dataLocality == null) {
			if (other.dataLocality != null)
				return false;
		} else if (!dataLocality.equals(other.dataLocality))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MRMapAttemptInfo [ID="
				+ getExecId()
				+ ", StartTime="
				+ getStartTime()
				+ ", EndTime="
				+ getEndTime()
				+ ", TaskTracker="
				+ ((getTaskTracker() == null) ? "null" : getTaskTracker()
						.getName()) + ", Status=" + getStatus() + ", Locality="
				+ dataLocality + "]";
	}

}
