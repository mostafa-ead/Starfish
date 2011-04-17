package edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts;

import java.util.Date;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * Represents an execution attempt for a Setup Task in a Map-Reduce job
 * 
 * @author hero
 * 
 */
public class MRSetupAttemptInfo extends MRTaskAttemptInfo {

	/**
	 * Default Constructor
	 */
	public MRSetupAttemptInfo() {
		super();
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
	 */
	public MRSetupAttemptInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			TaskTrackerInfo taskTracker) {
		super(internalId, execId, startTime, endTime, status, errorMsg,
				taskTracker);
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MRSetupAttemptInfo(MRSetupAttemptInfo other) {
		super(other);
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	@Override
	public MRTaskProfile getProfile() {
		return new MRMapProfile(this.getExecId());
	}

	@Override
	public void setProfile(MRTaskProfile profile) {
	}

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
		if (!(obj instanceof MRSetupAttemptInfo))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MRSetupAttemptInfo [ID="
				+ getExecId()
				+ ", StartTime="
				+ getStartTime()
				+ ", EndTime="
				+ getEndTime()
				+ ", TaskTracker="
				+ ((getTaskTracker() == null) ? "null" : getTaskTracker()
						.getName()) + ", Status=" + getStatus() + "]";
	}

}
