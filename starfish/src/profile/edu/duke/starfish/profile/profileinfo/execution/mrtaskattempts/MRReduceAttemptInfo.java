package edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts;

import java.util.Date;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * Represents an execution attempt for a Reduce Task in a Map-Reduce job
 * 
 * @author hero
 * 
 */
public class MRReduceAttemptInfo extends MRTaskAttemptInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private Date shuffleEndTime; // The end time of the shuffler phase
	private Date sortEndTime; // The end time of the sort phase
	private MRReduceProfile profile; // The Reduce profile

	/**
	 * Default Constructor
	 */
	public MRReduceAttemptInfo() {
		super();
		this.shuffleEndTime = null;
		this.sortEndTime = null;
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
	 * @param shuffleEndTime
	 * @param sortEndTime
	 */
	public MRReduceAttemptInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			TaskTrackerInfo taskTracker, Date shuffleEndTime, Date sortEndTime) {
		super(internalId, execId, startTime, endTime, status, errorMsg,
				taskTracker);
		this.shuffleEndTime = shuffleEndTime;
		this.sortEndTime = sortEndTime;
		this.profile = null;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MRReduceAttemptInfo(MRReduceAttemptInfo other) {
		super(other);
		this.shuffleEndTime = this.shuffleEndTime == null ? null : new Date(
				other.shuffleEndTime.getTime());
		this.sortEndTime = this.sortEndTime == null ? null : new Date(
				other.sortEndTime.getTime());
		this.profile = other.profile == null ? null : new MRReduceProfile(
				other.profile);
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return return the duration of the shuffle phase in ms
	 */
	public long getShuffleDuration() {
		if (shuffleEndTime != null && getStartTime() != null)
			return shuffleEndTime.getTime() - getStartTime().getTime();
		else
			return 0;
	}

	/**
	 * @return return the duration of the sort phase in ms
	 */
	public long getSortDuration() {
		if (sortEndTime != null && shuffleEndTime != null)
			return sortEndTime.getTime() - shuffleEndTime.getTime();
		else
			return 0;
	}

	/**
	 * @return return the duration of the reduce phase in ms
	 */
	public long getReduceDuration() {
		if (getEndTime() != null && sortEndTime != null)
			return getEndTime().getTime() - sortEndTime.getTime();
		else
			return 0;
	}

	/**
	 * @return the end time of the shuffler phase
	 */
	public Date getShuffleEndTime() {
		return shuffleEndTime;
	}

	/**
	 * @return the end time of the sort phase
	 */
	public Date getSortEndTime() {
		return sortEndTime;
	}

	/**
	 * @param shuffleEndTime
	 *            the shuffleEndTime to set
	 */
	public void setShuffleEndTime(Date shuffleEndTime) {
		this.hash = -1;
		this.shuffleEndTime = shuffleEndTime;
	}

	/**
	 * @param sortEndTime
	 *            the sortEndTime to set
	 */
	public void setSortEndTime(Date sortEndTime) {
		this.hash = -1;
		this.sortEndTime = sortEndTime;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the reduce profile
	 */
	@Override
	public MRReduceProfile getProfile() {
		if (profile == null)
			profile = new MRReduceProfile(this.getExecId());
		return profile;
	}

	@Override
	public void setProfile(MRTaskProfile profile) {
		this.profile = (MRReduceProfile) profile;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == -1) {
			hash = super.hashCode();
			hash = 31
					* hash
					+ ((shuffleEndTime == null) ? 0 : shuffleEndTime.hashCode());
			hash = 37 * hash
					+ ((sortEndTime == null) ? 0 : sortEndTime.hashCode());
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
		if (!(obj instanceof MRReduceAttemptInfo))
			return false;
		MRReduceAttemptInfo other = (MRReduceAttemptInfo) obj;
		if (shuffleEndTime == null) {
			if (other.shuffleEndTime != null)
				return false;
		} else if (!shuffleEndTime.equals(other.shuffleEndTime))
			return false;
		if (sortEndTime == null) {
			if (other.sortEndTime != null)
				return false;
		} else if (!sortEndTime.equals(other.sortEndTime))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MRReduceAttemptInfo [ID="
				+ getExecId()
				+ ", StartTime="
				+ getStartTime()
				+ ", EndTime="
				+ getEndTime()
				+ ", TaskTracker="
				+ ((getTaskTracker() == null) ? "null" : getTaskTracker()
						.getName()) + ", Status=" + getStatus()
				+ ", ShuffleEndTime=" + shuffleEndTime + ", SortEndTime="
				+ sortEndTime + "]";
	}

}
