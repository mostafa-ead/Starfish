package edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts;

import java.util.Date;

import edu.duke.starfish.profile.profileinfo.execution.ClusterExecutionInfo;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * Represents the information about a Map-Reduce task attempt
 * 
 * @author hero
 * 
 */
public abstract class MRTaskAttemptInfo extends ClusterExecutionInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private TaskTrackerInfo taskTracker; // The tracker running the attempt

	/**
	 * Default Constructor
	 */
	public MRTaskAttemptInfo() {
		super();
		this.taskTracker = null;
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
	public MRTaskAttemptInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			TaskTrackerInfo taskTracker) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.taskTracker = taskTracker;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MRTaskAttemptInfo(MRTaskAttemptInfo other) {
		super(other);
		this.taskTracker = other.taskTracker == null ? null
				: new TaskTrackerInfo(other.taskTracker);
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the task Tracker
	 */
	public TaskTrackerInfo getTaskTracker() {
		return taskTracker;
	}

	/**
	 * @param taskTracker
	 *            the task Tracker to set
	 */
	public void setTaskTracker(TaskTrackerInfo taskTracker) {
		this.hash = -1;
		this.taskTracker = taskTracker;
	}

	/**
	 * This method returns the truncated task attempt id. For example, if the
	 * task attempt id is: attempt_201102151322_0148_m_00001_0 this method will
	 * return m_00001_0
	 * 
	 * @return the truncated task id
	 */
	public String getTruncatedTaskId() {
		String taskId = getExecId();
		if (taskId == null)
			return "";

		// Check and get the task id of the
		int index = taskId.indexOf("_m_");
		if (index != -1)
			return taskId.substring(index + 1);

		index = taskId.indexOf("_r_");
		if (index != -1)
			return taskId.substring(index + 1);

		return taskId;
	}

	/**
	 * @return the task profile
	 */
	public abstract MRTaskProfile getProfile();

	/**
	 * @param profile
	 *            the profile to set
	 */
	public abstract void setProfile(MRTaskProfile profile);

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
		if (hash == -1) {
			hash = super.hashCode();
			hash = 31 * hash
					+ ((taskTracker == null) ? 0 : taskTracker.hashCode());
		}
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
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRTaskAttemptInfo))
			return false;
		MRTaskAttemptInfo other = (MRTaskAttemptInfo) obj;
		if (taskTracker == null) {
			if (other.taskTracker != null)
				return false;
		} else if (!taskTracker.equals(other.taskTracker))
			return false;
		return true;
	}

}
