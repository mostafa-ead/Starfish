package edu.duke.starfish.profile.profileinfo.execution;

import java.util.Date;

import edu.duke.starfish.profile.profileinfo.ClusterInfo;

/**
 * This is the base class for all information in the cluster related to any kind
 * of execution (e.g., jobs, tasks)
 * 
 * @author hero
 * 
 */
public abstract class ClusterExecutionInfo extends ClusterInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private String execId; // The execution id
	private Date startTime; // The start time of execution
	private Date endTime; // The end time of execution
	private MRExecutionStatus status; // The success/failure status
	private String errorMsg; // Possible error message

	/**
	 * Default Constructor
	 */
	public ClusterExecutionInfo() {
		super();
		this.execId = null;
		this.startTime = null;
		this.endTime = null;
		this.status = null;
		this.errorMsg = null;
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
	 */
	public ClusterExecutionInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg) {
		super(internalId);
		this.execId = execId;
		this.startTime = startTime;
		this.endTime = endTime;
		this.status = status;
		this.errorMsg = errorMsg;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public ClusterExecutionInfo(ClusterExecutionInfo other) {
		super(other);
		this.execId = other.execId;
		this.startTime = other.startTime == null ? null : new Date(
				other.startTime.getTime());
		this.endTime = other.endTime == null ? null : new Date(other.endTime
				.getTime());
		this.status = other.status;
		this.errorMsg = other.errorMsg;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the duration of execution in ms
	 */
	public long getDuration() {
		if (endTime != null && startTime != null)
			return endTime.getTime() - startTime.getTime();
		else
			return 0;
	}

	/**
	 * @return the execution id
	 */
	public String getExecId() {
		return execId;
	}

	/**
	 * @return the start time of the execution
	 */
	public Date getStartTime() {
		return startTime;
	}

	/**
	 * @return the end time of the execution
	 */
	public Date getEndTime() {
		return endTime;
	}

	/**
	 * @return the status of the execution
	 */
	public MRExecutionStatus getStatus() {
		return status;
	}

	/**
	 * @return the error message if the execution failed
	 */
	public String getErrorMsg() {
		return errorMsg;
	}

	/**
	 * @param execId
	 *            the execution id
	 */
	public void setExecId(String execId) {
		this.hash = -1;
		this.execId = execId;
	}

	/**
	 * @param startTime
	 *            the start time of execution
	 */
	public void setStartTime(Date startTime) {
		this.hash = -1;
		this.startTime = startTime;
	}

	/**
	 * @param endTime
	 *            the end time of execution
	 */
	public void setEndTime(Date endTime) {
		this.hash = -1;
		this.endTime = endTime;
	}

	/**
	 * @param status
	 *            the success/failure status
	 */
	public void setStatus(MRExecutionStatus status) {
		this.hash = -1;
		this.status = status;
	}

	/**
	 * @param errorMsg
	 *            the error message
	 */
	public void setErrorMsg(String errorMsg) {
		this.hash = -1;
		this.errorMsg = errorMsg;
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
			hash = 31 * hash + ((endTime == null) ? 0 : endTime.hashCode());
			hash = 37 * hash + ((execId == null) ? 0 : execId.hashCode());
			hash = 41 * hash + ((startTime == null) ? 0 : startTime.hashCode());
			hash = 43 * hash + ((status == null) ? 0 : status.hashCode());
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
		if (!(obj instanceof ClusterExecutionInfo))
			return false;
		ClusterExecutionInfo other = (ClusterExecutionInfo) obj;
		if (endTime == null) {
			if (other.endTime != null)
				return false;
		} else if (!endTime.equals(other.endTime))
			return false;
		if (execId == null) {
			if (other.execId != null)
				return false;
		} else if (!execId.equals(other.execId))
			return false;
		if (startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!startTime.equals(other.startTime))
			return false;
		if (status == null) {
			if (other.status != null)
				return false;
		} else if (!status.equals(other.status))
			return false;
		return true;
	}

}
