package edu.duke.starfish.profile.profileinfo.execution.mrtasks;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;

/**
 * Represents the information about a Setup Task executed in a Map-Reduce job in
 * the cluster
 * 
 * @author hero
 * 
 */
public class MRSetupInfo extends MRTaskInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private List<MRSetupAttemptInfo> attempts; // The task attempts

	/**
	 * Default Constructor
	 */
	public MRSetupInfo() {
		super();
		this.attempts = new ArrayList<MRSetupAttemptInfo>(1);
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
	public MRSetupInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.attempts = new ArrayList<MRSetupAttemptInfo>(1);
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MRSetupInfo(MRSetupInfo other) {
		super(other);
		this.attempts = new ArrayList<MRSetupAttemptInfo>(other.attempts.size());
		for (MRSetupAttemptInfo attempt : other.attempts)
			addAttempt(new MRSetupAttemptInfo(attempt));
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the attempts
	 */
	@Override
	public List<MRSetupAttemptInfo> getAttempts() {
		return attempts;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	@Override
	public MRSetupAttemptInfo getSuccessfulAttempt() {
		for (MRSetupAttemptInfo attempt : attempts)
			if (attempt.getStatus() == MRExecutionStatus.SUCCESS)
				return attempt;

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == -1) {
			hash = super.hashCode();
			hash = 31 * hash + ((attempts == null) ? 0 : attempts.hashCode());
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
		if (!(obj instanceof MRSetupInfo))
			return false;
		MRSetupInfo other = (MRSetupInfo) obj;
		if (attempts == null) {
			if (other.attempts != null)
				return false;
		} else if (!attempts.equals(other.attempts))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MRSetupInfo [TaskId=" + getExecId() + ", StartTime="
				+ getStartTime() + ", EndTime=" + getEndTime() + ", Status="
				+ getStatus() + ", NumAttempts=" + attempts.size() + "]";
	}

	/* ***************************************************************
	 * PUBLIC METHOD
	 * ***************************************************************
	 */

	/**
	 * @param attempt
	 *            the attempt to add
	 */
	public void addAttempt(MRSetupAttemptInfo attempt) {
		hash = -1;
		attempts.add(attempt);
	}

}
