package edu.duke.starfish.profile.profileinfo.execution.jobs;

import java.util.Date;

import edu.duke.starfish.profile.profileinfo.execution.ClusterExecutionInfo;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;

/**
 * Represents the information about a job executed in the cluster (e.g., MR job,
 * Pig job)
 * 
 * @author hero
 * 
 */
public abstract class JobInfo extends ClusterExecutionInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private String name; // A user-friendly name of the job
	private String user; // The name of the user who submitted the job

	/**
	 * Default Constructor
	 */
	public JobInfo() {
		super();
		this.name = null;
		this.user = null;
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
	 * @param name
	 *            the name of the job
	 * @param user
	 *            the user who submitted job
	 */
	public JobInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			String name, String user) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.name = name;
		this.user = user;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public JobInfo(JobInfo other) {
		super(other);
		this.name = other.name;
		this.user = other.user;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the name of the job
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the user who submitted the job
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.hash = -1;
		this.name = name;
	}

	/**
	 * @param user
	 *            the user to set
	 */
	public void setUser(String user) {
		this.hash = -1;
		this.user = user;
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
			hash = 37 * hash + ((user == null) ? 0 : user.hashCode());
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
		if (!(obj instanceof JobInfo))
			return false;
		JobInfo other = (JobInfo) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
	}

}
