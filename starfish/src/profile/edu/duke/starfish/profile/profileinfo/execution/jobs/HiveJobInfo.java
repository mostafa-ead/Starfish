package edu.duke.starfish.profile.profileinfo.execution.jobs;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;

/**
 * Represents information about a Hive Job, containing Map-Reduce jobs.
 * 
 * @author hero
 * 
 */
public class HiveJobInfo extends JobInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private List<MRJobInfo> mrJobs; // The Map-Reduce Jobs

	/**
	 * Default Constructor
	 */
	public HiveJobInfo() {
		super();
		this.mrJobs = new ArrayList<MRJobInfo>();
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
	 * @param mrJobs
	 *            the Map-Reduce jobs comprising this job
	 */
	public HiveJobInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			String name, String user, List<MRJobInfo> mrJobs) {
		super(internalId, execId, startTime, endTime, status, errorMsg, name,
				user);
		this.mrJobs = mrJobs;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public HiveJobInfo(HiveJobInfo other) {
		super(other);
		this.mrJobs = new ArrayList<MRJobInfo>(other.mrJobs.size());
		for (MRJobInfo mrJob : other.mrJobs)
			addMRJobInfo(new MRJobInfo(mrJob));
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the Map-Reduce jobs comprising the Hive job
	 */
	public List<MRJobInfo> getMrJobs() {
		return mrJobs;
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
			hash = 31 * hash + ((mrJobs == null) ? 0 : mrJobs.hashCode());
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
		if (!(obj instanceof HiveJobInfo))
			return false;
		HiveJobInfo other = (HiveJobInfo) obj;
		if (mrJobs == null) {
			if (other.mrJobs != null)
				return false;
		} else if (!mrJobs.equals(other.mrJobs))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "HiveJobInfo [ID=" + getExecId() + ", Name=" + getName()
				+ ", User=" + getUser() + ", StartTime=" + getStartTime()
				+ ", EndTime=" + getEndTime() + ", Status=" + getStatus()
				+ ", NumMRJobs=" + ((mrJobs == null) ? 0 : mrJobs.size()) + "]";
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @param mrJobInfo
	 *            the MR Job info to add
	 */
	public void addMRJobInfo(MRJobInfo mrJobInfo) {
		if (mrJobs == null) {
			mrJobs = new ArrayList<MRJobInfo>();
		}

		hash = -1;
		mrJobs.add(mrJobInfo);
	}

}
