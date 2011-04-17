package edu.duke.starfish.profile.profileinfo.execution.mrtasks;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;

/**
 * Represents the information about a Map Task executed in a Map-Reduce job in
 * the cluster
 * 
 * @author hero
 * 
 */
public class MRMapInfo extends MRTaskInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	List<MRMapAttemptInfo> attempts; // The task attempts
	List<SlaveHostInfo> splitHosts; // Hosts where the split resides

	/**
	 * Default Constructor
	 */
	public MRMapInfo() {
		super();
		this.attempts = new ArrayList<MRMapAttemptInfo>(1);
		this.splitHosts = new ArrayList<SlaveHostInfo>(3);
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
	 * @param splitHosts
	 *            the hosts of the split process by this map task
	 */
	public MRMapInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			List<SlaveHostInfo> splitHosts) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
		this.attempts = new ArrayList<MRMapAttemptInfo>(1);
		this.splitHosts = splitHosts;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MRMapInfo(MRMapInfo other) {
		super(other);

		this.attempts = new ArrayList<MRMapAttemptInfo>(other.attempts.size());
		for (MRMapAttemptInfo attempt : other.attempts)
			addAttempt(new MRMapAttemptInfo(attempt));

		this.splitHosts = new ArrayList<SlaveHostInfo>(other.splitHosts.size());
		for (SlaveHostInfo host : other.splitHosts)
			addSplitHost(new SlaveHostInfo(host));
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the map attempts
	 */
	@Override
	public List<MRMapAttemptInfo> getAttempts() {
		return attempts;
	}

	/**
	 * @return the hosts of the input split
	 */
	public List<SlaveHostInfo> getSplitHosts() {
		return splitHosts;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	@Override
	public MRMapAttemptInfo getSuccessfulAttempt() {
		for (MRMapAttemptInfo attempt : attempts)
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
			hash = 37 * hash
					+ ((splitHosts == null) ? 0 : splitHosts.hashCode());
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
		if (!(obj instanceof MRMapInfo))
			return false;
		MRMapInfo other = (MRMapInfo) obj;
		if (attempts == null) {
			if (other.attempts != null)
				return false;
		} else if (!attempts.equals(other.attempts))
			return false;
		if (splitHosts == null) {
			if (other.splitHosts != null)
				return false;
		} else if (!splitHosts.equals(other.splitHosts))
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
		String str = "MRMapInfo [TaskId=" + getExecId() + ", StartTime="
				+ getStartTime() + ", EndTime=" + getEndTime() + ", Status="
				+ getStatus() + ", NumAttempts=" + attempts.size()
				+ ", splitHosts=[";

		if (splitHosts != null) {
			for (SlaveHostInfo host : splitHosts) {
				str += host.getName() + ",";
			}
		} else {
			str += "null";
		}

		str += "]]";
		return str;
	}

	/* ***************************************************************
	 * PUBLIC METHOD
	 * ***************************************************************
	 */

	/**
	 * @param attempt
	 *            the attempt to add
	 */
	public void addAttempt(MRMapAttemptInfo attempt) {
		hash = -1;
		attempts.add(attempt);
	}

	/**
	 * 
	 * @param host
	 *            the host of the split to add
	 */
	public void addSplitHost(SlaveHostInfo host) {
		hash = -1;
		splitHosts.add(host);
	}

	/**
	 * Find and return the map attempt that corresponds to the input attempt id
	 * 
	 * @param attemptId
	 *            the attempt id to look for
	 * @return the requested map attempt
	 */
	public MRMapAttemptInfo findMRMapAttempt(String attemptId) {
		for (MRMapAttemptInfo mrMapAttempt : attempts) {
			if (mrMapAttempt.getExecId().equals(attemptId)) {
				return mrMapAttempt;
			}
		}

		return null;
	}
}
