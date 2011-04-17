package edu.duke.starfish.profile.profileinfo.execution.mrtasks;

import java.util.Date;
import java.util.List;

import edu.duke.starfish.profile.profileinfo.execution.ClusterExecutionInfo;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;

/**
 * Represents the information about a Map-Reduce Task executed in a Map-Reduce
 * job in the cluster
 * 
 * @author hero
 * 
 */
public abstract class MRTaskInfo extends ClusterExecutionInfo {

	/**
	 * Default Constructor
	 */
	public MRTaskInfo() {
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
	 */
	public MRTaskInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg) {
		super(internalId, execId, startTime, endTime, status, errorMsg);
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MRTaskInfo(MRTaskInfo other) {
		super(other);
	}

	/**
	 * @return Return the task attempt that was successful (if any)
	 */
	public abstract MRTaskAttemptInfo getSuccessfulAttempt();

	/**
	 * @return Return all task attempts
	 */
	public abstract List<? extends MRTaskAttemptInfo> getAttempts();

}
