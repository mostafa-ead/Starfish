package edu.duke.starfish.profile.profileinfo.execution.jobs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRCleanupInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRSetupInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;

/**
 * Represents the information about a Map-Reduce job executed in the cluster
 * 
 * @author hero
 * 
 */
public class MRJobInfo extends JobInfo {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private List<MRSetupInfo> setupTasks; // The setup tasks
	private List<MRMapInfo> mapTasks; // The Map tasks
	private List<MRReduceInfo> reduceTasks; // The Reduce tasks
	private List<MRCleanupInfo> cleanupTasks; // The cleanup tasks

	private List<DataTransfer> dataTransfers; // The data transfers
	private MRJobProfile profile; // The job profile
	
	private MRJobProfile adjProfile; // The adjusted job profile
	private boolean hasAdjProfile; // Whether an adjusted profile exists

	// Cache for successful attempts
	private List<MRMapAttemptInfo> sucMapAttempts;
	private List<MRReduceAttemptInfo> sucReduceAttempts;

	// CONSTANTS
	private static final String JOB = "job";
	private static final String TASK = "task";
	private static final String USCORE = "_";

	/**
	 * Default Constructor
	 */
	public MRJobInfo() {
		super();

		cleanupTasks = new ArrayList<MRCleanupInfo>(1);
		mapTasks = new ArrayList<MRMapInfo>();
		reduceTasks = new ArrayList<MRReduceInfo>();
		setupTasks = new ArrayList<MRSetupInfo>(1);

		dataTransfers = new ArrayList<DataTransfer>(0);
		profile = null;
		adjProfile = null;
		hasAdjProfile = false;

		sucMapAttempts = null;
		sucReduceAttempts = null;
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
	public MRJobInfo(long internalId, String execId, Date startTime,
			Date endTime, MRExecutionStatus status, String errorMsg,
			String name, String user) {
		super(internalId, execId, startTime, endTime, status, errorMsg, name,
				user);

		cleanupTasks = new ArrayList<MRCleanupInfo>(1);
		mapTasks = new ArrayList<MRMapInfo>();
		reduceTasks = new ArrayList<MRReduceInfo>();
		setupTasks = new ArrayList<MRSetupInfo>(1);

		dataTransfers = new ArrayList<DataTransfer>(0);
		profile = null;
		adjProfile = null;
		hasAdjProfile = false;

		sucMapAttempts = null;
		sucReduceAttempts = null;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public MRJobInfo(MRJobInfo other) {
		super(other);

		cleanupTasks = new ArrayList<MRCleanupInfo>(other.cleanupTasks.size());
		for (MRCleanupInfo cleanup : other.cleanupTasks)
			addCleanupTaskInfo(new MRCleanupInfo(cleanup));

		mapTasks = new ArrayList<MRMapInfo>(other.mapTasks.size());
		for (MRMapInfo map : other.mapTasks)
			addMapTaskInfo(new MRMapInfo(map));

		reduceTasks = new ArrayList<MRReduceInfo>(other.reduceTasks.size());
		for (MRReduceInfo reduce : other.reduceTasks)
			addReduceTaskInfo(new MRReduceInfo(reduce));

		setupTasks = new ArrayList<MRSetupInfo>(other.setupTasks.size());
		for (MRSetupInfo setup : other.setupTasks)
			addSetupTaskInfo(new MRSetupInfo(setup));

		dataTransfers = new ArrayList<DataTransfer>(other.dataTransfers.size());
		for (DataTransfer transfer : other.dataTransfers)
			addDataTransfer(new DataTransfer(transfer));

		profile = (other.profile == null) ? null
				: new MRJobProfile(other.profile);
		adjProfile = (other.adjProfile == null) ? null
				: new MRJobProfile(other.adjProfile);
		hasAdjProfile = other.hasAdjProfile;

		sucMapAttempts = null;
		sucReduceAttempts = null;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the setup tasks in the job
	 */
	public List<MRSetupInfo> getSetupTasks() {
		return setupTasks;
	}

	/**
	 * @return the map tasks in the job
	 */
	public List<MRMapInfo> getMapTasks() {
		return mapTasks;
	}

	/**
	 * @return the reduce tasks in the job
	 */
	public List<MRReduceInfo> getReduceTasks() {
		return reduceTasks;
	}

	/**
	 * @return the cleanup tasks in the job
	 */
	public List<MRCleanupInfo> getCleanupTasks() {
		return cleanupTasks;
	}

	/**
	 * @return the data transfers
	 */
	public List<DataTransfer> getDataTransfers() {
		return dataTransfers;
	}

	/**
	 * @return the job profile
	 */
	public MRJobProfile getProfile() {
		if (profile == null) // Create on demand
			profile = new MRJobProfile(getExecId());
		return profile;
	}

	/**
	 * @return the adjusted job profile
	 */
	public MRJobProfile getAdjProfile() {
		return (hasAdjProfile) ? adjProfile : getProfile();
	}

	/**
	 * @param profile
	 *            the job profile to set
	 */
	public void setProfile(MRJobProfile profile) {
		this.profile = profile;
	}

	/**
	 * @param adjProfile
	 *            the adjusted job profile to set
	 */
	public void setAdjProfile(MRJobProfile adjProfile) {
		this.adjProfile = adjProfile;
		this.hasAdjProfile = true;
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
			hash = 31 * hash
					+ ((cleanupTasks == null) ? 0 : cleanupTasks.hashCode());
			hash = 37 * hash + ((mapTasks == null) ? 0 : mapTasks.hashCode());
			hash = 41 * hash
					+ ((reduceTasks == null) ? 0 : reduceTasks.hashCode());
			hash = 43 * hash
					+ ((setupTasks == null) ? 0 : setupTasks.hashCode());
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
		if (!(obj instanceof MRJobInfo))
			return false;
		MRJobInfo other = (MRJobInfo) obj;
		if (cleanupTasks == null) {
			if (other.cleanupTasks != null)
				return false;
		} else if (!cleanupTasks.equals(other.cleanupTasks))
			return false;
		if (mapTasks == null) {
			if (other.mapTasks != null)
				return false;
		} else if (!mapTasks.equals(other.mapTasks))
			return false;
		if (reduceTasks == null) {
			if (other.reduceTasks != null)
				return false;
		} else if (!reduceTasks.equals(other.reduceTasks))
			return false;
		if (setupTasks == null) {
			if (other.setupTasks != null)
				return false;
		} else if (!setupTasks.equals(other.setupTasks))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MRJobInfo [ID=" + getExecId() + ", Name=" + getName()
				+ ", User=" + getUser() + ", StartTime=" + getStartTime()
				+ ", EndTime=" + getEndTime() + ", Status=" + getStatus()
				+ ", NumCleanupTasks=" + cleanupTasks.size() + ", NumMapTasks="
				+ mapTasks.size() + ", NumReduceTasks=" + reduceTasks.size()
				+ ", NumSetupTasks=" + setupTasks.size() + "]";
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add a data transfers in this job.
	 * 
	 * @param transfer
	 *            the data transfer to add
	 */
	public void addDataTransfer(DataTransfer transfer) {
		dataTransfers.add(transfer);
	}

	/**
	 * Add a collection of data transfers in this job.
	 * 
	 * @param transfers
	 *            the data transfers to add
	 */
	public void addDataTransfers(Collection<DataTransfer> transfers) {
		dataTransfers.addAll(transfers);
	}

	/**
	 * @param cleanupInfo
	 *            the cleanup info to add
	 */
	public void addCleanupTaskInfo(MRCleanupInfo cleanupInfo) {
		hash = -1;
		cleanupTasks.add(cleanupInfo);
	}

	/**
	 * @param mapInfo
	 *            the map info to add
	 */
	public void addMapTaskInfo(MRMapInfo mapInfo) {
		hash = -1;
		mapTasks.add(mapInfo);
	}

	/**
	 * @param reduceInfo
	 *            the reduce info to add
	 */
	public void addReduceTaskInfo(MRReduceInfo reduceInfo) {
		hash = -1;
		reduceTasks.add(reduceInfo);
	}

	/**
	 * @param setupInfo
	 *            the setup info to add
	 */
	public void addSetupTaskInfo(MRSetupInfo setupInfo) {
		hash = -1;
		setupTasks.add(setupInfo);
	}

	/**
	 * Copy all the content of the other job into this job.
	 * 
	 * NOTE: This is a shallow copy! If you want to perform a deep copy, use the
	 * copy constructor
	 * 
	 * @param other
	 *            the other job to copy from
	 */
	public void copyOtherJob(MRJobInfo other) {
		this.setupTasks = other.setupTasks;
		this.mapTasks = other.mapTasks;
		this.reduceTasks = other.reduceTasks;
		this.cleanupTasks = other.cleanupTasks;

		this.dataTransfers = other.dataTransfers;
		this.profile = other.profile;
		this.adjProfile = other.adjProfile;
		this.hasAdjProfile = other.hasAdjProfile;

		this.sucMapAttempts = other.sucMapAttempts;
		this.sucReduceAttempts = other.sucReduceAttempts;

		setName(other.getName());
		setUser(other.getUser());

		setExecId(other.getExecId());
		setStartTime(other.getStartTime());
		setEndTime(other.getEndTime());
		setStatus(other.getStatus());
		setErrorMsg(other.getErrorMsg());
		setInternalId(other.getInternalId());
	}

	/**
	 * Get the data transfers originating from this map attempt
	 * 
	 * @param mrMapAttempt
	 *            the map attempt
	 * @return data transfers
	 */
	public List<DataTransfer> getDataTransfersFromMap(
			MRMapAttemptInfo mrMapAttempt) {

		// Find the matching transfers from the map attempt
		List<DataTransfer> result = new ArrayList<DataTransfer>(reduceTasks
				.size());
		for (DataTransfer dataTransfer : dataTransfers) {
			if (dataTransfer.getSource().equals(mrMapAttempt)) {
				result.add(dataTransfer);
			}
		}

		return result;
	}

	/**
	 * Get the data transfers towards this reduce attempt
	 * 
	 * @param mrReduceAttempt
	 *            the reduce attempt
	 * @return data transfers
	 */
	public List<DataTransfer> getDataTransfersToReduce(
			MRReduceAttemptInfo mrReduceAttempt) {

		// Find the matching transfers to the reduce attempt
		List<DataTransfer> result = new ArrayList<DataTransfer>(mapTasks.size());
		for (DataTransfer dataTransfer : dataTransfers) {
			if (dataTransfer.getDestination().equals(mrReduceAttempt)) {
				result.add(dataTransfer);
			}
		}

		return result;
	}

	/**
	 * Get a list of the map attempts with the given execution status
	 * 
	 * @param status
	 *            the desired execution status
	 * @return a list of map attempts
	 */
	public List<MRMapAttemptInfo> getMapAttempts(MRExecutionStatus status) {
		// Look in the cache first
		if (status == MRExecutionStatus.SUCCESS && sucMapAttempts != null)
			return sucMapAttempts;

		// Gather the map attempts
		List<MRMapAttemptInfo> mrMapAttempts = new ArrayList<MRMapAttemptInfo>(
				mapTasks.size());

		for (MRMapInfo mrMap : mapTasks) {
			for (MRMapAttemptInfo mrMapAttempt : mrMap.getAttempts()) {
				if (mrMapAttempt.getStatus() == status) {
					mrMapAttempts.add(mrMapAttempt);
				}
			}
		}

		// Save the list in the cache
		if (status == MRExecutionStatus.SUCCESS)
			sucMapAttempts = mrMapAttempts;

		return mrMapAttempts;
	}

	/**
	 * Get a list of the reduce attempts with the given execution status
	 * 
	 * @param status
	 *            the desired execution status
	 * @return a list of reduce attempts
	 */
	public List<MRReduceAttemptInfo> getReduceAttempts(MRExecutionStatus status) {
		// Look in the cache first
		if (status == MRExecutionStatus.SUCCESS && sucReduceAttempts != null)
			return sucReduceAttempts;

		// Gather the map attempts
		List<MRReduceAttemptInfo> mrReduceAttempts = new ArrayList<MRReduceAttemptInfo>(
				reduceTasks.size());

		for (MRReduceInfo mrReduce : reduceTasks) {
			for (MRReduceAttemptInfo mrReduceAttempt : mrReduce.getAttempts()) {
				if (mrReduceAttempt.getStatus() == status) {
					mrReduceAttempts.add(mrReduceAttempt);
				}
			}
		}

		// Save the list in the cache
		if (status == MRExecutionStatus.SUCCESS)
			sucReduceAttempts = mrReduceAttempts;

		return mrReduceAttempts;
	}

	/**
	 * Find and return the map attempt given its id
	 * 
	 * @param attemptId
	 *            the attempt id of the map attempt to find
	 * @return the map attempt
	 */
	public MRMapAttemptInfo findMRMapAttempt(String attemptId) {
		String mapId = getTaskIdFromAttemptId(attemptId);
		for (MRMapInfo map : mapTasks) {
			if (map.getExecId().equals(mapId)) {
				return map.findMRMapAttempt(attemptId);
			}
		}
		return null;
	}

	/**
	 * Find and return the reduce attempt given its id
	 * 
	 * @param attemptId
	 *            the attempt id of the reduce attempt to find
	 * @return the reduce attempt
	 */
	public MRReduceAttemptInfo findMRReduceAttempt(String attemptId) {
		String reduceId = getTaskIdFromAttemptId(attemptId);
		for (MRReduceInfo reduce : reduceTasks) {
			if (reduce.getExecId().equals(reduceId)) {
				return reduce.findMRReduceAttempt(attemptId);
			}
		}
		return null;
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Constructs the task id given the task attempt id.
	 * 
	 * Example: attempt_201007181819_0051_m_000001_0 to
	 * task_201007181819_0051_m_000001
	 * 
	 * @param attemptId
	 *            the attempt id
	 * @return the task id
	 */
	public static String getTaskIdFromAttemptId(String attemptId) {
		String[] pieces = attemptId.split(USCORE);
		if (pieces != null && pieces.length == 6) {
			StringBuffer sb = new StringBuffer(32);
			sb.append(TASK);
			sb.append(USCORE);
			sb.append(pieces[1]);
			sb.append(USCORE);
			sb.append(pieces[2]);
			sb.append(USCORE);
			sb.append(pieces[3]);
			sb.append(USCORE);
			sb.append(pieces[4]);
			return sb.toString();
		} else
			return null;
	}

	/**
	 * Constructs the job id given the task attempt id.
	 * 
	 * Example: attempt_201007181819_0051_m_000001_0 to job_201007181819_0051
	 * 
	 * @param attemptId
	 *            the attempt id
	 * @return the job id
	 */
	public static String getJobIdFromAttemptId(String attemptId) {
		String[] pieces = attemptId.split(USCORE);
		if (pieces != null && pieces.length == 6) {
			StringBuffer sb = new StringBuffer(22);
			sb.append(JOB);
			sb.append(USCORE);
			sb.append(pieces[1]);
			sb.append(USCORE);
			sb.append(pieces[2]);
			return sb.toString();
		} else
			return null;
	}

}
