package edu.duke.starfish.whatif.scheduler;

import static edu.duke.starfish.profile.utils.Constants.DEF_RED_SLOWSTART_MAPS;
import static edu.duke.starfish.profile.utils.Constants.MR_RED_SLOWSTART_MAPS;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRCleanupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRCleanupInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRSetupInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * A basic FIFO scheduler that simulates the execution of a MapReduce on a
 * Hadoop cluster, based on a virtual job profile and a provided configuration.
 * The simulator follows a model-based approach.
 * 
 * @author hero
 */
public class BasicFIFOScheduler implements IWhatIfScheduler {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Simulation setup
	private PriorityQueue<TaskSlot> mapSlots;
	private PriorityQueue<TaskSlot> redSlots;

	private boolean ignoreReducers; // Flag to not schedule the reducers
	private ClusterConfiguration cluster;

	// Constants
	private static final long HALF_HEARTBEAT_DELAY = 1500l;
	private static final long HEARTBEAT_DELAY = 3000l;
	private static final long SETUP_CLEANUP_TIME = 3000l;
	private static final String JOB_NAME = "Virtual Job";
	private static final String USER_NAME = "Virtual User";
	private static final String VIRTUAL_TASK = "virtual_task_";
	private static final String VIRTUAL_ATTEMPT = "virtual_attempt_";
	private static final String U_MAP_U = "_m_";
	private static final String U_RED_U = "_r_";
	private static final NumberFormat nf = NumberFormat.getInstance();
	private static final Pattern JOB_ID_PATTERN = Pattern
			.compile(".*_([0-9]+_[0-9]+)");

	{
		nf.setMinimumIntegerDigits(6);
		nf.setGroupingUsed(false);
	}

	/**
	 * Constructor
	 * 
	 * @param cluster
	 *            the cluster to schedule tasks on
	 */
	public BasicFIFOScheduler(ClusterConfiguration cluster) {

		// Initialize the task slots
		this.mapSlots = new PriorityQueue<TaskSlot>(cluster.getTotalMapSlots());
		this.redSlots = new PriorityQueue<TaskSlot>(
				cluster.getTotalReduceSlots());
		Date launchTime = new Date();

		for (TaskTrackerInfo taskTracker : cluster.getAllTaskTrackersInfos()) {
			// Initialize the map slots
			int numMapSlots = taskTracker.getNumMapSlots();
			for (int i = 0; i < numMapSlots; ++i)
				mapSlots.add(new TaskSlot(taskTracker, launchTime));

			// Initialize the reduce slots
			int numRedSlots = taskTracker.getNumReduceSlots();
			for (int i = 0; i < numRedSlots; ++i)
				redSlots.add(new TaskSlot(taskTracker, launchTime));
		}

		this.ignoreReducers = false;
		this.cluster = cluster;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see IWhatIfScheduler#checkpoint()
	 */
	@Override
	public void checkpoint() {
		// Checkpoint all slots
		for (TaskSlot slot : mapSlots)
			slot.checkpoint();
		for (TaskSlot slot : redSlots)
			slot.checkpoint();
	}

	/**
	 * @see IWhatIfScheduler#reset()
	 */
	@Override
	public void reset() {
		// Reset all slots
		for (TaskSlot slot : mapSlots)
			slot.reset();
		for (TaskSlot slot : redSlots)
			slot.reset();
	}

	/**
	 * @see IWhatIfScheduler#getCluster()
	 */
	@Override
	public ClusterConfiguration getCluster() {
		return cluster;
	}

	/**
	 * @see IWhatIfScheduler#scheduleJobGetJobInfo(Date, MRJobProfile,
	 *      Configuration)
	 */
	@Override
	public MRJobInfo scheduleJobGetJobInfo(Date submissionTime,
			MRJobProfile jobProfile, Configuration conf) {

		// Find the job start time
		Date jobStartTime = mapSlots.peek().getReadyTime();
		if (jobStartTime.before(submissionTime))
			jobStartTime = submissionTime;
		jobStartTime = new Date(jobStartTime.getTime() + HEARTBEAT_DELAY);

		// Create the job
		MRJobInfo job = new MRJobInfo(0, jobProfile.getJobId(), jobStartTime,
				null, MRExecutionStatus.SUCCESS, null, JOB_NAME, USER_NAME);
		job.setProfile(jobProfile);

		// Parse the job id
		String jobId = jobProfile.getJobId();
		Matcher m = JOB_ID_PATTERN.matcher(jobId);
		if (m.matches()) {
			jobId = m.group(1);
		}

		// The Hadoop schedulers sorts the input splits based on size
		List<MRMapProfile> mapProfs = jobProfile.getMapProfiles();
		Collections.sort(mapProfs, new Comparator<MRMapProfile>() {
			@Override
			public int compare(MRMapProfile p1, MRMapProfile p2) {
				Long size1 = p1.getCounter(MRCounter.HDFS_BYTES_READ,
						p1.getCounter(MRCounter.S3N_BYTES_READ, 0l));
				Long size2 = p2.getCounter(MRCounter.HDFS_BYTES_READ,
						p2.getCounter(MRCounter.S3N_BYTES_READ, 0l));

				return size2.compareTo(size1);
			}
		});

		// Schedule the setup task attempt on a map slot
		int numMapTasks = jobProfile.getCounter(MRCounter.MAP_TASKS).intValue();
		TaskSlot setupSlot = mapSlots.poll();
		MRSetupAttemptInfo setupAttempt = scheduleSetupExecution(setupSlot,
				buildAttemptId(jobId, numMapTasks + 1, 0, true), jobStartTime);
		mapSlots.add(setupSlot);

		// Create the setup task
		MRSetupInfo setup = new MRSetupInfo(0, buildTaskId(jobId,
				numMapTasks + 1, true), setupAttempt.getStartTime(),
				setupAttempt.getEndTime(), MRExecutionStatus.SUCCESS, null);
		setup.addAttempt(setupAttempt);
		job.addSetupTaskInfo(setup);

		// Move the job start time after the setup completes
		jobStartTime = new Date(setupAttempt.getEndTime().getTime());

		// Schedule all the map tasks
		int mapId = 0;
		Date lastMapEndTime = jobStartTime;
		TaskSlot lastMapTaskSlot = null;

		for (MRMapProfile mapProf : mapProfs) {
			int numTasks = mapProf.getNumTasks();
			for (int i = 0; i < numTasks; ++i) {

				// Schedule this map task on a map slot
				TaskSlot mapSlot = mapSlots.poll();
				MRMapAttemptInfo mapAttempt = scheduleMapExecution(mapSlot,
						mapProf, jobStartTime);
				mapAttempt.setProfile(mapProf);
				mapSlots.add(mapSlot);

				// Add the map into the job
				MRMapInfo map = new MRMapInfo(0, mapProf.getTaskId(),
						mapAttempt.getStartTime(), mapAttempt.getEndTime(),
						MRExecutionStatus.SUCCESS, null, null);
				map.addAttempt(mapAttempt);
				job.addMapTaskInfo(map);

				// Set the ids
				map.setExecId(buildTaskId(jobId, mapId, true));
				mapAttempt.setExecId(buildAttemptId(jobId, mapId, 0, true));
				++mapId;

				// Keep track of the last map end time
				if (lastMapEndTime.before(map.getEndTime())) {
					lastMapEndTime = map.getEndTime();
					lastMapTaskSlot = mapSlot;
				}
			}
		}

		// Stop here if there are no reducers or asked to
		List<MRReduceProfile> redProfiles = jobProfile.getReduceProfiles();
		if (redProfiles.size() == 0 || ignoreReducers) {

			// Schedule the cleanup task attempt on the slot run the last map
			mapSlots.remove(lastMapTaskSlot);
			MRCleanupAttemptInfo cleanupAttempt = scheduleCleanupExecution(
					lastMapTaskSlot,
					buildAttemptId(jobId, numMapTasks, 0, true), lastMapEndTime);
			mapSlots.add(lastMapTaskSlot);

			// Create the cleanup task
			MRCleanupInfo cleanup = new MRCleanupInfo(0, buildTaskId(jobId,
					numMapTasks, true), cleanupAttempt.getStartTime(),
					cleanupAttempt.getEndTime(), MRExecutionStatus.SUCCESS,
					null);
			cleanup.addAttempt(cleanupAttempt);
			job.addCleanupTaskInfo(cleanup);

			// Move the job end time after the cleanup completes
			job.setEndTime(new Date(cleanupAttempt.getEndTime().getTime()
					+ HEARTBEAT_DELAY));

			return job;
		}

		// Calculate the number of completed maps before reducers start
		int numMapsBeforeReducers = (int) Math.ceil((conf.getFloat(
				MR_RED_SLOWSTART_MAPS, DEF_RED_SLOWSTART_MAPS) * numMapTasks));
		if (numMapsBeforeReducers == 0)
			++numMapsBeforeReducers;
		if (numMapsBeforeReducers < 0 || numMapsBeforeReducers > numMapTasks)
			throw new RuntimeException("ERROR: The number of maps to complete "
					+ "before reducers can start is out of range: "
					+ numMapsBeforeReducers);

		// All reducers will start after some number of maps have completed
		List<MRMapInfo> maps = job.getMapTasks();
		Collections.sort(maps, new Comparator<MRMapInfo>() {
			@Override
			public int compare(MRMapInfo o1, MRMapInfo o2) {
				return (int) (o1.getEndTime().getTime() - o2.getEndTime()
						.getTime());
			}
		});
		Date redSlowStartTime = maps.get(numMapsBeforeReducers - 1)
				.getEndTime();

		// Schedule all the reduce tasks
		int redId = 0;
		Date lastReduceEndTime = lastMapEndTime;
		TaskSlot lastRedTaskSlot = null;

		for (MRReduceProfile redProfile : redProfiles) {
			int numRedTasks = redProfile.getNumTasks();
			for (int i = 0; i < numRedTasks; ++i) {

				// Schedule this reduce task on a reduce slot
				TaskSlot redSlot = redSlots.poll();
				MRReduceAttemptInfo redAttempt = scheduleReduceExecution(
						redSlot, redProfile, redSlowStartTime, lastMapEndTime,
						numMapTasks);
				redAttempt.setProfile(redProfile);
				redSlots.add(redSlot);

				// Add the reducer in the job
				MRReduceInfo reducer = new MRReduceInfo(0,
						redProfile.getTaskId(), redAttempt.getStartTime(),
						redAttempt.getEndTime(), MRExecutionStatus.SUCCESS,
						null);
				reducer.addAttempt(redAttempt);
				job.addReduceTaskInfo(reducer);

				// Set the ids
				reducer.setExecId(buildTaskId(jobId, redId, false));
				redAttempt.setExecId(buildAttemptId(jobId, redId, 0, false));
				++redId;

				// Keep track of the last reduce end time
				if (lastReduceEndTime.before(reducer.getEndTime())) {
					lastReduceEndTime = reducer.getEndTime();
					lastRedTaskSlot = redSlot;
				}
			}
		}

		// Schedule the cleanup task attempt on the slot run the last reduce
		redSlots.remove(lastRedTaskSlot);
		MRCleanupAttemptInfo cleanupAttempt = scheduleCleanupExecution(
				lastRedTaskSlot, buildAttemptId(jobId, redId + 1, 0, false),
				lastReduceEndTime);
		redSlots.add(lastRedTaskSlot);

		// Create the cleanup task
		MRCleanupInfo cleanup = new MRCleanupInfo(0, buildTaskId(jobId,
				redId + 1, false), cleanupAttempt.getStartTime(),
				cleanupAttempt.getEndTime(), MRExecutionStatus.SUCCESS, null);
		cleanup.addAttempt(cleanupAttempt);
		job.addCleanupTaskInfo(cleanup);

		// Move the job end time after the cleanup completes
		job.setEndTime(new Date(cleanupAttempt.getEndTime().getTime()
				+ HEARTBEAT_DELAY));

		return job;
	}

	/**
	 * @see IWhatIfScheduler#scheduleJobGetTime(Date, MRJobProfile,
	 *      Configuration)
	 */
	@Override
	public double scheduleJobGetTime(Date submissionTime,
			MRJobProfile jobProfile, Configuration conf) {
		return scheduleJobGetJobInfo(submissionTime, jobProfile, conf)
				.getDuration();
	}

	/**
	 * @see IWhatIfScheduler#setIgnoreReducers(boolean)
	 */
	@Override
	public void setIgnoreReducers(boolean ignoreReducers) {
		this.ignoreReducers = ignoreReducers;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Build and return a virtual task attempt id
	 * 
	 * @param jobId
	 *            the job id e.g., 201011062135_0003
	 * @param taskId
	 *            the task id e.g., 5
	 * @param attemptId
	 *            the attempt id e.g., 0
	 * @param isMap
	 *            true if map task
	 * @return the attempt id e.g., virtual_task_201011062135_0003_m_000005_0
	 */
	private String buildAttemptId(String jobId, int taskId, int attemptId,
			boolean isMap) {
		StringBuilder sb = new StringBuilder();
		sb.append(VIRTUAL_ATTEMPT);
		sb.append(jobId);
		sb.append(isMap ? U_MAP_U : U_RED_U);
		sb.append(nf.format(taskId));
		sb.append('_');
		sb.append(attemptId);
		return sb.toString();
	}

	/**
	 * Build and return a virtual task id
	 * 
	 * @param jobId
	 *            the job id e.g., 201011062135_0003
	 * @param taskId
	 *            the task id e.g., 5
	 * @param isMap
	 *            true if map task
	 * @return the attempt id e.g., virtual_task_201011062135_0003_m_000005
	 */
	private String buildTaskId(String jobId, int taskId, boolean isMap) {
		StringBuilder sb = new StringBuilder();
		sb.append(VIRTUAL_TASK);
		sb.append(jobId);
		sb.append(isMap ? U_MAP_U : U_RED_U);
		sb.append(nf.format(taskId));
		return sb.toString();
	}

	/**
	 * Schedule a setup attempt execution on a task slot
	 * 
	 * @param taskSlot
	 *            the task slot
	 * @param attemptId
	 *            the attempt id
	 * @param jobStartTime
	 *            the job start time
	 * @return the setup attempt
	 */
	private MRSetupAttemptInfo scheduleSetupExecution(TaskSlot taskSlot,
			String attemptId, Date jobStartTime) {

		// Calculate the start and end times
		Date startTime = taskSlot.getReadyTime();
		if (startTime.before(jobStartTime))
			startTime = jobStartTime;

		startTime = new Date(startTime.getTime() + HALF_HEARTBEAT_DELAY);
		Date endTime = new Date(startTime.getTime() + SETUP_CLEANUP_TIME);

		// Schedule the map attempt
		MRSetupAttemptInfo setupAttempt = new MRSetupAttemptInfo(0, attemptId,
				startTime, endTime, MRExecutionStatus.SUCCESS, null,
				taskSlot.getTaskTracker());
		taskSlot.scheduleTaskAttempt(setupAttempt);

		return setupAttempt;
	}

	/**
	 * Schedule a map attempt execution on a task slot.
	 * 
	 * @param taskSlot
	 *            the task slot
	 * @param mapProfile
	 *            the map profile
	 * @param jobStartTime
	 *            the start time of the job
	 * @return the map attempt
	 */
	private MRMapAttemptInfo scheduleMapExecution(TaskSlot taskSlot,
			MRMapProfile mapProfile, Date jobStartTime) {

		// Simply add up the sub-phase timings
		double execTime = 0d;
		for (Double subTime : mapProfile.getTimings().values())
			execTime += subTime;

		// Add up the expected heart beat delay
		execTime += HALF_HEARTBEAT_DELAY;

		// Calculate the start and end times
		Date startTime = taskSlot.getReadyTime();
		if (startTime.before(jobStartTime))
			startTime = jobStartTime;

		startTime = new Date(startTime.getTime() + HALF_HEARTBEAT_DELAY);
		Date endTime = new Date(startTime.getTime() + (long) execTime);

		// Schedule the map attempt
		MRMapAttemptInfo mapAttempt = new MRMapAttemptInfo(0,
				mapProfile.getTaskId(), startTime, endTime,
				MRExecutionStatus.SUCCESS, null, taskSlot.getTaskTracker(),
				DataLocality.DATA_LOCAL);
		taskSlot.scheduleTaskAttempt(mapAttempt);

		return mapAttempt;
	}

	/**
	 * Schedule a reduce attempt for execution on a task slot
	 * 
	 * @param taskSlot
	 *            the task slot
	 * @param redProfile
	 *            the reducer profile
	 * @param redSlowStartTime
	 *            the earliest time for reducers to start
	 * @param lastMapEndTime
	 *            the end time of the last map task
	 * @param numMappers
	 *            the total number of map tasks
	 * @return the reduce attempt
	 */
	private MRReduceAttemptInfo scheduleReduceExecution(TaskSlot taskSlot,
			MRReduceProfile redProfile, Date redSlowStartTime,
			Date lastMapEndTime, int numMappers) {

		// Calculate the start time
		Date startTime = taskSlot.getReadyTime();
		if (startTime.before(redSlowStartTime))
			startTime = redSlowStartTime;
		startTime = new Date(startTime.getTime() + HALF_HEARTBEAT_DELAY);

		// The shuffle will complete only after all maps have completed
		double shuffleTime = redProfile.getTiming(MRTaskPhase.SHUFFLE, 0d);
		Date endShuffleTime;
		if (startTime.before(lastMapEndTime)
				&& shuffleTime <= lastMapEndTime.getTime()
						- startTime.getTime()) {
			endShuffleTime = new Date(lastMapEndTime.getTime()
					+ (long) (shuffleTime / numMappers));
		} else {
			endShuffleTime = new Date(startTime.getTime() + (long) shuffleTime);
		}

		// Calculate the end sort time
		double sortTime = redProfile.getTiming(MRTaskPhase.SORT, 0d);
		Date endSortTime = new Date(endShuffleTime.getTime() + (long) sortTime);

		// Calculate the end reduce time
		double redTime = 0d;
		for (Double subTime : redProfile.getTimings().values())
			redTime += subTime;
		redTime = redTime - sortTime - shuffleTime + HALF_HEARTBEAT_DELAY;
		Date endReduceTime = new Date(endSortTime.getTime() + (long) redTime);

		MRReduceAttemptInfo redAttempt = new MRReduceAttemptInfo(0,
				redProfile.getTaskId(), startTime, endReduceTime,
				MRExecutionStatus.SUCCESS, null, taskSlot.getTaskTracker(),
				endShuffleTime, endSortTime);
		taskSlot.scheduleTaskAttempt(redAttempt);

		return redAttempt;
	}

	/**
	 * Schedule a cleanup attempt execution on a task slot
	 * 
	 * @param taskSlot
	 *            the task slot
	 * @param attemptId
	 *            the attempt id
	 * @param lastTaskEndTime
	 *            the end time of the last map or reduce task
	 * @return the setup attempt
	 */
	private MRCleanupAttemptInfo scheduleCleanupExecution(TaskSlot taskSlot,
			String attemptId, Date lastTaskEndTime) {

		// Calculate the start and end times
		Date startTime = taskSlot.getReadyTime();
		if (startTime.before(lastTaskEndTime))
			startTime = lastTaskEndTime;

		startTime = new Date(startTime.getTime() + HALF_HEARTBEAT_DELAY);
		Date endTime = new Date(startTime.getTime() + SETUP_CLEANUP_TIME);

		// Schedule the map attempt
		MRCleanupAttemptInfo cleanupAttempt = new MRCleanupAttemptInfo(0,
				attemptId, startTime, endTime, MRExecutionStatus.SUCCESS, null,
				taskSlot.getTaskTracker());
		taskSlot.scheduleTaskAttempt(cleanupAttempt);

		return cleanupAttempt;
	}

	/* ***************************************************************
	 * PRIVATE CLASS
	 * ***************************************************************
	 */

	/**
	 * Represents a single task (map or reduce) execution slot.
	 * 
	 * @author hero
	 */
	private class TaskSlot implements Comparable<TaskSlot> {

		private TaskTrackerInfo taskTracker; // The task tracker
		private Date checkpointTime; // Checkpoint time
		private Date readyTime; // Time this slot is ready to execute a task

		/**
		 * Constructor
		 * 
		 * @param taskTracker
		 *            the task tracker
		 * @param launchTime
		 *            the launch time for this slot
		 */
		public TaskSlot(TaskTrackerInfo taskTracker, Date launchTime) {
			this.taskTracker = taskTracker;
			this.checkpointTime = launchTime;
			this.readyTime = launchTime;
		}

		/**
		 * @return the task tracker
		 */
		public TaskTrackerInfo getTaskTracker() {
			return taskTracker;
		}

		/**
		 * @return the earliest time this slot can schedule a task
		 */
		public Date getReadyTime() {
			return readyTime;
		}

		/**
		 * Checkpoint the ready time
		 */
		public void checkpoint() {
			checkpointTime = readyTime;
		}

		/**
		 * Reset the ready time
		 */
		public void reset() {
			readyTime = checkpointTime;
		}

		/**
		 * @param taskAttempt
		 *            task attempt to schedule
		 */
		public void scheduleTaskAttempt(MRTaskAttemptInfo taskAttempt) {
			readyTime = taskAttempt.getEndTime();
		}

		@Override
		public int compareTo(TaskSlot other) {
			return this.readyTime.compareTo(other.readyTime);
		}

	}

}
