package edu.duke.starfish.whatif.scheduler;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_RED_SLOWSTART_MAPS;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.DEF_RED_SLOWSTART_MAPS;

import java.text.NumberFormat;
import java.util.ArrayList;
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
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
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
	private ClusterConfiguration cluster;
	private PriorityQueue<TaskSlot> mapSlots;
	private PriorityQueue<TaskSlot> redSlots;

	private boolean ignoreReducers; // Flag to not schedule the reducers

	// Constants
	private static final long HALF_HEARTBEAT_DELAY = 1500l;
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
	 * Default Constructor
	 */
	public BasicFIFOScheduler() {
		this.cluster = null;
		this.mapSlots = null;
		this.redSlots = null;
		this.ignoreReducers = false;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see edu.duke.starfish.whatif.scheduler.IWhatIfScheduler#scheduleJobGetJobInfo(ClusterConfiguration,
	 *      MRJobProfile, Configuration)
	 */
	@Override
	public MRJobInfo scheduleJobGetJobInfo(ClusterConfiguration cluster,
			MRJobProfile jobProfile, Configuration conf) {

		// Initialize the task slots
		initialize(cluster);

		// Create the job
		Date jobStartTime = new Date();
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
				long size1 = p1.getCounter(MRCounter.HDFS_BYTES_READ, 0l);
				long size2 = p2.getCounter(MRCounter.HDFS_BYTES_READ, 0l);

				if (size1 == size2)
					return 0;
				else if (size1 > size2)
					return -1;
				else
					return 1;
			}
		});

		// Schedule all the map tasks
		int mapId = 0;
		long mapsExecutionTime = 0l;
		Date lastMapEndTime = null;
		for (MRMapProfile mapProf : mapProfs) {

			// Schedule all map tasks to the map slots
			int numTasks = mapProf.getNumTasks();
			for (int i = 0; i < numTasks; ++i) {
				TaskSlot mapSlot = mapSlots.poll();
				MRMapAttemptInfo mapAttempt = scheduleMapExecution(mapSlot,
						mapProf, jobStartTime);
				mapAttempt.setProfile(mapProf);
				mapSlot.addTaskAttempt(mapAttempt);
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

				// Keep track of the overall map completion time
				if (mapSlot.getTotalExecTime() > mapsExecutionTime) {
					mapsExecutionTime = mapSlot.getTotalExecTime();
					lastMapEndTime = mapSlot.getLastTaskAttempt().getEndTime();
				}
			}
		}

		// Stop here if there are no reducers or asked to
		List<MRReduceProfile> redProfiles = jobProfile.getReduceProfiles();
		if (redProfiles.size() == 0 || ignoreReducers) {
			job.setEndTime(new Date(lastMapEndTime.getTime()
					+ HALF_HEARTBEAT_DELAY));
			return job;
		}

		// Calculate the number of completed maps before reducers start
		int numMapTasks = jobProfile.getCounter(MRCounter.MAP_TASKS).intValue();

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
		long reducersExecutionTime = 0l;
		Date lastReduceEndTime = lastMapEndTime;

		for (MRReduceProfile redProfile : redProfiles) {

			// Schedule all reduce tasks to the reduce slots
			int numRedTasks = redProfile.getNumTasks();
			for (int i = 0; i < numRedTasks; ++i) {
				TaskSlot redSlot = redSlots.poll();
				MRReduceAttemptInfo redAttempt = scheduleReduceExecution(
						redSlot, redProfile, redSlowStartTime, lastMapEndTime,
						numMapTasks);
				redAttempt.setProfile(redProfile);
				redSlot.addTaskAttempt(redAttempt);
				redSlots.add(redSlot);

				// Add the reducer in the job
				MRReduceInfo reducer = new MRReduceInfo(0, redProfile
						.getTaskId(), redAttempt.getStartTime(), redAttempt
						.getEndTime(), MRExecutionStatus.SUCCESS, null);
				reducer.addAttempt(redAttempt);
				job.addReduceTaskInfo(reducer);

				// Set the ids
				reducer.setExecId(buildTaskId(jobId, redId, false));
				redAttempt.setExecId(buildAttemptId(jobId, redId, 0, false));
				++redId;

				// Keep track of the overall job completion time
				if (redSlot.getTotalExecTime() > reducersExecutionTime) {
					reducersExecutionTime = redSlot.getTotalExecTime();
					lastReduceEndTime = redSlot.getLastTaskAttempt()
							.getEndTime();
				}
			}
		}

		Date endTime = new Date(lastReduceEndTime.getTime()
				+ HALF_HEARTBEAT_DELAY);
		job.setEndTime(endTime);

		return job;
	}

	/**
	 * @see edu.duke.starfish.whatif.scheduler.IWhatIfScheduler#scheduleJobGetTime(ClusterConfiguration,
	 *      MRJobProfile, Configuration)
	 */
	@Override
	public double scheduleJobGetTime(ClusterConfiguration cluster,
			MRJobProfile jobProfile, Configuration conf) {
		return scheduleJobGetJobInfo(cluster, jobProfile, conf).getDuration();
	}

	/**
	 * @see edu.duke.starfish.whatif.scheduler.IWhatIfScheduler#setIgnoreReducers(boolean)
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
	 * Initialize the task slots
	 * 
	 * @param cluster
	 *            the cluster setup
	 */
	private void initialize(ClusterConfiguration cluster) {

		// Check if task slots are initialized already
		if (this.cluster == cluster) {
			for (TaskSlot slot : mapSlots)
				slot.initializeSlot();
			for (TaskSlot slot : redSlots)
				slot.initializeSlot();

			return;
		}

		// Initialize the task slots
		this.cluster = cluster;
		this.mapSlots = new PriorityQueue<TaskSlot>();
		this.redSlots = new PriorityQueue<TaskSlot>();

		for (TaskTrackerInfo taskTracker : cluster.getAllTaskTrackersInfos()) {
			// Initialize the map slots
			int numMapSlots = taskTracker.getNumMapSlots();
			for (int i = 0; i < numMapSlots; ++i)
				mapSlots.add(new TaskSlot(taskTracker));

			// Initialize the reduce slots
			int numRedSlots = taskTracker.getNumReduceSlots();
			for (int i = 0; i < numRedSlots; ++i)
				redSlots.add(new TaskSlot(taskTracker));
		}
	}

	/**
	 * Schedule a map execution on a task slot.
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
		Date startTime;
		MRTaskAttemptInfo lastAttempt = taskSlot.getLastTaskAttempt();
		if (lastAttempt != null)
			startTime = lastAttempt.getEndTime();
		else
			startTime = jobStartTime;
		startTime = new Date(startTime.getTime() + HALF_HEARTBEAT_DELAY);
		Date endTime = new Date(startTime.getTime() + (long) execTime);

		// Return the map attempt
		return new MRMapAttemptInfo(0, mapProfile.getTaskId(), startTime,
				endTime, MRExecutionStatus.SUCCESS, null, taskSlot
						.getTaskTracker(), DataLocality.DATA_LOCAL);
	}

	/**
	 * Schedule a reducer for execution on a task slot
	 * 
	 * @param taskSlot
	 *            the task slot
	 * @param redProfile
	 *            the reducer profile
	 * @param redSlowStartTime
	 *            the earliest time for reducers to start
	 * @param mapEndTime
	 *            the end time of the last mapper
	 * @param numMappers
	 *            the total number of map tasks
	 * @return the reduce attempt
	 */
	private MRReduceAttemptInfo scheduleReduceExecution(TaskSlot taskSlot,
			MRReduceProfile redProfile, Date redSlowStartTime, Date mapEndTime,
			int numMappers) {

		// Calculate the start time
		Date startTime;
		MRTaskAttemptInfo lastAttempt = taskSlot.getLastTaskAttempt();
		if (lastAttempt != null)
			startTime = lastAttempt.getEndTime();
		else
			startTime = redSlowStartTime;
		startTime = new Date(startTime.getTime() + HALF_HEARTBEAT_DELAY);

		// The shuffle will complete only after all maps have completed
		double shuffleTime = redProfile.getTiming(MRTaskPhase.SHUFFLE, 0d);
		Date endShuffleTime;
		if (taskSlot.getNumWaves() == 0
				&& shuffleTime < mapEndTime.getTime() - startTime.getTime()) {
			endShuffleTime = new Date(mapEndTime.getTime()
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

		return new MRReduceAttemptInfo(0, redProfile.getTaskId(), startTime,
				endReduceTime, MRExecutionStatus.SUCCESS, null, taskSlot
						.getTaskTracker(), endShuffleTime, endSortTime);
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

		private TaskTrackerInfo taskTracker;
		private ArrayList<MRTaskAttemptInfo> taskAttempts;
		private long totalExecTime;

		/**
		 * Constructor
		 * 
		 * @param taskTracker
		 *            the task tracker
		 */
		public TaskSlot(TaskTrackerInfo taskTracker) {
			this.taskTracker = taskTracker;
			this.taskAttempts = new ArrayList<MRTaskAttemptInfo>();
			this.totalExecTime = 0l;
		}

		/**
		 * @return the numWaves
		 */
		public int getNumWaves() {
			return taskAttempts.size();
		}

		/**
		 * @return the total execution time
		 */
		public long getTotalExecTime() {
			return totalExecTime;
		}

		/**
		 * @return the task tracker
		 */
		public TaskTrackerInfo getTaskTracker() {
			return taskTracker;
		}

		public MRTaskAttemptInfo getLastTaskAttempt() {
			if (taskAttempts.size() != 0) {
				return taskAttempts.get(taskAttempts.size() - 1);
			} else {
				return null;
			}
		}

		/**
		 * Initialize the task slot
		 */
		public void initializeSlot() {
			this.taskAttempts.clear();
			this.totalExecTime = 0l;
		}

		/**
		 * @param totalExecTime
		 *            the execution time to add
		 */
		public void addTaskAttempt(MRTaskAttemptInfo taskAttempt) {
			this.taskAttempts.add(taskAttempt);
			this.totalExecTime += taskAttempt.getDuration();
		}

		@Override
		public int compareTo(TaskSlot other) {
			if (this.totalExecTime < other.totalExecTime)
				return -1;
			else if (this.totalExecTime > other.totalExecTime)
				return 1;
			else
				return 0;
		}

	}

}
