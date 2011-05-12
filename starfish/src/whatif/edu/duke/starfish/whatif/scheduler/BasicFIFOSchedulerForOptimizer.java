package edu.duke.starfish.whatif.scheduler;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_RED_SLOWSTART_MAPS;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.DEF_RED_SLOWSTART_MAPS;

import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
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
public class BasicFIFOSchedulerForOptimizer implements IWhatIfScheduler {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Simulation setup
	private ClusterConfiguration cluster;
	private PriorityQueue<TaskSlot> mapSlots;
	private PriorityQueue<TaskSlot> redSlots;

	private boolean ignoreReducers;

	// Constants
	private static final double HEARTBEAT_DELAY = 3000d;

	/**
	 * Default Constructor
	 */
	public BasicFIFOSchedulerForOptimizer() {
		this.cluster = null;
		mapSlots = null;
		redSlots = null;
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
		throw new RuntimeException("ERROR: The BasicFIFOSchedulerForOptimizer "
				+ "does not support the method scheduleJobGetJobInfo");
	}

	/**
	 * @see edu.duke.starfish.whatif.scheduler.IWhatIfScheduler#scheduleJobGetTime(ClusterConfiguration,
	 *      MRJobProfile, Configuration)
	 */
	@Override
	public double scheduleJobGetTime(ClusterConfiguration cluster,
			MRJobProfile jobProfile, Configuration conf) {

		// Initialize the task slots
		initializeTaskSlots(cluster);

		// Calculate the number of completed maps before reducers start
		int numMapTasks = jobProfile.getCounter(MRCounter.MAP_TASKS).intValue();
		int numMapsBeforeReducers = (int) Math.ceil((conf.getFloat(
				MR_RED_SLOWSTART_MAPS, DEF_RED_SLOWSTART_MAPS) * numMapTasks));
		double reducerStartTime = 0d;

		// Schedule all the map tasks
		double mapsCompletionTime = 0d;
		int numMapsScheduled = 0;
		for (MRMapProfile mapProf : jobProfile.getAvgMapProfiles()) {

			// Simulate the map execution
			int numTasks = mapProf.getNumTasks();
			double mapExecTime = simulateMapExecution(mapProf);

			// Schedule all map tasks to the map slots
			TaskSlot mapSlot = null;
			for (int i = 0; i < numTasks; ++i) {
				mapSlot = mapSlots.poll();
				mapSlot.addExecTime(mapExecTime);
				mapSlots.add(mapSlot);

				// Keep track of the overall map completion time
				if (mapSlot.getExecTime() > mapsCompletionTime) {
					mapsCompletionTime = mapSlot.getExecTime();
				}

				// Figure out when reducers can start executing
				++numMapsScheduled;
				if (numMapsScheduled == numMapsBeforeReducers) {
					reducerStartTime = mapsCompletionTime;
				}
			}
		}

		// Stop here if we want to ignore the reducers
		if (ignoreReducers)
			return mapsCompletionTime;

		// All reducers will start after the reducerStartTime calculated above
		for (TaskSlot redSlot : redSlots) {
			redSlot.addExecTime(reducerStartTime);
		}

		double jobCompletionTime = mapsCompletionTime;
		MRReduceProfile redProfile = jobProfile.getAvgReduceProfile();

		if (redProfile != null && redProfile.getNumTasks() > 0) {
			// Calculate the reduce execution time
			double shuffleMapOverlap = mapsCompletionTime - reducerStartTime;
			double redExecTimeFirstWave = simulateReduceExecution(redProfile,
					true, shuffleMapOverlap);
			double redExecTimeOtherWave = simulateReduceExecution(redProfile,
					false, shuffleMapOverlap);

			// Schedule all reduce tasks to the reduce slots
			int numRedTasks = redProfile.getNumTasks();
			TaskSlot redSlot = null;
			for (int i = 0; i < numRedTasks; ++i) {
				redSlot = redSlots.poll();
				redSlot
						.addExecTime((redSlot.getNumWaves() == 0) ? redExecTimeFirstWave
								: redExecTimeOtherWave);
				redSlots.add(redSlot);

				// Keep track of the overall job completion time
				if (redSlot.getExecTime() > jobCompletionTime) {
					jobCompletionTime = redSlot.getExecTime();
				}
			}
		}

		return jobCompletionTime;
	}

	/**
	 * @param ignoreReducers
	 *            set ignore reducers flag
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
	 * Initialize the task slots
	 * 
	 * @param cluster
	 *            the cluster setup
	 */
	private void initializeTaskSlots(ClusterConfiguration cluster) {

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
				mapSlots.add(new TaskSlot());

			// Initialize the reduce slots
			int numRedSlots = taskTracker.getNumReduceSlots();
			for (int i = 0; i < numRedSlots; ++i)
				redSlots.add(new TaskSlot());
		}
	}

	/**
	 * Simulate the execution of a map task and return the total execution time
	 * (in ms).
	 * 
	 * @param mapProfile
	 *            the virtual map profile
	 * @return the map execution time
	 */
	private double simulateMapExecution(MRMapProfile mapProfile) {

		// Simply add up the sub-phase timings
		double execTime = 0d;
		for (Double subTime : mapProfile.getTimings().values())
			execTime += subTime;

		// Add up the expected heart beat delay
		execTime += HEARTBEAT_DELAY;

		return execTime;
	}

	/**
	 * Simulate the execution of a reduce task and return the total execution
	 * time (in ms).
	 * 
	 * @param redProfile
	 *            the virtual reduce profile
	 * @param firstWave
	 *            whether this is the first wave
	 * @param shuffleMapOverlap
	 *            the time of overlap between shuffling and map execution
	 * @return the reduce execution time
	 */
	private double simulateReduceExecution(MRReduceProfile redProfile,
			boolean firstWave, double shuffleMapOverlap) {

		// Simply add up the sub-phase timings
		double execTime = 0d;
		for (Double subTime : redProfile.getTimings().values())
			execTime += subTime;

		// Add up the expected heart beat delay
		execTime += HEARTBEAT_DELAY;

		if (!firstWave)
			return execTime;

		// The reduce function will only start after all maps have completed
		double shuffleTime = redProfile.getTiming(MRTaskPhase.SHUFFLE, 0d);
		if (shuffleTime < shuffleMapOverlap)
			execTime = execTime - shuffleTime + shuffleMapOverlap;

		return execTime;
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

		private int numWaves;
		private double execTime;

		/**
		 * Default constructor
		 */
		public TaskSlot() {
			this.numWaves = 0;
			this.execTime = 0l;
		}

		/**
		 * @return the numWaves
		 */
		public int getNumWaves() {
			return numWaves;
		}

		/**
		 * @return the execTime
		 */
		public double getExecTime() {
			return execTime;
		}

		/**
		 * Initialize the task slot
		 */
		public void initializeSlot() {
			this.numWaves = 0;
			this.execTime = 0l;
		}

		/**
		 * @param execTime
		 *            the execution time to add
		 */
		public void addExecTime(double execTime) {
			this.execTime += execTime;
			++this.numWaves;
		}

		@Override
		public int compareTo(TaskSlot other) {
			if (this.execTime < other.execTime)
				return -1;
			else if (this.execTime > other.execTime)
				return 1;
			else
				return 0;
		}

	}

}
