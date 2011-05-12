package edu.duke.starfish.profile.test;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRCleanupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRCleanupInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRSetupInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import edu.duke.starfish.profile.profileinfo.metrics.Metric;
import edu.duke.starfish.profile.profileinfo.metrics.MetricType;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profileinfo.setup.JobTrackerInfo;
import edu.duke.starfish.profile.profileinfo.setup.MasterHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.RackInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * An InfoManager that randomly generates cluster configurations and MR jobs
 * based on input parameters.
 * 
 * @author hero
 * 
 */
public class TestInfoManager implements IMRInfoManager {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private ClusterConfiguration cluster; // the cluster configuration
	private MRJobInfo jobInfo; // The map reduce job

	private Random r; // A random number generator
	private NumberFormat nf; // A number format

	/**
	 * Constructor - Enter the parameters to randomly generate a cluster
	 * configuration and a MR job
	 * 
	 * @param numRacks
	 *            the number of racks of the cluster
	 * @param numHostsPerRack
	 *            the number of hosts per rack
	 * @param numMapSlots
	 *            the number of map reduce slots per task
	 * @param numReduceSlots
	 *            the number of reduce reduce slots per task
	 * @param numMappers
	 *            the number of mappers for the job
	 * @param numReducers
	 *            the number of reducers for the job
	 */
	public TestInfoManager(int numRacks, int numHostsPerRack, int numMapSlots,
			int numReduceSlots, int numMappers, int numReducers) {

		this.r = new Random();
		this.nf = NumberFormat.getIntegerInstance();

		this.cluster = generateClusterConfiguration(numRacks, numHostsPerRack,
				numMapSlots, numReduceSlots);
		this.jobInfo = generateTestMRJob(1, cluster, numMappers, numReducers);
		this.jobInfo
				.addDataTransfers(generateDataTransferInJob((MRJobInfo) jobInfo));

	}

	/* ***************************************************************
	 * OVERRIDDEN METHODS
	 * ***************************************************************
	 */

	@Override
	public List<MRJobInfo> getAllMRJobInfos() {
		List<MRJobInfo> allJobs = new ArrayList<MRJobInfo>(1);
		allJobs.add(jobInfo);
		return allJobs;
	}

	@Override
	public List<MRJobInfo> getAllMRJobInfos(Date start, Date end) {
		if (jobInfo.getStartTime().after(start)
				&& jobInfo.getEndTime().before(end)) {
			return getAllMRJobInfos();
		} else {
			return new ArrayList<MRJobInfo>(0);
		}
	}

	@Override
	public MRJobInfo getMRJobInfo(String execId) {
		// Already loaded
		if (jobInfo.getExecId().equalsIgnoreCase(execId)) {
			return jobInfo;
		} else
			return null;
	}

	@Override
	public ClusterConfiguration getClusterConfiguration(String mrJobId) {
		if (mrJobId.equals(jobInfo.getExecId())) {
			return cluster;
		} else {
			return null;
		}
	}

	@Override
	public boolean loadDataTransfersForMRJob(MRJobInfo mrJob) {
		// Already loaded
		return (mrJob == jobInfo);
	}

	@Override
	public boolean loadTaskDetailsForMRJob(MRJobInfo mrJob) {
		// Already loaded
		return (mrJob == jobInfo);
	}

	public boolean loadProfilesForMRJob(MRJobInfo mrJob) {
		// Already loaded
		return (mrJob == jobInfo);
	}

	@Override
	public Configuration getHadoopConfiguration(String mrJobId) {
		return new Configuration(false);
	}

	@Override
	public MRJobProfile getMRJobProfile(String mrJobId) {
		if (mrJobId.equals(jobInfo.getExecId()))
			return jobInfo.getProfile();

		return null;
	}

	@Override
	public List<Metric> getHostMetrics(MetricType type, HostInfo host,
			Date start, Date end) {

		switch (type) {
		case CPU:
			return generateMetrics(start, end, 0.3, 0.8);
		case MEMORY:
			return generateMetrics(start, end, 500, 1200);
		case DISK_READS:
			return generateMetrics(start, end, 100, 500);
		case DISK_WRITES:
			return generateMetrics(start, end, 100, 500);
		case NET_IN:
			return generateMetrics(start, end, 100, 500);
		case NET_OUT:
			return generateMetrics(start, end, 100, 500);
		default:
			break;
		}

		return null;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Creates and returns a cluster configuration for testing purposes. The
	 * cluster will contain 'numRacks' racks, each with 'numHostsPerRack' slave
	 * hosts. One task tracker with the specified numMapSlots and numReduceSlots
	 * will be created for each slave host. A master host will be included in
	 * the first rack, which will also host the job tracker.
	 * 
	 * @param numRacks
	 *            the number of racks of the cluster
	 * @param numHostsPerRack
	 *            the number of hosts per rack
	 * @param numMapSlots
	 *            the number of map reduce slots per task
	 * @param numReduceSlots
	 *            the number of reduce reduce slots per task
	 * @return a cluster configuration
	 */
	private ClusterConfiguration generateClusterConfiguration(int numRacks,
			int numHostsPerRack, int numMapSlots, int numReduceSlots) {

		// Set properties for number format
		nf.setMinimumIntegerDigits(3);

		// Create the cluster configuration
		ClusterConfiguration cluster = new ClusterConfiguration();

		for (int rackId = 1; rackId <= numRacks; ++rackId) {
			// Create the rack and add it to the cluster
			RackInfo rack = new RackInfo(rackId, "rack_" + nf.format(rackId));
			cluster.addRackInfo(rack);

			// Create the master host and job tracker on the first rack
			if (rackId == 1) {
				MasterHostInfo host = new MasterHostInfo(0,
						"hadoop.cs.duke.edu", "127.0.0.1:10" + nf.format(0),
						rack.getName());
				JobTrackerInfo tracker = new JobTrackerInfo(0, "job_tracker_"
						+ host.getName(), host.getName(), 50060);
				cluster.addMasterHostInfo(host);
				cluster.addJobTrackerInfo(tracker);
			}

			// Generate the hosts for each rack
			for (int hostId = 1; hostId <= numHostsPerRack; ++hostId) {

				int id = (rackId - 1) * numHostsPerRack + hostId;

				// Create one host and task tracker
				SlaveHostInfo host = new SlaveHostInfo(id, "rack"
						+ nf.format(rackId) + ".hadoop" + nf.format(hostId)
						+ ".cs.duke.edu", "127.0.0.1:10" + nf.format(id), rack
						.getName());
				TaskTrackerInfo tracker = new TaskTrackerInfo(id,
						"task_tracker_" + host.getName(), host.getName(),
						50060, numMapSlots, numReduceSlots, 200l << 20);

				// Add them to the cluster
				cluster.addSlaveHostInfo(host);
				cluster.addTaskTrackerInfo(tracker);
			}
		}

		return cluster;
	}

	/**
	 * Get a Map-Reduce job for testing purposes. The job will contain one setup
	 * task, one cleanup task, 'numMappers' map tasks, and 'numReducers' reduce
	 * tasks. Each task will have one successful attempt. The task tracker for
	 * each task attempt is chosen randomly from the list of task trackers in
	 * the cluster. All timings are also generated randomly.
	 * 
	 * Note: The input 'jobId' is also used as the seed to the random generator
	 * to ensure the same data is generated every time the function is called
	 * with the same parameters.
	 * 
	 * Note: The timings might not be realistic.
	 * 
	 * @param jobId
	 *            the job id (also used as seed to the random generator)
	 * @param cluster
	 *            the cluster configuration
	 * @param numMappers
	 *            the number of mappers for the job
	 * @param numReducers
	 *            the number of reducers for the job
	 * @return a Map-Reduce job
	 */
	private MRJobInfo generateTestMRJob(int jobId,
			ClusterConfiguration cluster, int numMappers, int numReducers) {

		// Set properties for number format and random generator
		r.setSeed(jobId);
		nf.setGroupingUsed(false);
		nf.setMinimumIntegerDigits(4);

		// Create the job
		String id = nf.format(jobId);
		Date startTime = new Date(1271077200035L); // 2010-04-12 09:00:00 EDT
		MRJobInfo job = new MRJobInfo(jobId, "job_" + id, startTime, null,
				MRExecutionStatus.SUCCESS, null, "test_job_" + id, "user");

		// Get a list of all the trackers
		ArrayList<SlaveHostInfo> hosts = new ArrayList<SlaveHostInfo>(cluster
				.getAllSlaveHostInfos());

		// Create the setup task
		Date setupEndTime = new Date(
				(long) (r.nextDouble() * 5 * 1000 + startTime.getTime()));
		job.addSetupTaskInfo(createTestSetupInfo(id, startTime, setupEndTime,
				hosts.get(0)));

		// Create all the map tasks
		generateMapReduceTasks(job, id, numMappers, hosts, true);

		// Create all the reduce tasks
		generateMapReduceTasks(job, id, numReducers, hosts, false);

		// Create the cleanup task
		Date cleanupStartTime = (numReducers == 0) ? getLastMapEndTime(job
				.getMapTasks()) : getLastReduceEndTime(job.getReduceTasks());
		Date cleanupEndTime = new Date(
				(long) (r.nextDouble() * 5 * 1000 + cleanupStartTime.getTime()));
		job.addCleanupTaskInfo(createTestCleanupInfo(id, cleanupStartTime,
				cleanupEndTime, hosts.get(0)));

		// Set the job end time
		job.setEndTime(cleanupEndTime);

		return job;
	}

	/**
	 * Generate the data transfer within a map reduce job. For each map task,
	 * this method randomly splits the number of output bytes from the map
	 * attempt to the most of the reducers. The data transfers are stored in a
	 * cache.
	 * 
	 * Note: If the job contains no reducer tasks, no data transfer occurs.
	 * 
	 * Assumptions: Each map and reduce task contains exactly 1 (successful)
	 * task attempt.
	 * 
	 * @param job
	 *            the job containing the map and reduce tasks
	 */
	private List<DataTransfer> generateDataTransferInJob(MRJobInfo job) {

		// Add the data transfer list in the cache
		int numRed = job.getReduceTasks().size();
		List<DataTransfer> dataTransfers = new ArrayList<DataTransfer>(numRed);

		// Make sure we have at least one reducer
		if (numRed == 0) {
			return dataTransfers;
		}

		// Generate the data transfers
		long data = 0l;
		for (MRMapInfo map : job.getMapTasks()) {

			// Create a data transfer from this map to the reducers
			MRMapAttemptInfo mapAttempt = map.getAttempts().get(0);

			for (int i = 0; i < numRed; ++i) {
				MRReduceInfo reduce = job.getReduceTasks().get(i);
				MRReduceAttemptInfo redAttempt = reduce.getAttempts().get(0);

				// Only send data to 75% of the reducers
				if (r.nextDouble() < 0.75) {
					data = Math.abs(r.nextLong() % 100000);
					dataTransfers.add(new DataTransfer(mapAttempt, redAttempt,
							data, data));
				}
			}
		}

		return dataTransfers;
	}

	/**
	 * Create information about the setup task.
	 * 
	 * @param jobId
	 *            the job id
	 * @param startTime
	 *            the setup task start time
	 * @param endTime
	 *            the setup task end time
	 * @param host
	 *            the tracker to execute this task
	 * @return a setup task info
	 */
	private MRSetupInfo createTestSetupInfo(String jobId, Date startTime,
			Date endTime, SlaveHostInfo host) {

		// Create the setup task
		MRSetupInfo setup = new MRSetupInfo(0, "task_" + jobId + "_setup",
				startTime, endTime, MRExecutionStatus.SUCCESS, null);

		// Create the setup task attempt
		MRSetupAttemptInfo setupAttempt = new MRSetupAttemptInfo(0, setup
				.getExecId()
				+ "_1", startTime, endTime, MRExecutionStatus.SUCCESS, null,
				host.getTaskTracker());
		setup.addAttempt(setupAttempt);

		return setup;
	}

	/**
	 * Create information about the cleanup task.
	 * 
	 * @param jobId
	 *            the job id
	 * @param startTime
	 *            the cleanup task start time
	 * @param endTime
	 *            the cleanup task end time
	 * @param host
	 *            the host to execute this task
	 * @return a cleanup task info
	 */
	private MRCleanupInfo createTestCleanupInfo(String jobId, Date startTime,
			Date endTime, SlaveHostInfo host) {

		// Create the cleanup task
		MRCleanupInfo cleanup = new MRCleanupInfo(0, "task_" + jobId
				+ "_cleanup", startTime, endTime, MRExecutionStatus.SUCCESS,
				null);

		// Create the cleanup task attempt
		MRCleanupAttemptInfo cleanupAttempt = new MRCleanupAttemptInfo(0,
				cleanup.getExecId() + "_1", startTime, endTime,
				MRExecutionStatus.SUCCESS, null, host.getTaskTracker());
		cleanup.addAttempt(cleanupAttempt);

		return cleanup;
	}

	/**
	 * Create information about a map task.
	 * 
	 * All data access are set to local
	 * 
	 * @param jobId
	 *            the job id
	 * @param mapId
	 *            the map id
	 * @param startTime
	 *            the map start time
	 * @param endTime
	 *            the map end time
	 * @param host
	 *            the host for the map task
	 * @return a map task info
	 */
	private MRMapInfo createTestMapInfo(String jobId, long mapId,
			Date startTime, Date endTime, SlaveHostInfo host) {

		// Create a data local scenario
		List<SlaveHostInfo> hosts = new ArrayList<SlaveHostInfo>(1);
		hosts.add(host);

		// Create the map and a single attempt
		MRMapInfo map = new MRMapInfo(mapId, "task_" + jobId + "_m_"
				+ nf.format(mapId), startTime, endTime,
				MRExecutionStatus.SUCCESS, null, hosts);
		MRMapAttemptInfo mapAttempt = new MRMapAttemptInfo(mapId, map
				.getExecId()
				+ "_1", startTime, endTime, MRExecutionStatus.SUCCESS, null,
				host.getTaskTracker(), DataLocality.DATA_LOCAL);
		map.addAttempt(mapAttempt);

		// Generate some info data
		MRTaskProfile profile = mapAttempt.getProfile();
		profile.addCounter(MRCounter.COMBINE_INPUT_RECORDS, Math.abs(r
				.nextLong() % 100000));
		profile.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, Math.abs(r
				.nextLong() % 100000));
		profile.addCounter(MRCounter.MAP_INPUT_BYTES, Math
				.abs(r.nextLong() % 100000));
		profile.addCounter(MRCounter.MAP_INPUT_RECORDS, Math
				.abs(r.nextLong() % 100000));
		profile.addCounter(MRCounter.MAP_OUTPUT_BYTES, Math
				.abs(r.nextLong() % 100000));
		profile.addCounter(MRCounter.MAP_OUTPUT_RECORDS, Math
				.abs(r.nextLong() % 100000));
		profile.addCounter(MRCounter.SPILLED_RECORDS, Math
				.abs(r.nextLong() % 100000));
		profile.addCounter(MRCounter.HDFS_BYTES_READ, Math
				.abs(r.nextLong() % 100000));

		return map;
	}

	/**
	 * Create information about a reduce task.
	 * 
	 * @param id
	 *            the job id
	 * @param redId
	 *            the reducer id
	 * @param startTime
	 *            the reduce start time
	 * @param endTime
	 *            the reduce end time
	 * @param lastMapEndTime
	 *            the end time of the last map task
	 * @param host
	 *            the task tracker for the reduce task
	 * @return a reduce task info
	 */
	private MRReduceInfo createTestReduceInfo(String id, long redId,
			Date startTime, Date endTime, Date lastMapEndTime,
			SlaveHostInfo host) {

		// The shuffle must end after the last map has ended
		long minEndShuffle = Math.max(startTime.getTime(), lastMapEndTime
				.getTime());

		// Generate the timings for the reducer
		Date shuffle = new Date(
				(long) (0.4 * (endTime.getTime() - minEndShuffle) + minEndShuffle));
		Date sort = new Date(
				(long) (0.7 * (endTime.getTime() - minEndShuffle) + minEndShuffle));

		// Create the reducer and a single attempt
		MRReduceInfo reducer = new MRReduceInfo(redId, "task_" + id + "_r_"
				+ nf.format(redId), startTime, endTime,
				MRExecutionStatus.SUCCESS, null);
		MRReduceAttemptInfo reducerAttempt = new MRReduceAttemptInfo(redId,
				reducer.getExecId() + "_1", startTime, endTime,
				MRExecutionStatus.SUCCESS, null, host.getTaskTracker(),
				shuffle, sort);
		reducer.addAttempt(reducerAttempt);

		// Generate some info data
		MRTaskProfile profile = reducerAttempt.getProfile();
		profile.addCounter(MRCounter.REDUCE_INPUT_GROUPS, Math
				.abs(r.nextLong() % 1000));
		profile.addCounter(MRCounter.REDUCE_SHUFFLE_BYTES, Math.abs(r
				.nextLong() % 100000));
		profile.addCounter(MRCounter.REDUCE_INPUT_RECORDS, Math.abs(r
				.nextLong() % 100000));
		profile.addCounter(MRCounter.REDUCE_OUTPUT_RECORDS, Math.abs(r
				.nextLong() % 100000));
		profile.addCounter(MRCounter.HDFS_BYTES_WRITTEN, Math
				.abs(r.nextLong() % 100000));

		return reducer;
	}

	/**
	 * Generate map/reduce tasks in a realistic way, i.e. the start and end
	 * times for each map/reduce task obey the maximum number of map/reduce
	 * slots for each task tracker.
	 * 
	 * The idea is to generate the tasks in waves. Suppose there are 4 trackers,
	 * 2 map slots per trackers, and want to generate 11 tasks. Then, the
	 * mappers will be allocated in the following way:
	 * <ul>
	 * <li>tracker1_slot1: Map1, Map2</li>
	 * <li>tracker1_slot2: Map3, Map4</li>
	 * <li>tracker2_slot1: Map5, Map6</li>
	 * <li>tracker2_slot2: Map7</li>
	 * <li>tracker3_slot1: Map8</li>
	 * <li>tracker3_slot2: Map9</li>
	 * <li>tracker4_slot1: Map10</li>
	 * <li>tracker4_slot2: Map11</li>
	 * </ul>
	 * 
	 * The same is true of reduce tasks. Also, a reducer must always finish
	 * after the last map has finished
	 * 
	 * @param job
	 *            the map reduce job
	 * @param id
	 *            the job id
	 * @param numTasks
	 *            the number of map tasks to generate
	 * @param hosts
	 *            the collection of hosts to use
	 * @param maps
	 *            true to generate map tasks, false to generate reduce tasks
	 */
	private void generateMapReduceTasks(MRJobInfo job, String id, int numTasks,
			Collection<SlaveHostInfo> hosts, boolean maps) {

		int numTrackers = hosts.size();
		if (numTrackers == 0)
			return;

		// Get the number of map or reduce slots per task
		TaskTrackerInfo firstTracker = hosts.iterator().next().getTaskTracker();
		int numSlotsPerTracker = (maps) ? firstTracker.getNumMapSlots()
				: firstTracker.getNumReduceSlots();

		// Get the last map end time
		Date lastMapEndTime = getLastMapEndTime(job.getMapTasks());

		// Calculate the number of slots and waves
		int totalNumSlots = numTrackers * numSlotsPerTracker;
		int numCompleteWaves = numTasks / totalNumSlots;
		int numSlotsExtraTasks = numTasks % totalNumSlots;

		// Some initialization
		long taskId = 1;
		Iterator<SlaveHostInfo> iterator = hosts.iterator();
		SlaveHostInfo host = iterator.next();

		// Create all the tasks
		int numSlots = Math.min(totalNumSlots, numTasks);
		for (int slot = 0; slot < numSlots; ++slot) {

			// Figure out the number of consecutive tasks
			int numWaves = numCompleteWaves
					+ ((slot < numSlotsExtraTasks) ? 1 : 0);

			// Initialize the times
			Date taskStart = null;
			Date prevEnd = new Date(job.getStartTime().getTime() + 5 * 1000);

			for (int j = 0; j < numWaves; ++j) {
				// Generate the start and end times for the task
				taskStart = new Date(
						(long) (r.nextDouble() * 10 * 1000 + prevEnd.getTime()));
				prevEnd = new Date(
						(long) (r.nextDouble() * 10 * 60 * 1000 + taskStart
								.getTime()));

				// Generate the map or reduce task
				if (maps) {
					job.addMapTaskInfo(createTestMapInfo(id, taskId, taskStart,
							prevEnd, host));
				} else {
					if (prevEnd.before(lastMapEndTime)) {
						// A reducer must always finish after the last map has
						// finished
						prevEnd = new Date(
								(long) (r.nextDouble() * 10 * 60 * 1000 + lastMapEndTime
										.getTime()));
					}
					job.addReduceTaskInfo(createTestReduceInfo(id, taskId,
							taskStart, prevEnd, lastMapEndTime, host));
				}

				++taskId;
			}

			// Move on to the next task tracker
			if ((slot + 1) % numSlotsPerTracker == 0) {
				host = (iterator.hasNext()) ? iterator.next() : null;
			}
		}
	}

	/**
	 * Generates a list of metrics in the interval specified by the start and
	 * end times. The values of the metrics will range between low and high.
	 * 
	 * @param start
	 *            the start time for the metrics
	 * @param end
	 *            the end time for the metrics
	 * @param low
	 *            the low value for the metric
	 * @param high
	 *            the high value for the metric
	 * @return a list of metrics
	 */
	private List<Metric> generateMetrics(Date start, Date end, double low,
			double high) {
		List<Metric> metrics = new ArrayList<Metric>();

		// Set the interval between 10-20 seconds
		long interval = (long) (r.nextDouble() * 10000 + 10000);

		// Generate the metrics
		long time = start.getTime();
		while (time <= end.getTime()) {
			metrics.add(new Metric(new Date(time), r.nextDouble()
					* (high - low) + low));
			time += interval;
		}

		return metrics;
	}

	/**
	 * Returns the end time of the last map that finished
	 * 
	 * @param maps
	 *            a list of all the map tasks
	 * @return the end time of the last map that finished
	 */
	private Date getLastMapEndTime(List<MRMapInfo> maps) {
		Date maxEndTime = new Date(0);

		for (MRMapInfo map : maps) {
			for (MRMapAttemptInfo mapAttempt : map.getAttempts()) {
				if (mapAttempt.getEndTime().after(maxEndTime)) {
					maxEndTime = mapAttempt.getEndTime();
				}
			}
		}

		return maxEndTime;
	}

	/**
	 * Returns the end time of the last reducer that finished
	 * 
	 * @param reducers
	 *            a list of all the reduce tasks
	 * @return the end time of the last reducer that finished
	 */
	private Date getLastReduceEndTime(List<MRReduceInfo> reducers) {
		Date maxEndTime = new Date(0);

		for (MRReduceInfo reduce : reducers) {
			for (MRReduceAttemptInfo reduceAttempt : reduce.getAttempts()) {
				if (reduceAttempt.getEndTime().after(maxEndTime)) {
					maxEndTime = reduceAttempt.getEndTime();
				}
			}
		}

		return maxEndTime;
	}

}
