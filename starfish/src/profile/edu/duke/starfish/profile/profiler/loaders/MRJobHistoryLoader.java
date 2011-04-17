package edu.duke.starfish.profile.profiler.loaders;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.Task;
import org.apache.hadoop.mapred.JobHistory.TaskAttempt;
import org.apache.hadoop.util.StringUtils;

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
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRTaskInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.setup.JobTrackerInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * Represents the execution history of a Map-Reduce job in Hadoop. It parses the
 * job configuration and the job statistics file to gather all the information
 * available about the cluster and job execution.
 * 
 * @author hero
 */
@SuppressWarnings("deprecation")
public class MRJobHistoryLoader {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// LOCAL DATA MEMBERS
	private String jobConfFile; // Path to configuration file
	private String jobStatFile; // Path to statistics file
	private MRJobInfo mrJobInfo; // Our MR job object representation
	private ClusterConfiguration cluster; // Our cluster representation
	private Configuration hadoopConf; // Hadoop's job configuration
	private boolean detailedDataLoaded; // Have we loaded all data yet?
	private boolean summaryDataLoaded; // Have we loaded the summary data yet?

	// CONSTANTS
	private static final String TASK_COUNTER_GROUP = "org.apache.hadoop.mapred.Task$Counter";
	private static final String FILE_COUNTER_GROUP = "FileSystemCounters";
	private static final String MAP_TASKS_MAX = "mapred.tasktracker.map.tasks.maximum";
	private static final String RED_TASKS_MAX = "mapred.tasktracker.reduce.tasks.maximum";
	private static final String JOB_TRACKER = "mapred.job.tracker";
	private static final String JOB_TRACKER_PREFIX = "job_tracker_";
	private static final String MASTER_RACK = "/master-rack/";

	private static final String JOB = "JOB";
	private static final String MAP = "MAP";
	private static final String REDUCE = "REDUCE";
	private static final String CLEANUP = "CLEANUP";
	private static final String SETUP = "SETUP";
	private static final String COMMA = ",";
	private static final String EMPTY = "";
	private static final String SLASHES = "//";
	private static final char COLON = ':';
	private static final char EQUALS = '=';

	private static final Pattern pattern = Pattern
			.compile("(\\w+)=\"[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*\"");

	/**
	 * Constructor
	 * 
	 * @param jobConfFile
	 *            path to the job configuration file
	 * @param jobStatFile
	 *            path to the job statistics (history) file
	 * @throws IOException
	 */
	public MRJobHistoryLoader(String jobConfFile, String jobStatFile) {
		this.jobConfFile = jobConfFile;
		this.jobStatFile = jobStatFile;
		this.mrJobInfo = new MRJobInfo();
		this.cluster = new ClusterConfiguration();
		this.hadoopConf = null;
		this.detailedDataLoaded = false;
		this.summaryDataLoaded = false;
	}

	/* ***************************************************************
	 * Public Methods
	 * *************************************************************
	 */

	/**
	 * Returns the cluster configuration. If the data is not loaded in the
	 * cluster object, this method will load the cluster and full job data from
	 * the files.
	 * 
	 * @return the cluster configuration. null if the loading failed
	 */
	public ClusterConfiguration getClusterConfiguration() {
		if (detailedDataLoaded == false) {
			if (!loadJobAndClusterData(this.cluster, this.mrJobInfo)) {
				return null;
			}
		}

		return cluster;
	}

	/**
	 * Returns the MRJobInfo object containing all the task details. If the data
	 * is not already loaded, this method will load the cluster and full job
	 * data from the files.
	 * 
	 * @return the MRJobInfo object. null if the loading failed
	 */
	public MRJobInfo getMRJobInfoWithDetails() {
		if (detailedDataLoaded == false) {
			if (!loadJobAndClusterData(this.cluster, this.mrJobInfo)) {
				return null;
			}
		}

		return mrJobInfo;
	}

	/**
	 * Returns the MRJobInfo object containing only the job's attributes, no
	 * task data. If the data is not already loaded, this method will load the
	 * job's attributes from the statistics file.
	 * 
	 * @return the MRJobInfo object. null if the loading failed
	 */
	public MRJobInfo getMRJobInfoWithSummary() {
		if (summaryDataLoaded == false) {
			if (!loadMRJobSummaryData(this.mrJobInfo)) {
				return null;
			}
		}

		return mrJobInfo;
	}

	/**
	 * Returns the hadoop configuration containing all the hadoop parameters
	 * used during this job execution.
	 * 
	 * @return the hadoop configuration
	 */
	public Configuration getHadoopConfiguration() {
		if (detailedDataLoaded == false) {
			if (!loadJobAndClusterData(this.cluster, this.mrJobInfo)) {
				return null;
			}
		}

		return hadoopConf;
	}

	/**
	 * Load the cluster configuration from the files into the input object
	 * 
	 * @param cluster
	 *            the cluster to load
	 * @return true if successful
	 */
	public boolean loadClusterConfiguration(ClusterConfiguration cluster) {
		if (detailedDataLoaded == true && this.cluster == cluster) {
			// This data has already been loaded
			return true;
		} else {
			// Load all data
			this.cluster = cluster;
			return loadJobAndClusterData(this.cluster, this.mrJobInfo);
		}
	}

	/**
	 * Loads the job's attributes (no task data) from the files into the input
	 * object
	 * 
	 * @param mrJobInfo
	 *            the job to load
	 * @return true if successful
	 */
	public boolean loadMRJobInfoSummary(MRJobInfo mrJobInfo) {
		if (summaryDataLoaded == true && this.mrJobInfo == mrJobInfo) {
			// This data has already been loaded
			return true;
		} else {
			// Load the summary data
			this.mrJobInfo = mrJobInfo;
			return loadMRJobSummaryData(this.mrJobInfo);
		}
	}

	/**
	 * Loads the job's attributes including all the task data from the files
	 * into the input object
	 * 
	 * @param mrJobInfo
	 *            the job to load
	 * @return true if successful
	 */
	public boolean loadMRJobInfoDetails(MRJobInfo mrJobInfo) {
		if (detailedDataLoaded == true && this.mrJobInfo == mrJobInfo) {
			// This data has already been loaded
			return true;
		} else {
			// Load all data
			this.mrJobInfo = mrJobInfo;
			return loadJobAndClusterData(this.cluster, this.mrJobInfo);
		}
	}

	/* ***************************************************************
	 * Private Methods
	 * *************************************************************
	 */

	/**
	 * Loads summary data for the MR job
	 * 
	 * @param mrJobInfo
	 *            the MRJobInfo to populate with the summary data
	 * @return true if successful
	 */
	private boolean loadMRJobSummaryData(MRJobInfo mrJobInfo) {

		// Open the statistics file
		FileReader reader = null;
		try {
			reader = new FileReader(jobStatFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}

		// Read the statistics file
		BufferedReader br = new BufferedReader(reader);
		try {
			String line = br.readLine();
			while (line != null) {

				// Only interested in lines that start with "JOB"
				int idx = line.indexOf(' ');
				if (idx == -1) {
					line = br.readLine();
					continue;
				}
				String recType = line.substring(0, idx);

				if (recType.equalsIgnoreCase(JOB)) {
					// Parse the line with the job attributes
					String data = line.substring(idx + 1, line.length());
					Matcher matcher = pattern.matcher(data);

					while (matcher.find()) {
						// Set each attribute found
						String[] parts = StringUtils.split(matcher.group(0),
								StringUtils.ESCAPE_CHAR, EQUALS);
						String value = parts[1].substring(1,
								parts[1].length() - 1);

						setMRJobAttribute(mrJobInfo, Keys.valueOf(parts[0]),
								value);
					}
				}

				line = br.readLine();
			}

			// Done
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		summaryDataLoaded = true;
		return true;
	}

	/**
	 * Loads the Job and cluster data from the configuration and statistics
	 * files
	 * 
	 * @param cluster
	 *            the cluster configuration object to populate
	 * @param mrJobInfo
	 *            the MRJobInfo object to populate
	 * @return true if successful
	 */
	private boolean loadJobAndClusterData(ClusterConfiguration cluster,
			MRJobInfo mrJobInfo) {

		// Load the configuration file
		hadoopConf = new Configuration();
		hadoopConf.addResource(new Path(jobConfFile));

		// Load the job statistics data
		JobInfo hadoopJob = new JobInfo(EMPTY);
		try {
			DefaultJobHistoryParser.parseJobTasks(jobStatFile, hadoopJob,
					FileSystem.getLocal(hadoopConf));
		} catch (IOException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			return false;
		}

		// Populate the MRJobInfo object and the cluster
		populateClusterAndMRJobInfo(cluster, mrJobInfo, hadoopJob, hadoopConf);
		detailedDataLoaded = true;
		summaryDataLoaded = true;
		return true;
	}

	/**
	 * Populates the ClusterConfiguration and MRJobInfo objects given the
	 * Hadoop's JobInfo and Configuration objects
	 * 
	 * @param cluster
	 *            the cluster configuration
	 * @param mrJobInfo
	 *            the MRJobInfo object to populate
	 * @param job
	 *            the Hadoop representation of the job
	 * @param conf
	 *            the Hadoop representation of the job configurations
	 */
	private void populateClusterAndMRJobInfo(ClusterConfiguration cluster,
			MRJobInfo mrJobInfo, JobInfo job, Configuration conf) {

		// Populate the job's attributes
		for (Map.Entry<Keys, String> entry : job.getValues().entrySet()) {
			setMRJobAttribute(mrJobInfo, entry.getKey(), entry.getValue());
		}

		// Populates all the tasks in this job
		for (Task task : job.getAllTasks().values()) {
			if (task.get(Keys.TASK_TYPE).equalsIgnoreCase(MAP)) {
				// Populate this map task
				MRMapInfo mrMapInfo = new MRMapInfo();
				mrJobInfo.addMapTaskInfo(mrMapInfo);
				populateMRMapInfo(cluster, mrMapInfo, task);

			} else if (task.get(Keys.TASK_TYPE).equalsIgnoreCase(REDUCE)) {
				// Populate this reduce task
				MRReduceInfo mrReduceInfo = new MRReduceInfo();
				mrJobInfo.addReduceTaskInfo(mrReduceInfo);
				populateMRReduceInfo(cluster, mrReduceInfo, task);

			} else if (task.get(Keys.TASK_TYPE).equalsIgnoreCase(SETUP)) {
				// Populate this setup task
				MRSetupInfo mrSetupInfo = new MRSetupInfo();
				mrJobInfo.addSetupTaskInfo(mrSetupInfo);
				populateMRSetupInfo(cluster, mrSetupInfo, task);

			} else if (task.get(Keys.TASK_TYPE).equalsIgnoreCase(CLEANUP)) {
				// Populate this cleanup task
				MRCleanupInfo mrCleanupInfo = new MRCleanupInfo();
				mrJobInfo.addCleanupTaskInfo(mrCleanupInfo);
				populateMRCleanupInfo(cluster, mrCleanupInfo, task);
			}
		}

		// Populate the rest of the cluster
		populateCluster(cluster, conf);
	}

	/**
	 * Populates an MRTaskInfo object (and parts of the cluster) given the
	 * Hadoop's Task object
	 * 
	 * @param cluster
	 *            the cluster configuration
	 * @param mrTaskInfo
	 *            the MRJobInfo object to populate
	 * @param task
	 *            the Hadoop representation of a task
	 */
	private void populateMRTaskInfo(ClusterConfiguration cluster,
			MRTaskInfo mrTaskInfo, Task task) {

		// Populate the common task attributes
		for (Map.Entry<Keys, String> entry : task.getValues().entrySet()) {

			switch (entry.getKey()) {
			case TASKID:
				mrTaskInfo.setExecId(entry.getValue());
				break;
			case START_TIME:
				mrTaskInfo.setStartTime(new Date(Long.parseLong(entry
						.getValue())));
				break;
			case FINISH_TIME:
				mrTaskInfo
						.setEndTime(new Date(Long.parseLong(entry.getValue())));
				break;
			case TASK_STATUS:
				mrTaskInfo.setStatus(MRExecutionStatus
						.valueOf(entry.getValue()));
				break;
			case ERROR:
				mrTaskInfo.setErrorMsg(entry.getValue());
				break;
			default:
				break;
			}
		}
	}

	/**
	 * Populates an MRTaskAttemptInfo object (and parts of the cluster) given
	 * the Hadoop's TaskAttempt object
	 * 
	 * @param cluster
	 *            the cluster configuration
	 * @param mrTaskAttemptInfo
	 *            the MRTaskAttemptInfo object to populate
	 * @param taskAttempt
	 *            the Hadoop representation of a task attempt
	 */
	private void populateMRTaskAttemptInfo(ClusterConfiguration cluster,
			MRTaskAttemptInfo mrTaskAttemptInfo, TaskAttempt taskAttempt) {

		String trackerName = null;
		String fullHostName = null;
		int port = 0;

		// Populate the common task attempt attributes
		for (Map.Entry<Keys, String> entry : taskAttempt.getValues().entrySet()) {

			switch (entry.getKey()) {
			case TASK_ATTEMPT_ID:
				mrTaskAttemptInfo.setExecId(entry.getValue());
				break;
			case START_TIME:
				mrTaskAttemptInfo.setStartTime(new Date(Long.parseLong(entry
						.getValue())));
				break;
			case FINISH_TIME:
				mrTaskAttemptInfo.setEndTime(new Date(Long.parseLong(entry
						.getValue())));
				break;
			case TASK_STATUS:
				mrTaskAttemptInfo.setStatus(MRExecutionStatus.valueOf(entry
						.getValue()));
				break;
			case ERROR:
				mrTaskAttemptInfo.setErrorMsg(entry.getValue());
				break;
			case TRACKER_NAME:
				trackerName = entry.getValue();
				int colIndex = trackerName.indexOf(COLON);
				if (colIndex != -1) {
					trackerName = trackerName.substring(0, colIndex);
				}
				break;
			case HTTP_PORT:
				port = Integer.parseInt(entry.getValue());
				break;
			case HOSTNAME:
				fullHostName = entry.getValue();
				break;
			case COUNTERS:
				parseMRTaskAttemptCounters(mrTaskAttemptInfo.getProfile(),
						entry.getValue());
				break;
			default:
				break;
			}
		}

		// Set the task id in the profile
		mrTaskAttemptInfo.getProfile().setTaskId(mrTaskAttemptInfo.getExecId());

		// Set the task tracker
		if (trackerName != null && fullHostName != null) {
			TaskTrackerInfo tracker = cluster.addFindTaskTrackerInfo(
					trackerName, fullHostName);
			if (tracker != null) {
				tracker.setPort(port);
				mrTaskAttemptInfo.setTaskTracker(tracker);
			}
		}
	}

	/**
	 * Populates an MRMapInfo object (and parts of the cluster) given the
	 * Hadoop's Task object
	 * 
	 * @param cluster
	 *            the cluster configuration
	 * @param mrMapInfo
	 *            the MRMapInfo object to populate
	 * @param task
	 *            the Hadoop representation of a task
	 */
	private void populateMRMapInfo(ClusterConfiguration cluster,
			MRMapInfo mrMapInfo, Task task) {

		// Populate common task attributes
		populateMRTaskInfo(cluster, mrMapInfo, task);

		// Find and create the split hosts
		for (String split : task.get(Keys.SPLITS).split(COMMA)) {
			SlaveHostInfo host = cluster.addFindSlaveHostInfo(split);
			if (host != null) {
				mrMapInfo.addSplitHost(host);
			}
		}

		// Populate all the Map attempts
		for (TaskAttempt taskAttempt : task.getTaskAttempts().values()) {
			MRMapAttemptInfo mrMapAttemptInfo = new MRMapAttemptInfo();
			mrMapInfo.addAttempt(mrMapAttemptInfo);
			populateMRTaskAttemptInfo(cluster, mrMapAttemptInfo, taskAttempt);

			// Determine locality
			SlaveHostInfo host = cluster.addFindSlaveHostInfo(taskAttempt
					.get(Keys.HOSTNAME));
			if (host != null) {
				boolean hostLocal = false;
				boolean rackLocal = false;

				// Go through the splits to match the current host
				for (SlaveHostInfo splitHost : mrMapInfo.getSplitHosts()) {
					if (splitHost.equals(host)) {
						hostLocal = true;
					} else if (splitHost.getRackName().equals(
							host.getRackName())) {
						rackLocal = true;
					}
				}

				// Set the data locality
				if (hostLocal) {
					mrMapAttemptInfo.setDataLocality(DataLocality.DATA_LOCAL);
				} else if (rackLocal) {
					mrMapAttemptInfo.setDataLocality(DataLocality.RACK_LOCAL);
				} else {
					mrMapAttemptInfo.setDataLocality(DataLocality.NON_LOCAL);
				}
			}

		}
	}

	/**
	 * Populates an MRReduceInfo object (and parts of the cluster) given the
	 * Hadoop's Task object
	 * 
	 * @param cluster
	 *            the cluster configuration
	 * @param mrReduceInfo
	 *            the MRReduceInfo object to populate
	 * @param task
	 *            the Hadoop representation of a task
	 */
	private void populateMRReduceInfo(ClusterConfiguration cluster,
			MRReduceInfo mrReduceInfo, Task task) {

		// Populate common task attributes
		populateMRTaskInfo(cluster, mrReduceInfo, task);

		// Populate all the Reduce attempts
		for (TaskAttempt taskAttempt : task.getTaskAttempts().values()) {

			// Populate the attempt
			MRReduceAttemptInfo mrReduceAttemptInfo = new MRReduceAttemptInfo();
			mrReduceInfo.addAttempt(mrReduceAttemptInfo);
			populateMRTaskAttemptInfo(cluster, mrReduceAttemptInfo, taskAttempt);

			// Get the shuffle end time
			if (taskAttempt.getValues().containsKey(Keys.SHUFFLE_FINISHED)) {
				mrReduceAttemptInfo.setShuffleEndTime(new Date(taskAttempt
						.getLong(Keys.SHUFFLE_FINISHED)));
			}

			// Get the sort end time
			if (taskAttempt.getValues().containsKey(Keys.SORT_FINISHED)) {
				mrReduceAttemptInfo.setSortEndTime(new Date(taskAttempt
						.getLong(Keys.SORT_FINISHED)));
			}
		}
	}

	/**
	 * Populates an MRSetupInfo object (and parts of the cluster) given the
	 * Hadoop's Task object
	 * 
	 * @param cluster
	 *            the cluster configuration
	 * @param mrSetupInfo
	 *            the MRSetupInfo object to populate
	 * @param task
	 *            the Hadoop representation of a task
	 */
	private void populateMRSetupInfo(ClusterConfiguration cluster,
			MRSetupInfo mrSetupInfo, Task task) {

		// Populate common task attributes
		populateMRTaskInfo(cluster, mrSetupInfo, task);

		// Populate all the Map attempts
		for (TaskAttempt taskAttempt : task.getTaskAttempts().values()) {
			MRSetupAttemptInfo mrSetupAttemptInfo = new MRSetupAttemptInfo();
			mrSetupInfo.addAttempt(mrSetupAttemptInfo);
			populateMRTaskAttemptInfo(cluster, mrSetupAttemptInfo, taskAttempt);
		}
	}

	/**
	 * Populates an MRCleanupInfo object (and parts of the cluster) given the
	 * Hadoop's Task object
	 * 
	 * @param cluster
	 *            the cluster configuration
	 * @param mrCleanupInfo
	 *            the MRCleanupInfo object to populate
	 * @param task
	 *            the Hadoop representation of a task
	 */
	private void populateMRCleanupInfo(ClusterConfiguration cluster,
			MRCleanupInfo mrCleanupInfo, Task task) {

		// Populate common task attributes
		populateMRTaskInfo(cluster, mrCleanupInfo, task);

		// Populate all the Cleanup attempts
		for (TaskAttempt taskAttempt : task.getTaskAttempts().values()) {
			MRCleanupAttemptInfo mrCleanupAttemptInfo = new MRCleanupAttemptInfo();
			mrCleanupInfo.addAttempt(mrCleanupAttemptInfo);
			populateMRTaskAttemptInfo(cluster, mrCleanupAttemptInfo,
					taskAttempt);
		}
	}

	/**
	 * Populates some aspects of the cluster configuration that was not
	 * populated by the populateClusterAndMRJobInfo() method}
	 * 
	 * @param cluster
	 *            the cluster configuration
	 * @param conf
	 *            the Hadoop configuration object
	 */
	private void populateCluster(ClusterConfiguration cluster,
			Configuration conf) {

		// Set the number of map and reduce slots in all the task trackers
		int numMapSlots = conf.getInt(MAP_TASKS_MAX, 0);
		int numReduceSlots = conf.getInt(RED_TASKS_MAX, 0);
		for (TaskTrackerInfo taskTrackerInfo : cluster
				.getAllTaskTrackersInfos()) {
			taskTrackerInfo.setNumMapSlots(numMapSlots);
			taskTrackerInfo.setNumReduceSlots(numReduceSlots);
		}

		// Create the job tracker
		String jobTracker = conf.get(JOB_TRACKER);
		if (jobTracker != null) {
			// Parse the tracker. Example:
			// hdfs://ip-10-244-18-114.ec2.internal:50002
			int prefix = jobTracker.indexOf(SLASHES);
			prefix = (prefix == -1) ? 0 : (prefix + 2);
			int colIndex = jobTracker.lastIndexOf(COLON);
			colIndex = (colIndex == -1 || colIndex <= prefix) ? jobTracker
					.length() : colIndex;
			String hostName = jobTracker.substring(prefix, colIndex);

			// Create the job tracker
			JobTrackerInfo jobTrackerInfo = cluster.addFindJobTrackerInfo(
					JOB_TRACKER_PREFIX + hostName, MASTER_RACK + hostName);

			// Set the job tracker's port
			if (colIndex < jobTracker.length()) {
				try {
					jobTrackerInfo.setPort(Integer.parseInt(jobTracker
							.substring(colIndex + 1)));
				} catch (Exception e) { // Ignore silently
				}
			}
		}
	}

	/**
	 * Parses a string representing all the counters for a given task attempt
	 * and places the relevant counters in mrTaskAttemptInfo.
	 * 
	 * Expected format for the counters string:
	 * 
	 * {(groupname)(group-displayname)[(countername)(displayname)(value)]*}*
	 * 
	 * @param mrTaskProfile
	 *            the MRTaskAttemptInfo to which to add the counters
	 * @param strCounters
	 *            a string representation of the counters
	 */
	private void parseMRTaskAttemptCounters(MRTaskProfile mrTaskProfile,
			String strCounters) {

		// Parse the counters string
		Counters counters = null;
		try {
			counters = Counters.fromEscapedCompactString(strCounters);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			return;
		}

		// Iterate over all groups - only interested in 2 of them
		for (Group group : counters) {
			if (group.getName().equalsIgnoreCase(TASK_COUNTER_GROUP)
					|| group.getName().equalsIgnoreCase(FILE_COUNTER_GROUP)) {

				// Iterate over counters in group and add them to the attempt
				for (Counter counter : group) {
					try {
						mrTaskProfile.addCounter(MRCounter.valueOf(counter
								.getName()), counter.getValue());
					} catch (Exception e) {
						System.err.println(e.getMessage());
						e.printStackTrace();
					}
				}
			}
		}

	}

	/**
	 * Set the appropriate MR job attribute given the input key and value
	 * 
	 * @param mrJobInfo
	 *            the MRJobInfo to populate
	 * @param key
	 *            the key of the atribute
	 * @param value
	 *            the value of the attribute
	 */
	private void setMRJobAttribute(MRJobInfo mrJobInfo, Keys key, String value) {

		switch (key) {
		case JOBID:
			mrJobInfo.setExecId(value);
			break;
		case JOBNAME:
			mrJobInfo.setName(value);
			break;
		case USER:
			mrJobInfo.setUser(value);
			break;
		case LAUNCH_TIME:
			mrJobInfo.setStartTime(new Date(Long.parseLong(value)));
			break;
		case FINISH_TIME:
			mrJobInfo.setEndTime(new Date(Long.parseLong(value)));
			break;
		case JOB_STATUS:
			mrJobInfo.setStatus(MRExecutionStatus.valueOf(value));
			break;
		case ERROR:
			mrJobInfo.setErrorMsg(value);
			break;
		default:
			break;
		}
	}

}
