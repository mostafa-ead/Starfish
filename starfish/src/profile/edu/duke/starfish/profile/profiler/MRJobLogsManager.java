package edu.duke.starfish.profile.profiler;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.metrics.Metric;
import edu.duke.starfish.profile.profileinfo.metrics.MetricType;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profiler.loaders.MRJobProfileLoader;
import edu.duke.starfish.profile.profiler.loaders.MRTaskProfilesLoader;
import edu.duke.starfish.profile.profiler.loaders.MRJobTransfersLoader;
import edu.duke.starfish.profile.profiler.loaders.MRJobHistoryLoader;

/**
 * A manager for MR job log files. Given the location of the history, userlogs,
 * and profiles directories, this manager is responsible for parsing the files
 * and building the object representation for the different jobs and cluster
 * configurations.
 * 
 * NOTE: The history files will completely populate the MRJobInfo objects. The
 * userlogs are used to get the data transfers and they might also contain the
 * profiles. Alternatively, the profiles could be located in their own
 * directory.
 * 
 * @author hero
 */
public class MRJobLogsManager implements IMRInfoManager {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// DATA MEMBERS
	private String historyDir; // The directory with the history files
	private String userlogsDir; // The directory with the user logs
	private String jobProfilesDir; // The directory with the XML job profiles
	private String taskProfilesDir; // The directory with the task profiles
	private String transfersDir; // The directory with the transfers

	private Map<String, MRJobHistoryLoader> jobHistories; // The job histories
	private Map<String, MRJobProfileLoader> jobProfiles; // The XML job profiles
	private Map<String, MRTaskProfilesLoader> taskProfiles; // The task profiles
	private Map<String, MRJobTransfersLoader> jobTransfers; // The job transfers

	// CONSTANTS
	private static final String U_JOB_U = "_job_";
	private static final String DOT_XML = ".xml";
	private static final String USCORE = "_";
	private static final Pattern NAME_PATTERN = Pattern
			.compile(".*_job_[0-9]+_[0-9]+_.*");

	/**
	 * Default Constructor
	 */
	public MRJobLogsManager() {

		this.historyDir = null;
		this.userlogsDir = null;
		this.jobProfilesDir = null;
		this.taskProfilesDir = null;
		this.transfersDir = null;
		this.jobHistories = new HashMap<String, MRJobHistoryLoader>();
		this.jobProfiles = new HashMap<String, MRJobProfileLoader>();
		this.taskProfiles = new HashMap<String, MRTaskProfilesLoader>();
		this.jobTransfers = new HashMap<String, MRJobTransfersLoader>();
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @param historyDir
	 *            the historyDir to set
	 */
	public void setHistoryDir(String historyDir) {
		this.historyDir = historyDir;
		readHistoryDirectory();
	}

	/**
	 * @param userlogsDir
	 *            the userlogsDir to set
	 */
	public void setUserlogsDir(String userlogsDir) {
		this.userlogsDir = userlogsDir;
		if (taskProfilesDir == null)
			setTaskProfilesDir(this.userlogsDir);
		if (transfersDir == null)
			setTransfersDir(this.userlogsDir);
	}

	/**
	 * @param jobProfilesDir
	 *            the jobProfilesDir to set
	 */
	public void setJobProfilesDir(String jobProfilesDir) {
		this.jobProfilesDir = jobProfilesDir;
	}

	/**
	 * @param taskProfilesDir
	 *            the taskProfilesDir to set
	 */
	public void setTaskProfilesDir(String taskProfilesDir) {
		this.taskProfilesDir = taskProfilesDir;
	}

	/**
	 * @param transfersDir
	 *            the transfersDir to set
	 */
	public void setTransfersDir(String transfersDir) {
		this.transfersDir = transfersDir;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	@Override
	public List<MRJobInfo> getAllMRJobInfos() {
		// Get all the jobs populated with their summaries
		List<MRJobInfo> jobInfos = new ArrayList<MRJobInfo>();
		for (MRJobHistoryLoader history : jobHistories.values()) {
			jobInfos.add(history.getMRJobInfoWithSummary());
		}

		// Sort based on their execution ids
		Collections.sort(jobInfos, new Comparator<MRJobInfo>() {
			public int compare(MRJobInfo o1, MRJobInfo o2) {
				return o1.getExecId().compareTo(o2.getExecId());
			}
		});

		return jobInfos;
	}

	@Override
	public List<MRJobInfo> getAllMRJobInfos(Date start, Date end) {
		List<MRJobInfo> allJobs = new ArrayList<MRJobInfo>();

		// Get all the jobs within the interval
		for (MRJobInfo job : getAllMRJobInfos()) {
			if (start.before(job.getStartTime()) && end.after(job.getEndTime())) {
				allJobs.add(job);
			}
		}

		return allJobs;
	}

	@Override
	public MRJobInfo getMRJobInfo(String mrJobId) {
		if (jobHistories.containsKey(mrJobId)) {
			return jobHistories.get(mrJobId).getMRJobInfoWithSummary();
		}
		return null;
	}

	@Override
	public ClusterConfiguration getClusterConfiguration(String mrJobId) {
		if (jobHistories.containsKey(mrJobId)) {
			return jobHistories.get(mrJobId).getClusterConfiguration();
		}
		return null;
	}

	@Override
	public Configuration getHadoopConfiguration(String mrJobId) {
		if (jobHistories.containsKey(mrJobId)) {
			return jobHistories.get(mrJobId).getHadoopConfiguration();
		}
		return null;
	}

	@Override
	public MRJobProfile getMRJobProfile(String mrJobId) {

		MRJobInfo mrJob = getMRJobInfo(mrJobId);
		if (mrJob != null && loadProfilesForMRJob(mrJob)) {
			return mrJob.getProfile();
		} else {
			return null;
		}
	}

	@Override
	public List<Metric> getHostMetrics(MetricType type, HostInfo host,
			Date start, Date end) {
		// We don't have such data
		return null;
	}

	@Override
	public boolean loadTaskDetailsForMRJob(MRJobInfo mrJob) {
		if (jobHistories.containsKey(mrJob.getExecId())) {
			return jobHistories.get(mrJob.getExecId()).loadMRJobInfoDetails(
					(MRJobInfo) mrJob);
		}
		return false;
	}

	@Override
	public boolean loadDataTransfersForMRJob(MRJobInfo mrJob) {

		// In order to load data transfers, the task details must be loaded
		if (transfersDir == null || !loadTaskDetailsForMRJob(mrJob))
			return false;

		// If this is the first time for this job, create the data transfer
		if (!jobTransfers.containsKey(mrJob.getExecId())) {
			jobTransfers.put(mrJob.getExecId(), new MRJobTransfersLoader(mrJob,
					transfersDir));
		}

		// Return the data transfers
		return jobTransfers.get(mrJob.getExecId()).loadDataTransfers(mrJob);
	}

	@Override
	public boolean loadProfilesForMRJob(MRJobInfo mrJob) {

		// In order to load the profiles, the task details must be loaded
		if (taskProfilesDir == null && jobProfilesDir == null)
			return false;
		if (!loadTaskDetailsForMRJob(mrJob))
			return false;

		String mrJobId = mrJob.getExecId();
		boolean loaded = false;

		// Try to load from the job profiles
		if (jobProfilesDir != null) {
			if (!jobProfiles.containsKey(mrJobId)) {
				// If this is the first time, create the profile loader
				jobProfiles.put(mrJobId, new MRJobProfileLoader(mrJob,
						jobProfilesDir));
			}

			// Load the job profiles
			loaded = jobProfiles.get(mrJobId).loadJobProfile(mrJob);
		}

		// Try to load from the task profiles
		if (taskProfilesDir != null) {
			if (!taskProfiles.containsKey(mrJobId)) {
				// If this is the first time, create the profile loader
				taskProfiles.put(mrJobId, new MRTaskProfilesLoader(mrJob,
						getHadoopConfiguration(mrJobId), taskProfilesDir));
			}

			// Load the task profiles
			loaded = taskProfiles.get(mrJobId).loadExecutionProfile(mrJob);
		}

		return loaded;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Read the files in the history directory and populate the map with the job
	 * histories. Note that the data in the files are not loaded at this time.
	 * The data for each job will be loaded when the user asks to get a
	 * particular job.
	 */
	private void readHistoryDirectory() {
		if (historyDir == null)
			return;

		// Check for a valid directory
		File dir = new File(historyDir);
		if (!dir.isDirectory()) {
			System.err.println(dir.getAbsolutePath() + " is not a directory!");
			return;
		}

		// List all relevant files
		File[] files = dir.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.isFile() && !pathname.isHidden()
						&& NAME_PATTERN.matcher(pathname.getName()).matches();
			}
		});

		// Sort the files based on the filename
		Arrays.sort(files);

		// Go through all the files and pair them up
		File confFile = null;
		File statFile = null;
		for (File file : files) {

			// One file has XML configurations and the other statistics
			if (file.getName().endsWith(DOT_XML)) {
				confFile = file;
			} else {
				statFile = file;
			}

			if (confFile != null && statFile != null) {
				// Found two consecutive files
				String jobId1 = buildJobId(confFile.getName());
				String jobId2 = buildJobId(statFile.getName());

				if (jobId1 != null && jobId2 != null
						&& jobId1.equalsIgnoreCase(jobId2)) {
					// Matchings job Ids => found a valid pair of files
					jobHistories.put(jobId1, new MRJobHistoryLoader(confFile
							.getAbsolutePath(), statFile.getAbsolutePath()));
					confFile = null;
					statFile = null;
				}
			}
		}
	}

	/**
	 * Given a filename, build the job id. Expected format for filename:
	 * 
	 * "<hostName>_<hostStartTime>_job_<jobTrackerStartTime>_<id>_[conf.xml|<user>_<name>]"
	 * 
	 * The constructed job id has the format: "job_<jobTrackerStartTime>_<id>"
	 * 
	 * @param fileName
	 *            the name of the configuration or statistics file
	 * @return the job id
	 */
	private String buildJobId(String fileName) {
		String jobId = null;
		int index = fileName.indexOf(U_JOB_U);

		if (index != -1) {
			// Split on "_" and merge the necessary pieces
			String[] pieces = fileName.substring(index).split(USCORE);
			if (pieces.length > 3) {
				jobId = pieces[1] + USCORE + pieces[2] + USCORE + pieces[3];
			}
		}
		return jobId;
	}

}
