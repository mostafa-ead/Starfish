package edu.duke.starfish.profile.profiler;

import static edu.duke.starfish.profile.utils.Constants.HADOOP_COMPLETED_HISTORY;
import static edu.duke.starfish.profile.utils.Constants.HADOOP_HDFS_JOB_HISTORY;
import static edu.duke.starfish.profile.utils.Constants.HADOOP_LOCAL_JOB_HISTORY;
import static edu.duke.starfish.profile.utils.Constants.HADOOP_LOG_DIR;
import static edu.duke.starfish.profile.utils.Constants.MR_JAR;
import static edu.duke.starfish.profile.utils.Constants.MR_JOB_REUSE_JVM;
import static edu.duke.starfish.profile.utils.Constants.MR_MAP_SPECULATIVE_EXEC;
import static edu.duke.starfish.profile.utils.Constants.MR_NUM_SPILLS_COMBINE;
import static edu.duke.starfish.profile.utils.Constants.MR_OUTPUT_DIR;
import static edu.duke.starfish.profile.utils.Constants.MR_RED_IN_BUFF_PERC;
import static edu.duke.starfish.profile.utils.Constants.MR_RED_PARALLEL_COPIES;
import static edu.duke.starfish.profile.utils.Constants.MR_RED_SPECULATIVE_EXEC;
import static edu.duke.starfish.profile.utils.Constants.MR_TASK_PROFILE;
import static edu.duke.starfish.profile.utils.Constants.MR_TASK_PROFILE_MAPS;
import static edu.duke.starfish.profile.utils.Constants.MR_TASK_PROFILE_REDS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profiler.loaders.MRJobHistoryLoader;
import edu.duke.starfish.profile.profiler.loaders.MRTaskProfilesLoader;
import edu.duke.starfish.profile.utils.XMLProfileParser;

/**
 * This class provides static methods for enabling and performing MapReduce job
 * profiling.
 * 
 * @author hero
 */
public class Profiler {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Public constants
	public static final String PROFILER_BTRACE_DIR = "starfish.profiler.btrace.dir";
	public static final String PROFILER_CLUSTER_NAME = "starfish.profiler.cluster.name";
	public static final String PROFILER_OUTPUT_DIR = "starfish.profiler.output.dir";
	public static final String PROFILER_RETAIN_TASK_PROFS = "starfish.profiler.retain.task.profiles";
	public static final String PROFILER_COLLECT_TRANSFERS = "starfish.profiler.collect.data.transfers";
	public static final String PROFILER_SAMPLING_MODE = "starfish.profiler.sampling.mode";
	public static final String PROFILER_SAMPLING_FRACTION = "starfish.profiler.sampling.fraction";

	private static final Log LOG = LogFactory.getLog(Profiler.class);

	private static final Pattern JOB_PATTERN = Pattern
			.compile(".*(job_[0-9]+_[0-9]+).*");

	private static final Pattern TRANSFERS_PATTERN = Pattern
			.compile(".*(Shuffling|Read|Failed).*");

	private static String OLD_MAPPER_CLASS = "mapred.mapper.class";
	private static String OLD_REDUCER_CLASS = "mapred.reducer.class";

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * This method will enable dynamic profiling using the BTraceTaskProfile
	 * script for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "starfish.profiler.btrace.dir"
	 * <li>The code must be using the new Hadoop API
	 * </ol>
	 * 
	 * @param conf
	 *            the configuration describing the current MR job
	 * @return true if profiling has been enabled, false otherwise
	 */
	public static boolean enableExecutionProfiling(Configuration conf) {

		if (enableProfiling(conf)) {
			conf.set("mapred.task.profile.params", "-javaagent:"
					+ "${starfish.profiler.btrace.dir}/btrace-agent.jar="
					+ "dumpClasses=false,debug=false,"
					+ "unsafe=true,probeDescPath=.,noServer=true,"
					+ "script=${starfish.profiler.btrace.dir}/"
					+ "BTraceTaskProfile.class,scriptOutputFile=%s");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method will enable dynamic profiling using the BTraceTaskMemProfile
	 * script for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "starfish.profiler.btrace.dir"
	 * <li>The code must be using the new Hadoop API
	 * </ol>
	 * 
	 * @param conf
	 *            the configuration describing the current MR job
	 * @return true if profiling has been enabled, false otherwise
	 */
	public static boolean enableMemoryProfiling(Configuration conf) {

		if (enableProfiling(conf)) {
			conf.set("mapred.task.profile.params", "-javaagent:"
					+ "${starfish.profiler.btrace.dir}/btrace-agent.jar="
					+ "dumpClasses=false,debug=false,"
					+ "unsafe=true,probeDescPath=.,noServer=true,"
					+ "script=${starfish.profiler.btrace.dir}/"
					+ "BTraceTaskMemProfile.class,scriptOutputFile=%s");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method will enable dynamic profiling using the BTraceTaskProfile
	 * script for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "starfish.profiler.btrace.dir"
	 * <li>The code must be using the new Hadoop API
	 * </ol>
	 * 
	 * @param conf
	 *            the configuration describing the current MR job
	 * @return true if profiling has been enabled, false otherwise
	 */
	private static boolean enableProfiling(Configuration conf) {

		// The user must specify the btrace directory
		if (conf.get(PROFILER_BTRACE_DIR) == null) {
			LOG.warn("The parameter 'starfish.profiler.btrace.dir' "
					+ "is required to enable profiling");
			return false;
		}

		// We only support the new Hadoop API
		if (conf.get(OLD_MAPPER_CLASS) != null
				|| conf.get(OLD_REDUCER_CLASS) != null) {
			LOG.warn("Job profiling is only supported with the new API");
			return false;
		}

		// Set the profiling parameters
		conf.setBoolean(MR_TASK_PROFILE, true);
		conf.set(MR_TASK_PROFILE_MAPS, "0-9999");
		conf.set(MR_TASK_PROFILE_REDS, "0-9999");
		conf.setInt(MR_JOB_REUSE_JVM, 1);
		conf.setInt(MR_NUM_SPILLS_COMBINE, 9999);
		conf.setInt(MR_RED_PARALLEL_COPIES, 1);
		conf.setFloat(MR_RED_IN_BUFF_PERC, 0f);
		conf.setBoolean(MR_MAP_SPECULATIVE_EXEC, false);
		conf.setBoolean(MR_RED_SPECULATIVE_EXEC, false);

		LOG.info("Job profiling enabled");
		return true;
	}

	/**
	 * Gathers the job history files, the task profiles, and data transfers if
	 * requested. Generates the job profile.
	 * 
	 * For more info see
	 * {@link Profiler#gatherJobExecutionFiles(Configuration, File)}.
	 * 
	 * NOTE: This method is to be used ONLY by BTrace scripts.
	 * 
	 * @param conf
	 *            the MapReduce job configuration
	 * @param localDir
	 *            the local output directory
	 */
	public static void gatherJobExecutionFiles(Configuration conf,
			String localDir) {

		// Note: we must surround the entire method to catch all exceptions
		// because BTrace cannot catch them
		try {
			String jobId = Profiler.gatherJobExecutionFiles(conf, new File(
					localDir));
			LOG.info("Job profiling completed for " + jobId);
		} catch (Exception e) {
			LOG.error("Job profiling failed!", e);
		}
	}

	/**
	 * Gathers the job history files, the task profiles, and data transfers if
	 * requested. For more info see
	 * {@link Profiler#gatherJobExecutionFiles(Configuration, File)}.
	 * 
	 * NOTE: This method is to be used ONLY by BTrace scripts.
	 * 
	 * @param conf
	 *            the MapReduce job configuration
	 */
	public static void gatherJobExecutionFiles(Configuration conf) {

		// Note: we must surround the entire method to catch all exceptions
		// because BTrace cannot catch them
		try {
			String localDir = conf.get(Profiler.PROFILER_OUTPUT_DIR);
			String jobId = Profiler.gatherJobExecutionFiles(conf, new File(
					localDir));
			LOG.info("Gathered execution files for " + jobId);
		} catch (Exception e) {
			LOG.error("Unable to gather the execution files!", e);
		}
	}

	/**
	 * Gathers the job history files, the task profiles, and data transfers if
	 * requested. Generates the job profile. The generated directory structure
	 * is:
	 * 
	 * outputDir/history/conf.xml <br />
	 * outputDir/history/history_file <br />
	 * outputDir/task_profiles/task.profile <br />
	 * outputDir/job_profiles/job_profile.xml <br />
	 * outputDir/transfers/transfer <br />
	 * 
	 * @param conf
	 *            the MapReduce job configuration
	 * @param outputDir
	 *            the local output directory
	 * @return the job id
	 * @throws IOException
	 */
	public static String gatherJobExecutionFiles(Configuration conf,
			File outputDir) throws IOException {

		// Validate the results directory
		outputDir.mkdirs();
		if (!outputDir.isDirectory()) {
			throw new IOException("Not a valid directory "
					+ outputDir.toString());
		}

		// Gather the history files
		File historyDir = new File(outputDir, "history");
		historyDir.mkdir();
		File[] historyFiles = gatherJobHistoryFiles(conf, historyDir);

		// Load the history information into a job info
		MRJobHistoryLoader historyLoader = new MRJobHistoryLoader(
				historyFiles[0].getAbsolutePath(),
				historyFiles[1].getAbsolutePath());
		MRJobInfo mrJob = historyLoader.getMRJobInfoWithDetails();
		String jobId = mrJob.getExecId();

		if (conf.getBoolean(MR_TASK_PROFILE, false)) {

			// Gather the profile files
			File taskProfDir = new File(outputDir, "task_profiles");
			taskProfDir.mkdir();
			gatherJobProfileFiles(mrJob, taskProfDir);

			// Export the job profile XML file
			File jobProfDir = new File(outputDir, "job_profiles");
			jobProfDir.mkdir();
			File profileXML = new File(jobProfDir, "profile_" + jobId + ".xml");
			exportProfileXMLFile(mrJob, conf, taskProfDir, profileXML);

			// Remove the task profiles if requested
			if (!conf.getBoolean(PROFILER_RETAIN_TASK_PROFS, true)) {
				for (File file : listTaskProfiles(jobId, taskProfDir)) {
					file.delete();
				}
				taskProfDir.delete();
			}
		}

		// Get the data transfers if requested
		if (conf.getBoolean(PROFILER_COLLECT_TRANSFERS, false)) {
			File transfersDir = new File(outputDir, "transfers");
			transfersDir.mkdir();
			gatherJobTransferFiles(mrJob, transfersDir);
		}

		return jobId;
	}

	/**
	 * Copies the two history files (conf and stats) from the Hadoop history
	 * directory of the job (local or on HDFS) to the provided local history
	 * directory.
	 * 
	 * This method returns an array of two files, corresponding to the local
	 * conf and stats files respectively.
	 * 
	 * @param conf
	 *            the Hadoop configuration
	 * @param historyDir
	 *            the local history directory
	 * @return the conf and stats files
	 * @throws IOException
	 */
	public static File[] gatherJobHistoryFiles(Configuration conf,
			File historyDir) throws IOException {

		// Create the local directory
		historyDir.mkdirs();
		if (!historyDir.isDirectory()) {
			throw new IOException("Not a valid results directory "
					+ historyDir.toString());
		}

		// Get the HDFS Hadoop history directory
		Path hdfsHistoryDir = null;
		String outDir = conf.get(HADOOP_HDFS_JOB_HISTORY,
				conf.get(MR_OUTPUT_DIR));
		if (outDir != null && !outDir.equals("none")) {

			hdfsHistoryDir = new Path(new Path(outDir), "_logs"
					+ Path.SEPARATOR + "history");

			// Copy the history files
			File[] localFiles = copyHistoryFiles(conf, hdfsHistoryDir,
					historyDir);
			if (localFiles != null)
				return localFiles;
		}

		// Get the local Hadoop history directory (Hadoop v0.20.2)
		String localHistory = conf
				.get(HADOOP_LOCAL_JOB_HISTORY,
						"file:///"
								+ new File(System.getProperty(HADOOP_LOG_DIR))
										.getAbsolutePath() + File.separator
								+ "history");
		Path localHistoryDir = new Path(localHistory);

		// Copy the history files
		File[] localFiles = copyHistoryFiles(conf, localHistoryDir, historyDir);
		if (localFiles != null)
			return localFiles;

		// Get the local Hadoop history directory (Hadoop v0.20.203)
		String doneLocation = conf.get(HADOOP_COMPLETED_HISTORY);
		if (doneLocation == null)
			doneLocation = new Path(localHistoryDir, "done").toString();

		// Build the history location pattern. Example:
		// history/done/version-1/localhost_1306866807968_/2011/05/31/000000
		String localHistoryPattern = doneLocation
				+ "/version-[0-9]/*_/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/*";

		FileSystem fs = FileSystem.getLocal(conf);
		FileStatus[] status = fs.globStatus(new Path(localHistoryPattern));
		if (status != null && status.length > 0) {
			for (FileStatus stat : status) {
				// Copy the history files
				localFiles = copyHistoryFiles(conf, stat.getPath(), historyDir);
				if (localFiles != null)
					return localFiles;
			}
		}

		// Unable to copy the files
		throw new IOException("Unable to find history files in directories "
				+ localHistoryDir.toString() + " or "
				+ hdfsHistoryDir.toString());
	}

	/**
	 * Gathers the task profile files into the provided profiles directory.
	 * 
	 * This method will first look into the working directory for the task
	 * profile files. Hadoop will place them here when a job completes, if the
	 * user used the waitForCompletion method to submit the job. In this case,
	 * this method simply moves all the profile files from the working directory
	 * to the provided profiles directory.
	 * 
	 * If the files are not found in the working directory, the method will
	 * contact all task trackers and download all the task profile files.
	 * 
	 * @param mrJob
	 *            The MapReduce job info
	 * @param profilesDir
	 *            The profiles directory
	 * @throws IOException
	 */
	public static void gatherJobProfileFiles(MRJobInfo mrJob, File profilesDir)
			throws IOException {

		// Check for a valid destination directory
		profilesDir.mkdirs();
		if (!profilesDir.isDirectory()) {
			throw new IOException("Not a valid directory "
					+ profilesDir.getAbsolutePath());
		}

		// Get the current directory as the source directory
		File srcDir = new File(System.getProperty("user.dir"));

		// Move the profile files to the new directory
		boolean foundProfiles = false;
		for (File file : listTaskProfiles(mrJob.getExecId(), srcDir)) {
			if (!file.renameTo(new File(profilesDir, file.getName()))) {
				throw new IOException("Unable to move the file  "
						+ file.toString());
			}
			foundProfiles = true;
		}

		if (!foundProfiles) {
			// Download the profiles from the cluster
			for (MRMapAttemptInfo attempt : mrJob
					.getMapAttempts(MRExecutionStatus.SUCCESS)) {
				downloadTaskProfile(attempt, profilesDir);
			}
			for (MRReduceAttemptInfo attempt : mrJob
					.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
				downloadTaskProfile(attempt, profilesDir);
			}
		}
	}

	/**
	 * Collects all the transfer files (subset of the reducer's syslog files)
	 * from the task trackers and stores them in the provided transfers
	 * directory.
	 * 
	 * @param mrJob
	 *            The MapReduce job
	 * @param transfersDir
	 *            The transfers directory
	 * @throws IOException
	 */
	public static void gatherJobTransferFiles(MRJobInfo mrJob, File transfersDir)
			throws IOException {

		for (MRReduceAttemptInfo attempt : mrJob
				.getReduceAttempts(MRExecutionStatus.SUCCESS)) {

			// Open the connection to the syslog
			HttpURLConnection connection = openHttpTaskLogConnection(attempt,
					"syslog");

			// Get the input stream
			BufferedReader input = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));

			// Create the output transfer file
			File transfer = new File(transfersDir, "transfers_"
					+ attempt.getExecId());
			BufferedWriter output = new BufferedWriter(new FileWriter(transfer));

			try {
				// Copy the relevant data
				String logData = null;
				while ((logData = input.readLine()) != null) {
					if (TRANSFERS_PATTERN.matcher(logData).matches()) {
						output.write(logData + "\n");
						output.flush();
					}
				}
			} finally {
				input.close();
			}
		}

	}

	/**
	 * Given the conf, history, and profile files, this method will load all the
	 * necessary information, generate the job profile, and export it as an XML
	 * file.
	 * 
	 * @param mrJob
	 *            the job info
	 * @param conf
	 *            the job configuration
	 * @param profilesDir
	 *            the job profiles directory
	 * @param profileXML
	 *            the output XML profile file
	 */
	public static void exportProfileXMLFile(MRJobInfo mrJob,
			Configuration conf, File profilesDir, File profileXML) {

		// Parse the profile files and get the job profile
		MRTaskProfilesLoader profileLoader = new MRTaskProfilesLoader(mrJob,
				conf, profilesDir.getAbsolutePath());

		// Export the job profile
		if (profileLoader.loadExecutionProfile(mrJob)) {

			// Get the cluster name, if any
			MRJobProfile profile = mrJob.getOrigProfile();
			String clusterName = conf.get(PROFILER_CLUSTER_NAME);
			if (clusterName != null)
				profile.setClusterName(clusterName);

			XMLProfileParser.exportJobProfile(profile, profileXML);
		} else {
			LOG.error("Unable to create the job profile for "
					+ mrJob.getExecId());
		}
	}

	/**
	 * Builds and returns the job identifier (e.g., job_23412331234_1234)
	 * 
	 * @param conf
	 *            the Hadoop configuration of the job
	 * @return the job identifier
	 */
	public static String getJobId(Configuration conf) {
		String jar = conf.get(MR_JAR);

		Matcher matcher = JOB_PATTERN.matcher(jar);
		if (matcher.find()) {
			return matcher.group(1);
		}

		return "job_";
	}

	/**
	 * Loads system properties common to profiling, job analysis, what-if
	 * analysis, and optimization. The system properties are set in the
	 * bin/config.sh script.
	 * 
	 * @param conf
	 *            the configuration
	 */
	public static void loadCommonSystemProperties(Configuration conf) {

		// The BTrace directory for the task profiling
		if (conf.get(Profiler.PROFILER_BTRACE_DIR) == null)
			conf.set(Profiler.PROFILER_BTRACE_DIR,
					System.getProperty(Profiler.PROFILER_BTRACE_DIR));

		// The cluster name
		if (conf.get(Profiler.PROFILER_CLUSTER_NAME) == null)
			conf.set(Profiler.PROFILER_CLUSTER_NAME,
					System.getProperty(Profiler.PROFILER_CLUSTER_NAME));

		// The output directory for the result files
		if (conf.get(Profiler.PROFILER_OUTPUT_DIR) == null)
			conf.set(Profiler.PROFILER_OUTPUT_DIR,
					System.getProperty(Profiler.PROFILER_OUTPUT_DIR));
	}

	/**
	 * Loads system properties related to profiling into the Hadoop
	 * configuration. The system properties are set in the bin/config.sh script.
	 * 
	 * @param conf
	 *            the configuration
	 */
	public static void loadProfilingSystemProperties(Configuration conf) {

		// Load the common system properties
		loadCommonSystemProperties(conf);

		// The sampling mode (off, profiles, or tasks)
		if (conf.get(Profiler.PROFILER_SAMPLING_MODE) == null
				&& System.getProperty(Profiler.PROFILER_SAMPLING_MODE) != null)
			conf.set(Profiler.PROFILER_SAMPLING_MODE,
					System.getProperty(Profiler.PROFILER_SAMPLING_MODE));

		// The sampling fraction
		if (conf.get(Profiler.PROFILER_SAMPLING_FRACTION) == null
				&& System.getProperty(Profiler.PROFILER_SAMPLING_FRACTION) != null)
			conf.set(Profiler.PROFILER_SAMPLING_FRACTION,
					System.getProperty(Profiler.PROFILER_SAMPLING_FRACTION));

		// Flag to retain the task profiles
		if (conf.get(Profiler.PROFILER_RETAIN_TASK_PROFS) == null
				&& System.getProperty(Profiler.PROFILER_RETAIN_TASK_PROFS) != null)
			conf.set(Profiler.PROFILER_RETAIN_TASK_PROFS,
					System.getProperty(Profiler.PROFILER_RETAIN_TASK_PROFS));

		// Flag to collect the data transfers
		if (conf.get(Profiler.PROFILER_COLLECT_TRANSFERS) == null
				&& System.getProperty(Profiler.PROFILER_COLLECT_TRANSFERS) != null)
			conf.set(Profiler.PROFILER_COLLECT_TRANSFERS,
					System.getProperty(Profiler.PROFILER_COLLECT_TRANSFERS));
	}

	/**
	 * Build and return the URL to the log file for this particular task
	 * attempt.
	 * 
	 * Valid values for logFile: stdout, stderr, syslog, profile
	 * 
	 * NOTE: Hadoop 0.20.2 uses taskid in the URL whereas Hadoop 0.20.203.0 uses
	 * attemptid. The boolean useAttemptId is used to support both versions.
	 * 
	 * @param attempt
	 *            the task attempt
	 * @param logFile
	 *            the log file of interest
	 * @param useAttemptId
	 *            whether to use attemptid or taskid
	 * @return the URL to the log file
	 * @throws IOException
	 */
	private static URL buildHttpTaskLogUrl(MRTaskAttemptInfo attempt,
			String logFile, boolean useAttemptId) throws IOException {

		// Build the HTTP task log URL
		StringBuilder httpTaskLog = new StringBuilder();
		httpTaskLog.append("http://");
		httpTaskLog.append(attempt.getTaskTracker().getHostName());
		httpTaskLog.append(":");
		httpTaskLog.append(attempt.getTaskTracker().getPort());
		if (useAttemptId)
			httpTaskLog.append("/tasklog?plaintext=true&attemptid=");
		else
			httpTaskLog.append("/tasklog?plaintext=true&taskid=");
		httpTaskLog.append(attempt.getExecId());
		httpTaskLog.append("&filter=");
		httpTaskLog.append(logFile);

		// Open the connection and get the input stream
		return new URL(httpTaskLog.toString());
	}

	/**
	 * Copy the history files from Hadoop (local or HDFS) to the local directory
	 * 
	 * @param conf
	 *            the Hadoop configuration
	 * @param hadoopHistoryDir
	 *            the Hadoop history directory (local or on HDFS) to copy from
	 * @param localHistoryDir
	 *            the local history directory to copy to
	 * @return the two copied files
	 * @throws IOException
	 */
	private static File[] copyHistoryFiles(Configuration conf,
			Path hadoopHistoryDir, File localHistoryDir) throws IOException {

		// Ensure the Hadoop history dir exists
		FileSystem fs = hadoopHistoryDir.getFileSystem(conf);
		if (!fs.exists(hadoopHistoryDir)) {
			return null;
		}

		// Get the two job files
		final String jobId = getJobId(conf);
		Path[] jobFiles = FileUtil.stat2Paths(fs.listStatus(hadoopHistoryDir,
				new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return path.getName().contains(jobId);
					}
				}));

		if (jobFiles.length != 2) {
			return null;
		}

		// Copy the history files to the local directory
		File[] localJobFiles = new File[2];
		for (Path jobFile : jobFiles) {
			File localJobFile = new File(localHistoryDir, jobFile.getName());
			FileUtil.copy(fs, jobFile, localJobFile, false, conf);

			if (localJobFile.getName().endsWith(".xml"))
				localJobFiles[0] = localJobFile;
			else
				localJobFiles[1] = localJobFile;
		}

		return localJobFiles;
	}

	/**
	 * Download a task profile from the cluster to the provided profiles
	 * directory
	 * 
	 * @param attempt
	 *            the task attempt
	 * @param profilesDir
	 *            the profiles directory to place the file
	 * @throws IOException
	 */
	private static void downloadTaskProfile(MRTaskAttemptInfo attempt,
			File profilesDir) throws IOException {
		HttpURLConnection connection = openHttpTaskLogConnection(attempt,
				"profile");

		if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
			InputStream in = connection.getInputStream();
			OutputStream out = new FileOutputStream(new File(profilesDir,
					attempt.getExecId() + ".profile"));
			IOUtils.copyBytes(in, out, 64 * 1024, true);
		}
	}

	/**
	 * Returns an array of task profile files found in the provided directory
	 * for the particular MapReduce job
	 * 
	 * @param jobId
	 *            the MapReduce job id
	 * @param dir
	 *            the directory
	 * @return the task profile files
	 */
	private static File[] listTaskProfiles(String jobId, File dir) {

		// List all relevant files
		final String strippedJobId = jobId.substring(4);
		File[] files = dir.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.isFile() && !pathname.isHidden()
						&& pathname.getName().contains(strippedJobId)
						&& pathname.getName().endsWith(".profile");
			}
		});

		return files;
	}

	/**
	 * Opens an HTTP URL connection to the requested logFile for the particular
	 * task attempt.
	 * 
	 * Valid values for logFile: stdout, stderr, syslog, profile
	 * 
	 * If the log file does not exist, the connection's response code will be
	 * HttpURLConnection.HTTP_BAD_REQUEST
	 * 
	 * @param attempt
	 *            the task attempt
	 * @param logFile
	 *            the log file of interest
	 * @return the HTTP URL connection to the log file
	 * @throws IOException
	 */
	private static HttpURLConnection openHttpTaskLogConnection(
			MRTaskAttemptInfo attempt, String logFile) throws IOException {

		// Get the URL to the log file and open the connection
		URL taskLogUrl = buildHttpTaskLogUrl(attempt, logFile, false);
		HttpURLConnection connection = (HttpURLConnection) taskLogUrl
				.openConnection();

		if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
			// This case handles backwards incompatibility introduced with
			// Hadoop 0.20.203.0
			taskLogUrl = buildHttpTaskLogUrl(attempt, logFile, true);
			connection = (HttpURLConnection) taskLogUrl.openConnection();
		}

		return connection;
	}

}
