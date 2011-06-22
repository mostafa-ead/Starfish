package edu.duke.starfish.profile.profiler;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_JOB_REUSE_JVM;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_MAP_SPECULATIVE_EXEC;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_NUM_SPILLS_COMBINE;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_RED_IN_BUFF_PERC;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_RED_PARALLEL_COPIES;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_RED_SPECULATIVE_EXEC;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_TASK_PROFILE;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_TASK_PROFILE_MAPS;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_TASK_PROFILE_REDS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profiler.loaders.MRJobHistoryLoader;
import edu.duke.starfish.profile.profiler.loaders.MRTaskProfilesLoader;

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
	public static final String BTRACE_PROFILE_DIR = "btrace.profile.dir";
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
	 * This method will enable dynamic profiling using the HadoopBTrace script
	 * for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "btrace.profile.dir"
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
					+ "${btrace.profile.dir}/btrace-agent.jar="
					+ "dumpClasses=false,debug=false,"
					+ "unsafe=true,probeDescPath=.,noServer=true,"
					+ "script=${btrace.profile.dir}/HadoopBTrace.class,"
					+ "scriptOutputFile=%s");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method will enable dynamic profiling using the HadoopBTraceMem
	 * script for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "btrace.profile.dir"
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
					+ "${btrace.profile.dir}/btrace-agent.jar="
					+ "dumpClasses=false,debug=false,"
					+ "unsafe=true,probeDescPath=.,noServer=true,"
					+ "script=${btrace.profile.dir}/HadoopBTraceMem.class,"
					+ "scriptOutputFile=%s");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method will enable dynamic profiling using the HadoopBTrace script
	 * for all the job tasks. There are two requirements for performing
	 * profiling:
	 * <ol>
	 * <li>
	 * The user must specify in advance the location of the script in the
	 * "btrace.profile.dir"
	 * <li>The code must be using the new Hadoop API
	 * </ol>
	 * 
	 * @param conf
	 *            the configuration describing the current MR job
	 * @return true if profiling has been enabled, false otherwise
	 */
	private static boolean enableProfiling(Configuration conf) {

		// The user must specify the btrace directory
		if (conf.get(Profiler.BTRACE_PROFILE_DIR) == null) {
			LOG.warn("The parameter 'btrace.profile.dir' "
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
	 * Gathers the job history files, task profiles, and data transfers. Also
	 * creates the job profile.
	 * 
	 * See detailed comments at
	 * {@link Profiler#gatherJobExecutionFiles(Job, String)}
	 * 
	 * @param job
	 *            the Hadoop job
	 * @param localDir
	 *            the local directory to place the files at
	 * @param retainTaskProfs
	 *            flag to retain the task profiles
	 * @param collectTransfers
	 *            flag to collect the data transfers
	 */
	public static void gatherJobExecutionFiles(Job job, String localDir,
			boolean retainTaskProfs, boolean collectTransfers) {

		job.getConfiguration().setBoolean(Profiler.PROFILER_RETAIN_TASK_PROFS,
				retainTaskProfs);
		job.getConfiguration().setBoolean(Profiler.PROFILER_COLLECT_TRANSFERS,
				collectTransfers);
		Profiler.gatherJobExecutionFiles(job, localDir);
	}

	/**
	 * Gathers the job history files and the task profiles. Generates the job
	 * profile. The generated directory structure is:
	 * 
	 * localDir/history/conf.xml <br />
	 * localDir/history/history_file <br />
	 * localDir/task_profiles/task.profile <br />
	 * localDir/job_profiles/job_profile.xml <br />
	 * 
	 * Assumption: The task profiles are located in the working directory
	 * (placed there by Hadoop when a job completes execution).
	 * 
	 * @param job
	 *            the MapReduce job
	 * @param localDir
	 *            the local output directory
	 * @throws IOException
	 */
	public static void gatherJobExecutionFiles(Job job, String localDir) {

		// Note: we must surround the entire method to catch all exceptions
		// because BTrace cannot catch them
		try {

			// Validate the results directory
			File resultsDir = new File(localDir);
			resultsDir.mkdirs();
			if (!resultsDir.isDirectory()) {
				throw new IOException("Not a valid directory "
						+ resultsDir.toString());
			}

			// Gather the history files
			File historyDir = new File(resultsDir, "history");
			historyDir.mkdir();
			File[] historyFiles = gatherJobHistoryFiles(job, historyDir);

			// Gather the profile files
			File taskProfDir = new File(resultsDir, "task_profiles");
			taskProfDir.mkdir();
			gatherJobProfileFiles(job, taskProfDir);

			// Export the job profile XML file
			String jobId = getJobId(job);
			File jobProfDir = new File(resultsDir, "job_profiles");
			jobProfDir.mkdir();

			File profileXML = new File(jobProfDir, "profile_" + jobId + ".xml");
			MRJobInfo mrJob = exportProfileXMLFile(historyFiles[0],
					historyFiles[1], taskProfDir, profileXML);

			// Remove the task profiles if requested
			Configuration conf = job.getConfiguration();
			if (!conf.getBoolean(PROFILER_RETAIN_TASK_PROFS, true)) {
				for (File file : getTaskProfiles(job, taskProfDir)) {
					file.delete();
				}
				taskProfDir.delete();
			}

			// Get the data transfers if requested
			if (conf.getBoolean(PROFILER_COLLECT_TRANSFERS, false)) {
				File transfersDir = new File(resultsDir, "transfers");
				transfersDir.mkdir();
				gatherJobTransferFiles(mrJob, transfersDir);
			}

			LOG.info("Job profiling completed! Output directory: " + localDir);
		} catch (Exception e) {
			LOG.error("Job profiling failed!", e);
		}
	}

	/**
	 * Copies the two history files (conf and stats) from the
	 * output/_logs/history directory of the job to the provided local history
	 * directory.
	 * 
	 * This method returns an array of two files, corresponding to the local
	 * conf and stats files respectively.
	 * 
	 * @param job
	 *            The MapReduce job
	 * @param historyDir
	 *            the local history directory
	 * @return the conf and stats files
	 * @throws IOException
	 */
	public static File[] gatherJobHistoryFiles(Job job, File historyDir)
			throws IOException {

		// Create the local directory
		historyDir.mkdirs();
		if (!historyDir.isDirectory()) {
			throw new IOException("Not a valid results directory "
					+ historyDir.toString());
		}

		// Get the local Hadoop history directory
		Configuration conf = job.getConfiguration();
		String localHistory = conf
				.get("hadoop.job.history.location", "file:///"
						+ new File(System.getProperty("hadoop.log.dir"))
								.getAbsolutePath() + File.separator + "history");
		Path localHistoryDir = new Path(localHistory);

		// Copy the history files
		File[] localFiles = copyHistoryFiles(job, localHistoryDir, historyDir);
		if (localFiles != null)
			return localFiles;

		// Get the HDFS Hadoop history directory
		String outDir = conf.get("hadoop.job.history.user.location");
		if (outDir == null)
			outDir = conf.get("mapred.output.dir");

		Path output = new Path(outDir);
		Path hdfsHistoryDir = new Path(output, "_logs" + Path.SEPARATOR
				+ "history");

		// Copy the history files
		localFiles = copyHistoryFiles(job, hdfsHistoryDir, historyDir);
		if (localFiles != null)
			return localFiles;

		// Unable to copy the files
		throw new IOException("Unable to find history files in directories "
				+ localHistoryDir.toString() + " or "
				+ hdfsHistoryDir.toString());
	}

	/**
	 * Copy the history files from Hadoop (local or HDFS) to the local directory
	 * 
	 * @param job
	 *            the Hadoop job
	 * @param hadoopHistoryDir
	 *            the Hadoop history directory to copy from
	 * @param localHistoryDir
	 *            the local history directory to copy to
	 * @return the two copied files
	 * @throws IOException
	 */
	private static File[] copyHistoryFiles(Job job, Path hadoopHistoryDir,
			File localHistoryDir) throws IOException {

		// Ensure the Hadoop history dir exists
		Configuration conf = job.getConfiguration();
		FileSystem fs = hadoopHistoryDir.getFileSystem(conf);
		if (!fs.exists(hadoopHistoryDir)) {
			return null;
		}

		// Get the two job files
		final String jobId = getJobId(job);
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
	 * Moves all the profile files from the working directory (placed here by
	 * Hadoop when a job completes) to the provided profiles directory.
	 * 
	 * @param job
	 *            The MapReduce job
	 * @param profilesDir
	 *            The profiles directory
	 * @throws IOException
	 */
	public static void gatherJobProfileFiles(Job job, File profilesDir)
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
		for (File file : getTaskProfiles(job, srcDir)) {
			if (!file.renameTo(new File(profilesDir, file.getName()))) {
				throw new IOException("Unable to move the file  "
						+ file.toString());
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

			// Build the HTTP task log URL
			StringBuilder httpTaskLog = new StringBuilder();
			httpTaskLog.append("http://");
			httpTaskLog.append(attempt.getTaskTracker().getHostName());
			httpTaskLog.append(":");
			httpTaskLog.append(attempt.getTaskTracker().getPort());
			httpTaskLog.append("/tasklog?plaintext=true&taskid="
					+ attempt.getExecId());
			httpTaskLog.append("&filter=syslog");

			// Open the connection and get the input stream
			URL taskLogUrl = new URL(httpTaskLog.toString());
			URLConnection connection = taskLogUrl.openConnection();
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
	 * @param confFile
	 *            the job configuration file
	 * @param historyFile
	 *            the job history file
	 * @param profilesDir
	 *            the job profiles directory
	 * @param profileXML
	 *            the output XML profile file
	 */
	public static MRJobInfo exportProfileXMLFile(File confFile,
			File historyFile, File profilesDir, File profileXML) {

		// Parse the history files and get the MR job info
		MRJobHistoryLoader historyLoader = new MRJobHistoryLoader(confFile
				.getAbsolutePath(), historyFile.getAbsolutePath());
		MRJobInfo mrJob = historyLoader.getMRJobInfoWithDetails();
		Configuration conf = historyLoader.getHadoopConfiguration();

		// Parse the profile files and get the job profile
		MRTaskProfilesLoader profileLoader = new MRTaskProfilesLoader(mrJob,
				conf, profilesDir.getAbsolutePath());

		// Export the job profile
		if (profileLoader.loadExecutionProfile(mrJob)) {

			// Get the cluster name, if any
			String clusterName = conf.get(PROFILER_CLUSTER_NAME);
			if (clusterName != null)
				mrJob.getProfile().setClusterName(clusterName);

			XMLProfileParser.exportJobProfile(mrJob.getProfile(), profileXML);
		} else {
			LOG.error("Unable to create the job profile for "
					+ mrJob.getExecId());
		}

		return mrJob;
	}

	/**
	 * Builds and returns the job identifier (e.g., job_23412331234_1234)
	 * 
	 * @param job
	 *            the MapReduce job
	 * @return the job identifier
	 */
	public static String getJobId(Job job) {
		String jar = job.getJar();

		Matcher matcher = JOB_PATTERN.matcher(jar);
		if (matcher.find()) {
			return matcher.group(1);
		}

		return "job_";
	}

	/**
	 * Returns an array of task profile files found in the provided directory
	 * for the particular MapReduce job
	 * 
	 * @param job
	 *            the MapReduce job
	 * @param dir
	 *            the directory
	 * @return the task profile files
	 */
	private static File[] getTaskProfiles(Job job, File dir) {
		// List all relevant files
		final String jobId = getJobId(job).substring(4);
		File[] files = dir.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.isFile() && !pathname.isHidden()
						&& pathname.getName().contains(jobId)
						&& pathname.getName().endsWith(".profile");
			}
		});

		return files;
	}
}
