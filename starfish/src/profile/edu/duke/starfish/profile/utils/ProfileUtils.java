package edu.duke.starfish.profile.utils;

import static edu.duke.starfish.profile.utils.Constants.*;

import java.io.File;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRTaskInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import edu.duke.starfish.profile.profiler.MRJobLogsManager;
import edu.duke.starfish.profile.profiler.Profiler;

/**
 * Contains utility methods that manipulate the profileinfo classes.
 * 
 * @author hero
 */
public class ProfileUtils {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private static final String TAB = "\t";

	private static final Pattern jvmMem = Pattern
			.compile("-Xmx([0-9]+)([M|m|G|g])");

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * This functions expects as input two profiles for the same MapReduce job;
	 * the first job should be profiled without compression enabled, and the
	 * second with compression enabled.
	 * 
	 * This method will then create two new job profiles that will contain the
	 * correct IO and compression costs and statistics; the first profile will
	 * correspond to the input profile without compression and the second to the
	 * input profile with compression
	 * 
	 * @param profNoCompr
	 *            a profile obtained without compression
	 * @param profWithCompr
	 *            a profile obtained with compression
	 * @return two job profiles
	 */
	public static MRJobProfile[] adjustProfilesForCompression(
			MRJobProfile profNoCompr, MRJobProfile profWithCompr) {

		// Make sure we have the same number of map profiles
		List<MRMapProfile> mapProfsNoCompr = profNoCompr.getMapProfiles();
		List<MRMapProfile> mapProfsWithCompr = profWithCompr.getMapProfiles();
		if (mapProfsNoCompr.size() != mapProfsWithCompr.size())
			throw new RuntimeException(
					"ERROR: Expected the same number of map profiles");

		// Create the two adjusted profiles
		MRJobProfile adjProfNoCompr = new MRJobProfile(profNoCompr);
		MRJobProfile adjProfWithCompr = new MRJobProfile(profWithCompr);

		// Adjust the map profiles
		int numMapProfs = mapProfsNoCompr.size();
		for (int i = 0; i < numMapProfs; ++i) {
			MRMapProfile mapProf = new MRMapProfile("");
			adjustMapProfilesForCompression(mapProfsNoCompr.get(i),
					mapProfsWithCompr.get(i), mapProf);

			// Set the adjusted cost factors and statistics
			adjProfNoCompr.getMapProfiles().get(i)
					.addCostFactors(mapProf.getCostFactors());
			adjProfWithCompr.getMapProfiles().get(i)
					.addCostFactors(mapProf.getCostFactors());
			adjProfNoCompr.getMapProfiles().get(i)
					.addStatistics(mapProf.getStatistics());
			adjProfWithCompr.getMapProfiles().get(i)
					.addStatistics(mapProf.getStatistics());
		}

		// Make sure we have the same number of reduce profiles
		List<MRReduceProfile> redProfsNoCompr = profNoCompr.getReduceProfiles();
		List<MRReduceProfile> redProfsWithCompr = profWithCompr
				.getReduceProfiles();
		if (redProfsNoCompr.size() != redProfsWithCompr.size())
			throw new RuntimeException(
					"ERROR: Expected the same number of reduce profiles");

		// Adjust the reduce profiles
		int numRedProfs = redProfsNoCompr.size();
		for (int i = 0; i < numRedProfs; ++i) {
			MRReduceProfile redProf = new MRReduceProfile("");
			adjustReduceProfilesForCompression(redProfsNoCompr.get(i),
					redProfsWithCompr.get(i), redProf);

			// Set the adjusted cost factors and statistics
			adjProfNoCompr.getReduceProfiles().get(i)
					.addCostFactors(redProf.getCostFactors());
			adjProfWithCompr.getReduceProfiles().get(i)
					.addCostFactors(redProf.getCostFactors());
			adjProfNoCompr.getReduceProfiles().get(i)
					.addStatistics(redProf.getStatistics());
			adjProfWithCompr.getReduceProfiles().get(i)
					.addStatistics(redProf.getStatistics());
		}

		adjProfNoCompr.updateProfile();
		adjProfWithCompr.updateProfile();

		MRJobProfile[] result = { adjProfNoCompr, adjProfWithCompr };
		return result;
	}

	/**
	 * Calculates the average duration of the tasks
	 * 
	 * @param tasks
	 *            a list with tasks
	 * @return the average duration of the tasks
	 */
	public static double calculateDurationAverage(
			List<? extends MRTaskInfo> tasks) {
		// Error checking
		if (tasks == null || tasks.size() == 0)
			return 0;

		// Calculate the duration sum
		double sum = 0d;
		for (MRTaskInfo task : tasks) {
			sum += task.getDuration();
		}

		// Calculate and return the duration average
		return sum / tasks.size();
	}

	/**
	 * Calculates the standard deviation for the duration of the tasks
	 * 
	 * @param tasks
	 *            a list with tasks
	 * @return the standard deviation
	 */
	public static double calculateDurationDeviation(
			List<? extends MRTaskInfo> tasks) {
		// Error checking
		if (tasks == null || tasks.size() == 0)
			return 0;

		// Calculate the variance V = Sum{(Xi - m)^2} / N
		double avg = calculateDurationAverage(tasks);
		double variance = 0l;
		for (MRTaskInfo task : tasks) {
			variance += (task.getDuration() - avg) * (task.getDuration() - avg);
		}

		return Math.sqrt(variance / tasks.size());
	}

	/**
	 * Generate the data transfers among the task attempts that occur during the
	 * execution of a MapReduce job. The data transfers are placed in the
	 * provided job object.
	 * 
	 * This method assumes that the data transfered from each map attempt is
	 * proportional to the amount of data shuffled to each reduce attempt. For
	 * example, suppose the job run 2 reduce tasks and that they received 500MB
	 * and 300MB data respectively. If one map produced 160MB of data (amount of
	 * data stored on local disk after combiner/compression), then we assume
	 * 100MB (=160*5/8) were shuffled to the first reducer, and 60MB (=160*3/8)
	 * to the second reducer.
	 * 
	 * The duration of the data transfers is determined based on the shuffle
	 * time and the amount of data shuffled on each reducer.
	 * 
	 * @param job
	 *            the MapReduce job
	 * @param conf
	 *            the configuration
	 * @return true if the data transfers were generated
	 */
	public static boolean generateDataTransfers(MRJobInfo job,
			Configuration conf) {

		job.getDataTransfers().clear();
		List<MRReduceAttemptInfo> redAttempts = job
				.getReduceAttempts(MRExecutionStatus.SUCCESS);
		int numReducers = redAttempts.size();
		if (numReducers == 0) {
			// This is a map-only job, no data transfers
			return false;
		}

		// Calculate the total amount of shuffle data
		double totalShuffle = 0d;
		double[] redSizeRatio = new double[numReducers];
		double[] redTimeRatio = new double[numReducers];

		for (int i = 0; i < numReducers; ++i) {
			redSizeRatio[i] = redAttempts.get(i).getProfile()
					.getCounter(MRCounter.REDUCE_SHUFFLE_BYTES, 0l);
			redTimeRatio[i] = (redSizeRatio[i] != 0) ? (redAttempts.get(i)
					.getProfile().getTiming(MRTaskPhase.SHUFFLE, 0d) / redSizeRatio[i])
					: 0d;
			totalShuffle += redSizeRatio[i];
		}

		if (totalShuffle == 0) {
			// The information is not available
			return false;
		}

		// Calculate the ratio of data that will go to each reducer
		for (int i = 0; i < numReducers; ++i) {
			redSizeRatio[i] = redSizeRatio[i] / totalShuffle;
		}

		// Is the map output data compressed?
		long comprSize = 0l;
		long uncomprSize = 0l;
		boolean isCompr = conf.getBoolean(Constants.MR_COMPRESS_MAP_OUT, false);
		double comprRatio = job.getProfile().getAvgReduceProfile()
				.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO, 1d);

		// Create a new data transfer from each map to each reducer
		List<MRMapAttemptInfo> mapAttempts = job
				.getMapAttempts(MRExecutionStatus.SUCCESS);
		for (MRMapAttemptInfo mapAttempt : mapAttempts) {
			long outSize = mapAttempt.getProfile().getCounter(
					MRCounter.MAP_OUTPUT_MATERIALIZED_BYTES, 0l);

			for (int i = 0; i < numReducers; ++i) {
				// Create a new map to reduce transfer
				comprSize = (long) Math.round(outSize * redSizeRatio[i]);
				if (isCompr)
					uncomprSize = (long) Math.round(comprSize / comprRatio);
				else
					uncomprSize = comprSize;

				if (comprSize != 0) {
					DataTransfer transfer = new DataTransfer(mapAttempt,
							redAttempts.get(i), comprSize, uncomprSize);

					// Adjust the transfer's end time based on duration
					long duration = (long) Math.ceil(comprSize
							* redTimeRatio[i]);
					transfer.setEndTime(new Date(transfer.getStartTime()
							.getTime() + duration));

					job.addDataTransfer(transfer);
				}
			}
		}

		return true;
	}

	/**
	 * Get the list of input directories from the job configuration. An empty
	 * array is returned if none are found.
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the list of input directories
	 */
	public static String[] getInputDirs(Configuration conf) {

		String inputDirs = conf.get(MR_INPUT_DIR);
		if (inputDirs == null)
			inputDirs = conf.get(PIG_INPUT_DIRS);

		if (inputDirs == null)
			return new String[0];
		else
			return StringUtils.split(inputDirs);
	}

	/**
	 * Get the list of output directories from the job configuration. An empty
	 * array is returned if none are found.
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the list of output directories
	 */
	public static String[] getOutputDirs(Configuration conf) {

		String outputDirs = conf.get(PIG_MAP_OUT_DIRS);
		if (outputDirs == null)
			outputDirs = conf.get(PIG_RED_OUT_DIRS);
		if (outputDirs == null)
			outputDirs = conf.get(PIG_MAPRED_OUT_DIRS);
		if (outputDirs == null)
			outputDirs = conf.get(MR_OUTPUT_DIR);

		if (outputDirs == null)
			return new String[0];
		else
			return StringUtils.split(outputDirs);
	}

	/**
	 * Returns the task memory based on the java opts setting in bytes
	 * 
	 * @param conf
	 *            the configuration
	 * @return the task memory in bytes
	 */
	public static long getTaskMemory(Configuration conf) {

		String javaOpts = conf.get(MR_JAVA_OPTS);
		if (javaOpts == null)
			return DEF_TASK_MEM;

		Matcher m = jvmMem.matcher(javaOpts);
		if (m.find()) {
			if (m.group(2).equals("m") || m.group(2).equals("M"))
				return Long.parseLong(m.group(1)) << 20;
			else if (m.group(2).equals("g") || m.group(2).equals("G"))
				return Long.parseLong(m.group(1)) << 30;
			else
				return Long.parseLong(m.group(1));
		} else {
			return DEF_TASK_MEM;
		}
	}

	/**
	 * Checks if the task memory has been set in the java opts setting
	 * 
	 * @param conf
	 *            the configuration
	 * @return true if task memory is set
	 */
	public static boolean isTaskMemorySet(Configuration conf) {

		String javaOpts = conf.get(MR_JAVA_OPTS);
		if (javaOpts == null)
			return false;

		Matcher m = jvmMem.matcher(javaOpts);
		return m.find();
	}

	/**
	 * Determine whether the MapReduce output compression is on or off based on
	 * either the Hadoop parameters or the extension of the output paths.
	 * 
	 * Note: if no output paths are provided, this method will access the conf
	 * to get the output paths
	 * 
	 * @param conf
	 *            the configuration
	 * @param outPaths
	 *            the output paths
	 * @return true if output compression is on
	 */
	public static boolean isMROutputCompressionOn(Configuration conf,
			String... outPaths) {

		// Check the official parameter first
		if (conf.getBoolean(MR_COMPRESS_OUT, false) == true)
			return true;

		if (outPaths == null || outPaths.length == 0)
			outPaths = ProfileUtils.getOutputDirs(conf);

		// Look into the output paths
		for (String outDir : outPaths) {
			if (GeneralUtils.hasCompressionExtension(outDir))
				return true;
			if (ProfileUtils.isPigTempPath(conf, outDir))
				return conf.getBoolean(Constants.PIG_TEMP_COMPRESSION, false);
		}

		return false;
	}

	/**
	 * Checks if the provided path is a Pig temporary path
	 * 
	 * @param conf
	 *            the configuration
	 * @param path
	 *            the path to check
	 * @return true if the path is a Pig temporary path
	 */
	public static boolean isPigTempPath(Configuration conf, String path) {

		// Remove any leading URL scheme and domain
		int index = path.indexOf("://");
		if (index > 0) {
			index = path.indexOf('/', index + 3);
			path = path.substring(index);
		}

		if (path.startsWith(conf.get(Constants.PIG_TEMP_DIR, "/tmp")))
			return true;
		else
			return false;
	}

	/**
	 * Finds and loads the source profile based on either a job profile id or a
	 * profile file path.
	 * 
	 * This method exists only to ensure backwards compatibility with the time
	 * we expected profile paths. The new approach is to load the profiles based
	 * on job ids. In the case of a job id, we get the profiles directory from
	 * "starfish.profiler.output.dir".
	 * 
	 * @param profileIdOrFile
	 *            a job profile id or a profile file path
	 * @param conf
	 *            the job configuration
	 * @return a job profile
	 */
	public static MRJobProfile loadSourceProfile(String profileIdOrFile,
			Configuration conf) {

		File profFile = new File(profileIdOrFile);
		if (profFile.exists()) {
			// profileIdOrFile is a file
			return XMLProfileParser.importJobProfile(profFile);
		} else {
			// profileIdOrFile is a job id
			MRJobLogsManager manager = new MRJobLogsManager();
			manager.setResultsDir(conf.get(Profiler.PROFILER_OUTPUT_DIR));
			return manager.getMRJobProfile(profileIdOrFile);
		}
	}

	/**
	 * Sets the task memory in the java opts setting
	 * 
	 * @param conf
	 *            the configuration
	 * @param memory
	 *            the memory in bytes
	 */
	public static void setTaskMemory(Configuration conf, long memory) {

		// Build the memory string
		String taskMem = "-Xmx" + (memory >> 20) + "M";

		// Get the current java opt setting and set the new memory
		String javaOpts = conf.get(MR_JAVA_OPTS, "");
		Matcher m = jvmMem.matcher(javaOpts);

		if (m.find()) {
			javaOpts = m.replaceAll(taskMem);
		} else {
			javaOpts = javaOpts + " " + taskMem;
		}

		// Set the new java opts
		javaOpts = javaOpts.trim();
		conf.set(MR_JAVA_OPTS, javaOpts);
	}

	/**
	 * Prints out all available information for the job, including details for
	 * the job and all the task attempts.
	 * 
	 * @param out
	 *            The print stream to print at
	 * @param mrJob
	 *            the MR job of interest
	 */
	public static void printMRJobDetails(PrintStream out, MRJobInfo mrJob) {

		// Create a number format
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumFractionDigits(2);
		nf.setMaximumFractionDigits(2);

		// Print job information
		out.println("MapReduce Job: ");
		out.println("\tJob ID:\t" + mrJob.getExecId());
		out.println("\tJob Name:\t" + mrJob.getName());
		out.println("\tUser Name:\t" + mrJob.getUser());
		out.println("\tStart Time:\t" + mrJob.getStartTime());
		out.println("\tEnd Time:\t" + mrJob.getEndTime());
		out.println("\tDuration:\t"
				+ GeneralUtils.getFormattedDuration(mrJob.getDuration()));
		out.println("\tStatus: \t" + mrJob.getStatus());
		out.println();

		// Gather some counts
		int mapSuccessCount = 0;
		int mapFailedCount = 0;
		int mapKilledCount = 0;
		int dataLocalCount = 0;
		int rackLocalCount = 0;
		int nonLocalCount = 0;

		// Calculate map statistics
		for (MRMapInfo mrMap : mrJob.getMapTasks()) {
			for (MRMapAttemptInfo mrMapAttempt : mrMap.getAttempts()) {
				// Update counters
				switch (mrMapAttempt.getStatus()) {
				case SUCCESS:
					++mapSuccessCount;
					break;
				case FAILED:
					++mapFailedCount;
					break;
				case KILLED:
					++mapKilledCount;
					break;
				default:
					break;
				}

				switch (mrMapAttempt.getDataLocality()) {
				case DATA_LOCAL:
					++dataLocalCount;
					break;
				case RACK_LOCAL:
					++rackLocalCount;
					break;
				case NON_LOCAL:
					++nonLocalCount;
				default:
					break;
				}
			}
		}

		// Print out map statistics
		double mapAvg = ProfileUtils.calculateDurationAverage(mrJob
				.getMapTasks());
		double mapDev = ProfileUtils.calculateDurationDeviation(mrJob
				.getMapTasks());
		out.println("Map Statistics:");
		out.println("\tCount:\t" + mrJob.getMapTasks().size());
		out.println("\tSuccessful:\t" + mapSuccessCount);
		out.println("\tFailed:\t" + mapFailedCount);
		out.println("\tKilled:\t" + mapKilledCount);
		out.println("\tData Local:\t" + dataLocalCount);
		out.println("\tRack Local:\t" + rackLocalCount);
		out.println("\tNon Local:\t" + nonLocalCount);
		out.println("\tAverage Duration (ms):\t" + nf.format(mapAvg));
		out.println("\tSD of Duration (ms):\t" + nf.format(mapDev));
		out.println("\tCV of Duration:\t"
				+ nf.format((mapAvg != 0) ? mapDev / mapAvg : 0));
		out.println();

		// Gather counters for the reducers
		int redSuccessCount = 0;
		int redFailedCount = 0;
		int redKilledCount = 0;

		long sumShuffleDur = 0l;
		long sumSortDur = 0l;
		long sumReduceDur = 0l;
		long sumTotalDur = 0l;

		// Print reduce information
		for (MRReduceInfo mrReduce : mrJob.getReduceTasks()) {
			for (MRReduceAttemptInfo mrReduceAttempt : mrReduce.getAttempts()) {
				// Update counters
				switch (mrReduceAttempt.getStatus()) {
				case SUCCESS:
					++redSuccessCount;
					break;
				case FAILED:
					++redFailedCount;
					break;
				case KILLED:
					++redKilledCount;
					break;
				default:
					break;
				}

				if (mrReduceAttempt.getStatus() == MRExecutionStatus.SUCCESS) {
					sumShuffleDur += mrReduceAttempt.getShuffleDuration();
					sumSortDur += mrReduceAttempt.getSortDuration();
					sumReduceDur += mrReduceAttempt.getReduceDuration();
					sumTotalDur += mrReduceAttempt.getDuration();
				}
			}
		}

		// Print out reduce statistics
		double numReds = mrJob.getReduceTasks().size();
		if (numReds > 0) {
			double redAvg = sumTotalDur / numReds;
			double redDev = ProfileUtils.calculateDurationDeviation(mrJob
					.getReduceTasks());
			out.println("Reduce Statistics:");
			out.println("\tCount:\t" + mrJob.getReduceTasks().size());
			out.println("\tSuccessful:\t" + redSuccessCount);
			out.println("\tFailed:\t" + redFailedCount);
			out.println("\tKilled:\t" + redKilledCount);
			out.println("\tAvg Shuffle Duration (ms):\t"
					+ nf.format(sumShuffleDur / numReds));
			out.println("\tAvg Sort Duration (ms):\t"
					+ nf.format(sumSortDur / numReds));
			out.println("\tAvg Reduce Duration (ms):\t"
					+ nf.format(sumReduceDur / numReds));
			out.println("\tAvg Total Duration (ms):\t"
					+ nf.format(sumTotalDur / numReds));
			out.println("\tSD of Total Duration (ms):\t" + nf.format(redDev));
			out.println("\tCV of Total Duration:\t"
					+ nf.format((redAvg != 0) ? redDev / redAvg : 0));
			out.println();
		}

	}

	/**
	 * Prints out a timeline with the task execution of a job. It produces
	 * tabular data of the form "Time\tMaps\tShuffle\tMerge\tReduce\tWaste"
	 * 
	 * @param out
	 *            The print stream to print at
	 * @param mrJob
	 *            the MR job of interest
	 */
	public static void printMRJobTimeline(PrintStream out, MRJobInfo mrJob) {

		// Calculate and print out the timeline
		TimelineCalc timeline = new TimelineCalc(mrJob.getStartTime(),
				mrJob.getEndTime());
		timeline.addJob(mrJob);
		timeline.printTimeline(out);
	}

	/**
	 * Prints out information for the provided list of task attempts. It
	 * produces tabular data of the form
	 * "TaskID\tHost\tStatus\tLocality\tDuration"
	 * 
	 * @param out
	 *            The print stream to print at
	 * @param tasks
	 *            The list of task attempts
	 */
	public static void printMRMapInfo(PrintStream out, List<MRMapInfo> tasks) {

		out.println("TaskID\tHost\tStatus\tLocality\tDuration (ms)");
		for (MRMapInfo task : tasks) {
			MRMapAttemptInfo attempt = task.getSuccessfulAttempt();
			if (attempt != null) {
				out.print(task.getExecId());
				out.print(TAB);
				out.print(attempt.getTaskTracker().getHostName());
				out.print(TAB);
				out.print(attempt.getStatus());
				out.print(TAB);
				out.print(attempt.getDataLocality());
				out.print(TAB);
				out.println(attempt.getDuration());
			} else {
				out.print(task.getExecId());
				out.print("\tNA\t");
				out.print(task.getStatus());
				out.print("\tNA\t");
				out.println(task.getDuration());
			}
		}
	}

	/**
	 * Prints out information for the provided list of reduce attempts. It
	 * produces tabular data of the form
	 * "TaskID\tHost\tStatus\tShuffle\tSort\tReduce\tTotal"
	 * 
	 * @param out
	 *            The print stream to print at
	 * @param tasks
	 *            The list of task attempts
	 */
	public static void printMRReduceInfo(PrintStream out,
			List<MRReduceInfo> tasks) {

		out.println("TaskID\tHost\tStatus\tShuffle Duration (ms)"
				+ "\tSort Duration (ms)\tReduce Duration (ms)"
				+ "\tTotal Duration (ms)");
		for (MRReduceInfo task : tasks) {
			MRReduceAttemptInfo attempt = task.getSuccessfulAttempt();
			if (attempt != null) {
				out.print(task.getExecId());
				out.print(TAB);
				out.print(attempt.getTaskTracker().getHostName());
				out.print(TAB);
				out.print(attempt.getStatus());
				out.print(TAB);
				out.print(attempt.getShuffleDuration());
				out.print(TAB);
				out.print(attempt.getSortDuration());
				out.print(TAB);
				out.print(attempt.getReduceDuration());
				out.print(TAB);
				out.println(attempt.getDuration());
			} else {
				out.print(task.getExecId());
				out.print("\tNA\t");
				out.println(task.getStatus());
				out.print("\t0\t0\t0\t");
				out.println(task.getDuration());
			}
		}
	}

	/* ***************************************************************
	 * PRIVATE STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * This functions expects as input two profiles for the same map; the first
	 * map should be profiled without compression enabled, and the second with
	 * compression enabled. This method will calculate the correct IO and
	 * compression costs, and set them in the result map profile
	 * 
	 * @param profNoCompr
	 *            map profile obtained without compression
	 * @param profWithCompr
	 *            map profile obtained with compression
	 * @param profResult
	 *            the result map profile
	 */
	private static void adjustMapProfilesForCompression(
			MRMapProfile profNoCompr, MRMapProfile profWithCompr,
			MRMapProfile profResult) {

		// Average all the costs
		for (MRCostFactors factor : MRCostFactors.values()) {
			if (profNoCompr.containsCostFactor(factor)
					&& profWithCompr.containsCostFactor(factor)) {
				profResult.addCostFactor(factor, (profNoCompr
						.getCostFactor(factor) + profWithCompr
						.getCostFactor(factor)) / 2);
			}
		}

		// Adjust for input compression statistics
		if (profWithCompr.containsStatistic(MRStatistics.INPUT_COMPRESS_RATIO)) {
			adjustTaskInputCompression(profNoCompr, profWithCompr, profResult);
		}

		// Adjust for intermediate compression statistics
		if (profWithCompr.containsStatistic(MRStatistics.INTERM_COMPRESS_RATIO)) {

			double avgRecSize = profWithCompr
					.getCounter(MRCounter.MAP_OUTPUT_BYTES)
					/ (double) profWithCompr
							.getCounter(MRCounter.MAP_OUTPUT_RECORDS);

			adjustTaskIntermCompression(profNoCompr, profWithCompr, profResult,
					avgRecSize,
					profWithCompr.getCounter(MRCounter.SPILLED_RECORDS));
		}

		// Adjust for output compression statistics (map-only job)
		if (profWithCompr.containsStatistic(MRStatistics.OUT_COMPRESS_RATIO)) {
			adjustTaskOutputCompression(profNoCompr, profWithCompr, profResult,
					profWithCompr.getCounter(MRCounter.MAP_OUTPUT_RECORDS),
					profWithCompr.getCounter(MRCounter.MAP_OUTPUT_BYTES));
		}
	}

	/**
	 * This functions expects as input two profiles for the same reducer; the
	 * first reducer should be profiled without compression enabled, and the
	 * second with compression enabled. This method will calculate the correct
	 * IO and compression costs, and set them in the result reduce profile
	 * 
	 * @param profNoCompr
	 *            reduce profile obtained without compression
	 * @param profWithCompr
	 *            reduce profile obtained with compression
	 * @param profResult
	 *            the result reduce profile
	 */
	private static void adjustReduceProfilesForCompression(
			MRReduceProfile profNoCompr, MRReduceProfile profWithCompr,
			MRReduceProfile profResult) {

		// Average all the costs
		for (MRCostFactors factor : MRCostFactors.values()) {
			if (profNoCompr.containsCostFactor(factor)
					&& profWithCompr.containsCostFactor(factor)) {
				profResult.addCostFactor(factor, (profNoCompr
						.getCostFactor(factor) + profWithCompr
						.getCostFactor(factor)) / 2);
			}
		}

		// Adjust for intermediate compression statistics
		if (profWithCompr.containsStatistic(MRStatistics.INTERM_COMPRESS_RATIO)) {

			double avgRecSize = profWithCompr
					.getCounter(MRCounter.REDUCE_INPUT_BYTES)
					/ (double) profWithCompr
							.getCounter(MRCounter.REDUCE_INPUT_RECORDS);

			adjustTaskIntermCompression(profNoCompr, profWithCompr, profResult,
					avgRecSize,
					profWithCompr.getCounter(MRCounter.SPILLED_RECORDS));

			// Calculate intermediate uncompression cost during shuffle
			double uncomprCost = profWithCompr
					.getCostFactor(MRCostFactors.NETWORK_COST)
					- profNoCompr.getCostFactor(MRCostFactors.NETWORK_COST);
			if (uncomprCost < 0)
				uncomprCost = 0d;

			// Average with uncompression cost from reading calculated earlier
			uncomprCost = (uncomprCost + profResult
					.getCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST)) / 2;
			profResult.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
					uncomprCost);

			// Reset the network cost
			profResult.addCostFactor(MRCostFactors.NETWORK_COST,
					profNoCompr.getCostFactor(MRCostFactors.NETWORK_COST));
		}

		// Adjust for output compression statistics
		if (profWithCompr.containsStatistic(MRStatistics.OUT_COMPRESS_RATIO)) {
			adjustTaskOutputCompression(profNoCompr, profWithCompr, profResult,
					profWithCompr.getCounter(MRCounter.REDUCE_OUTPUT_RECORDS),
					profWithCompr.getCounter(MRCounter.REDUCE_OUTPUT_BYTES));
		}
	}

	/**
	 * Adjust the input compression costs.
	 * 
	 * @param profNoCompr
	 *            profile without compression
	 * @param profWithCompr
	 *            profile with compression
	 * @param profResult
	 *            the result profile
	 */
	private static void adjustTaskInputCompression(MRTaskProfile profNoCompr,
			MRTaskProfile profWithCompr, MRTaskProfile profResult) {

		if (profNoCompr.containsStatistic(MRStatistics.INPUT_COMPRESS_RATIO)) {
			// Input compressed in both jobs
			double halfReadCost = (profNoCompr
					.getCostFactor(MRCostFactors.READ_HDFS_IO_COST) + profWithCompr
					.getCostFactor(MRCostFactors.READ_HDFS_IO_COST)) / 4.0d;
			profResult.addCostFactor(MRCostFactors.READ_HDFS_IO_COST,
					halfReadCost);
			profResult.addCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST,
					halfReadCost);
		} else {
			// Input compressed only in the job with compression
			profResult.addStatistic(MRStatistics.INPUT_COMPRESS_RATIO,
					profWithCompr
							.getStatistic(MRStatistics.INPUT_COMPRESS_RATIO));

			// Calculate input uncompression cost
			double uncomprCost = profWithCompr
					.getCostFactor(MRCostFactors.READ_HDFS_IO_COST)
					- profNoCompr
							.getCostFactor(MRCostFactors.READ_HDFS_IO_COST);
			if (uncomprCost < 0)
				uncomprCost = 0d;
			profResult.addCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST,
					uncomprCost);

			// Reset the read HDFS cost
			profResult.addCostFactor(MRCostFactors.READ_HDFS_IO_COST,
					profNoCompr.getCostFactor(MRCostFactors.READ_HDFS_IO_COST));
		}
	}

	/**
	 * Adjust the intermediate compression costs
	 * 
	 * @param profNoCompr
	 *            profile without compression
	 * @param profWithCompr
	 *            profile with compression
	 * @param profResult
	 *            the result profile
	 * @param avgRecSize
	 *            the average record size compressed
	 * @param numRecs
	 *            the number of records compressed
	 */
	private static void adjustTaskIntermCompression(MRTaskProfile profNoCompr,
			MRTaskProfile profWithCompr, MRTaskProfile profResult,
			double avgRecSize, long numRecs) {

		profResult.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO,
				profWithCompr.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO));

		if (avgRecSize == 0d || numRecs == 0l)
			return;

		// Calculate intermediate uncompression cost
		if (profNoCompr.containsCostFactor(MRCostFactors.READ_LOCAL_IO_COST)
				&& profWithCompr
						.containsCostFactor(MRCostFactors.READ_LOCAL_IO_COST)) {

			double readCost = profNoCompr
					.getCostFactor(MRCostFactors.READ_LOCAL_IO_COST);
			double uncomprCost = profWithCompr
					.getCostFactor(MRCostFactors.READ_LOCAL_IO_COST) - readCost;

			profResult
					.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST, readCost);
			profResult.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
					uncomprCost < 0 ? 0 : uncomprCost);
		}

		// Calculate intermediate compression cost
		if (profNoCompr.containsCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST)
				&& profWithCompr
						.containsCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST)
				&& profWithCompr.containsCounter(MRCounter.FILE_BYTES_WRITTEN)) {

			double writeCost = profNoCompr
					.getCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST);
			double comprCost = ((profWithCompr
					.getCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST) - writeCost) * profWithCompr
					.getCounter(MRCounter.FILE_BYTES_WRITTEN))
					/ (numRecs * avgRecSize);

			profResult.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST,
					writeCost);
			profResult.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST,
					comprCost < 0 ? 0 : comprCost);
		}
	}

	/**
	 * Adjust the output compression costs
	 * 
	 * @param profNoCompr
	 *            profile without compression
	 * @param profWithCompr
	 *            profile with compression
	 * @param profResult
	 *            the result profile
	 * @param numRecs
	 *            the number of records compressed
	 * @param numBytes
	 *            the number of bytes compressed
	 */
	private static void adjustTaskOutputCompression(MRTaskProfile profNoCompr,
			MRTaskProfile profWithCompr, MRTaskProfile profResult,
			long numRecs, long numBytes) {

		profResult.addStatistic(MRStatistics.OUT_COMPRESS_RATIO,
				profWithCompr.getStatistic(MRStatistics.OUT_COMPRESS_RATIO));

		if (numRecs == 0l || numBytes == 0l)
			return;

		// Calculate output compression cost
		double avgRecSize = numBytes / (double) numRecs;
		double comprCost = ((profWithCompr
				.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST) - profNoCompr
				.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST)) * profWithCompr
				.getCounter(MRCounter.HDFS_BYTES_WRITTEN, profWithCompr
						.getCounter(MRCounter.S3N_BYTES_WRITTEN, 0l)))
				/ (numRecs * avgRecSize);
		if (comprCost < 0)
			comprCost = 0d;
		profResult.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
				comprCost);

		// Reset the HDFS write cost
		profResult.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST,
				profNoCompr.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST));

	}

}
