package edu.duke.starfish.profile.profileinfo.utils;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.DEF_TASK_MEM;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_JAVA_OPTS;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

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
	private static int MS_IN_SEC = 1000;
	private static int SEC_IN_MIN = 60;
	private static int MIN_IN_HR = 60;
	private static int SEC_IN_HR = 3600;

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
	 * second with compression enabled. This method will then create a new job
	 * profile that will contain the correct IO and compression costs.
	 * 
	 * @param profNoCompr
	 *            a profile obtained without compression
	 * @param profWithCompr
	 *            a profile obtained with compression
	 * @return a job profile
	 */
	public static MRJobProfile adjustProfilesForCompression(
			MRJobProfile profNoCompr, MRJobProfile profWithCompr) {

		MRJobProfile profResult = new MRJobProfile(profNoCompr);

		// Make sure we have the same number of map profiles
		List<MRMapProfile> mapProfsNoCompr = profNoCompr.getMapProfiles();
		List<MRMapProfile> mapProfsWithCompr = profWithCompr.getMapProfiles();
		if (mapProfsNoCompr.size() != mapProfsWithCompr.size())
			throw new RuntimeException(
					"ERROR: Expected the same number of map profiles");

		// Adjust the map profiles
		List<MRMapProfile> mapProfsResult = profResult.getMapProfiles();
		int numMapProfs = mapProfsResult.size();
		for (int i = 0; i < numMapProfs; ++i) {
			adjustMapProfilesForCompression(mapProfsNoCompr.get(i),
					mapProfsWithCompr.get(i), mapProfsResult.get(i));
		}

		// Make sure we have the same number of reduce profiles
		List<MRReduceProfile> redProfsNoCompr = profNoCompr.getReduceProfiles();
		List<MRReduceProfile> redProfsWithCompr = profWithCompr
				.getReduceProfiles();
		if (redProfsNoCompr.size() != redProfsWithCompr.size())
			throw new RuntimeException(
					"ERROR: Expected the same number of reduce profiles");

		// Adjust the reduce profiles
		List<MRReduceProfile> redProfsResult = profResult.getReduceProfiles();
		int numRedProfs = redProfsResult.size();
		for (int i = 0; i < numRedProfs; ++i) {
			adjustReduceProfilesForCompression(redProfsNoCompr.get(i),
					redProfsWithCompr.get(i), redProfsResult.get(i));
		}

		profResult.updateProfile();
		return profResult;
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
	 * Converts a line containing glob into the corresponding regex.
	 * 
	 * Note: The string.match method matches the entire string by default. If
	 * this method will be used to match an entire string, set matchAll to true.
	 * If this method will be used to match a substring of a string, set
	 * matchAll to false.
	 * 
	 * @param line
	 *            the input glob
	 * @param matchAll
	 *            whether the regex returned is intented to match an entire
	 *            string or a substring
	 * @return the corresponding regex
	 */
	public static String convertGlobToRegEx(String line, boolean matchAll) {

		line = line.trim();
		int strLen = line.length();
		StringBuilder sb = new StringBuilder(strLen);

		if (!matchAll)
			sb.append(".*");

		boolean escaping = false;
		int inCurlies = 0;
		for (char currentChar : line.toCharArray()) {
			switch (currentChar) {
			case '*':
				if (escaping)
					sb.append("\\*");
				else
					sb.append(".*");
				escaping = false;
				break;
			case '?':
				if (escaping)
					sb.append("\\?");
				else
					sb.append('.');
				escaping = false;
				break;
			case '.':
			case '(':
			case ')':
			case '+':
			case '|':
			case '^':
			case '$':
			case '@':
			case '%':
				sb.append('\\');
				sb.append(currentChar);
				escaping = false;
				break;
			case '\\':
				if (escaping) {
					sb.append("\\\\");
					escaping = false;
				} else
					escaping = true;
				break;
			case '{':
				if (escaping) {
					sb.append("\\{");
				} else {
					sb.append('(');
					inCurlies++;
				}
				escaping = false;
				break;
			case '}':
				if (inCurlies > 0 && !escaping) {
					sb.append(')');
					inCurlies--;
				} else if (escaping)
					sb.append("\\}");
				else
					sb.append("}");
				escaping = false;
				break;
			case ',':
				if (inCurlies > 0 && !escaping) {
					sb.append('|');
				} else if (escaping)
					sb.append("\\,");
				else
					sb.append(",");
				break;
			default:
				escaping = false;
				sb.append(currentChar);
			}
		}

		if (!matchAll)
			sb.append(".*");

		return sb.toString();
	}

	/**
	 * Formats the input duration in a human readable format. The output will be
	 * on of: ms, sec, min & sec, hr & min & sec depending on the duration.
	 * 
	 * @param duration
	 *            the duration in ms
	 * @return a formatted string
	 */
	public static String getFormattedDuration(long duration) {

		String result;
		long sec = Math.round(duration / (float) MS_IN_SEC);
		if (duration < MS_IN_SEC) {
			result = String.format("%d ms", duration);
		} else if (sec < SEC_IN_MIN) {
			result = String.format("%d sec %d ms", sec, duration % MS_IN_SEC);
		} else if (sec < SEC_IN_HR) {
			result = String.format("%d min %d sec", sec / SEC_IN_MIN, sec
					% SEC_IN_MIN);
		} else {
			result = String.format("%d hr %d min %d sec", sec / SEC_IN_HR,
					(sec / MIN_IN_HR) % SEC_IN_MIN, sec % SEC_IN_MIN);
		}

		return result;
	}

	/**
	 * Formats the input size in a human readable format. The output will be on
	 * of: Bytes, KB, MB, GB depending on the size.
	 * 
	 * @param bytes
	 *            the size in bytes
	 * @return a formatted string
	 */
	public static String getFormattedSize(long bytes) {

		String result;
		if (bytes < 1024) {
			result = String.format("%d Bytes", bytes);
		} else if (bytes < 1024l * 1024) {
			result = String.format("%.2f KB", bytes / 1024.0);
		} else if (bytes < 1024l * 1024 * 1024) {
			result = String.format("%.2f MB", bytes / (1024.0 * 1024));
		} else {
			result = String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
		}

		return result;
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
			javaOpts.trim();
		}

		// Set the new java opts
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
				+ ProfileUtils.getFormattedDuration(mrJob.getDuration()));
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

		// Initializations
		long jobStart = mrJob.getStartTime().getTime() / 1000;
		int jobDuration = (int) Math.ceil(mrJob.getDuration() / 1000d);
		boolean success = true;
		int start = 0;
		int shuffle = 0;
		int sort = 0;
		int end = 0;
		int time = 0;

		// Create and initialize the counter arrays
		int[] mappers = new int[jobDuration];
		int[] shuffling = new int[jobDuration];
		int[] sorting = new int[jobDuration];
		int[] reducing = new int[jobDuration];
		int[] waste = new int[jobDuration];

		for (int i = 0; i < jobDuration; ++i) {
			mappers[i] = shuffling[i] = sorting[i] = reducing[i] = waste[i] = 0;
		}

		// Count the map tasks
		for (MRMapInfo mrMap : mrJob.getMapTasks()) {
			for (MRMapAttemptInfo mrMapAttempt : mrMap.getAttempts()) {
				success = mrMapAttempt.getStatus() == MRExecutionStatus.SUCCESS;
				start = (int) (mrMapAttempt.getStartTime().getTime() / 1000 - jobStart);
				end = (int) (mrMapAttempt.getEndTime().getTime() / 1000 - jobStart);
				if (success) {
					for (time = start; time < end; ++time)
						++mappers[time];
				} else {
					for (time = start; time < end; ++time)
						++waste[time];
				}
			}
		}

		// Count the reduce tasks
		for (MRReduceInfo mrRed : mrJob.getReduceTasks()) {
			for (MRReduceAttemptInfo mrRedAttempt : mrRed.getAttempts()) {
				success = mrRedAttempt.getStatus() == MRExecutionStatus.SUCCESS;
				start = (int) (mrRedAttempt.getStartTime().getTime() / 1000 - jobStart);
				end = (int) (mrRedAttempt.getEndTime().getTime() / 1000 - jobStart);

				if (success) {
					shuffle = (int) (mrRedAttempt.getShuffleEndTime().getTime() / 1000 - jobStart);
					sort = (int) (mrRedAttempt.getSortEndTime().getTime() / 1000 - jobStart);

					for (time = start; time < shuffle; ++time)
						++shuffling[time];
					for (time = shuffle; time < sort; ++time)
						++sorting[time];
					for (time = sort; time < end; ++time)
						++reducing[time];
				} else {
					for (time = start; time < end; ++time)
						++waste[time];
				}
			}
		}

		// Print out the timeline
		StringBuffer sb = new StringBuffer();
		out.println("Time\tMaps\tShuffle\tMerge\tReduce\tWaste");
		for (int t = 0; t < jobDuration; ++t) {
			sb.append(t);
			sb.append(TAB);
			sb.append(mappers[t]);
			sb.append(TAB);
			sb.append(shuffling[t]);
			sb.append(TAB);
			sb.append(sorting[t]);
			sb.append(TAB);
			sb.append(reducing[t]);
			sb.append(TAB);
			sb.append(waste[t]);

			out.println(sb.toString());
			sb.delete(0, sb.length());
		}

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
					avgRecSize, profWithCompr
							.getCounter(MRCounter.SPILLED_RECORDS));
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
					avgRecSize, profWithCompr
							.getCounter(MRCounter.SPILLED_RECORDS));

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
			profResult.addCostFactor(MRCostFactors.NETWORK_COST, profNoCompr
					.getCostFactor(MRCostFactors.NETWORK_COST));
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
					.getCostFactor(MRCostFactors.READ_LOCAL_IO_COST)
					- readCost;

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

		profResult.addStatistic(MRStatistics.OUT_COMPRESS_RATIO, profWithCompr
				.getStatistic(MRStatistics.OUT_COMPRESS_RATIO));

		if (numRecs == 0l || numBytes == 0l)
			return;

		// Calculate output compression cost
		double avgRecSize = numBytes / (double) numRecs;
		double comprCost = ((profWithCompr
				.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST) - profNoCompr
				.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST)) * profWithCompr
				.getCounter(MRCounter.HDFS_BYTES_WRITTEN))
				/ (numRecs * avgRecSize);
		if (comprCost < 0)
			comprCost = 0d;
		profResult.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
				comprCost);

		// Reset the HDFS write cost
		profResult.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, profNoCompr
				.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST));

	}

}
