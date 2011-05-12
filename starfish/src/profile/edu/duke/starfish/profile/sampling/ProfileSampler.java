package edu.duke.starfish.profile.sampling;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

import edu.duke.starfish.profile.profiler.Profiler;

/**
 * Contains static methods for enabling sampling with profiling.
 * 
 * @author hero
 */
public class ProfileSampler {

	private static final Log LOG = LogFactory.getLog(ProfileSampler.class);
	private static final NumberFormat nf = NumberFormat.getNumberInstance();

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Enable profiling for only a fraction of the MR tasks. The fraction is
	 * expected in "starfish.profiler.sampling.fraction" as a number between 0
	 * and 1. The default value is 0.1.
	 * 
	 * This function sets the Hadoop parameters mapred.task.profile.maps and
	 * mapred.task.profile.reducers
	 * 
	 * @param conf
	 *            The job configuration
	 * @return False if something goes wrong
	 */
	public static boolean sampleTasksToProfile(Configuration conf) {

		double fraction = conf.getFloat(Profiler.PROFILER_SAMPLING_FRACTION,
				0.1f);
		if (fraction == 0d) {
			// Nothing to profile
			return false;
		}

		// Get the input format
		JobContext context = new JobContext(conf, null);
		InputFormat<?, ?> input = null;
		try {
			input = ReflectionUtils.newInstance(context.getInputFormatClass(),
					context.getConfiguration());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		List<InputSplit> splits = null;
		try {
			splits = input.getSplits(context);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}

		// Specify which mappers to profile
		if (splits != null && splits.size() != 0) {
			conf.set("mapred.task.profile.maps", sampleTasksToProfile(splits
					.size(), fraction));
		}

		// Specify which reducers to profile
		int numReducers = conf.getInt(MR_RED_TASKS, 1);
		if (numReducers != 0) {
			conf.set("mapred.task.profile.reduces", sampleTasksToProfile(
					numReducers, fraction));
		}

		nf.setMaximumFractionDigits(2);
		LOG.info("Profiling only " + nf.format(fraction * 100)
				+ "% of the tasks");
		return true;
	}

	/**
	 * This functions modifies the splits list in place to retain only a random
	 * fraction of the input splits. The fraction is expected in
	 * "starfish.profiler.sampling.fraction" as a number between 0 and 1. The
	 * default value is 0.1.
	 * 
	 * @param job
	 *            The job context
	 * @param splits
	 *            The list of input splits to modify
	 */
	public static void sampleInputSplits(JobContext job, List<InputSplit> splits) {

		// Get the sampling fraction
		Configuration conf = job.getConfiguration();
		double fraction = conf.getFloat(Profiler.PROFILER_SAMPLING_FRACTION,
				0.1f);
		if (fraction < 0 || fraction > 1)
			throw new RuntimeException("ERROR: Invalid sampling fraction: "
					+ fraction);

		// Handle corner cases
		if (fraction == 0 || splits.size() == 0) {
			splits.clear();
			return;
		}
		if (fraction == 1)
			return;

		// Calculate the number of samples
		int numSplits = splits.size();
		int sampleSize = (int) Math.round(numSplits * fraction);
		if (sampleSize == 0)
			sampleSize = 1;

		// Shuffle the splits
		Collections.shuffle(splits);

		// Retain only a sampleSize number of splits
		for (int i = splits.size() - 1; i >= sampleSize; --i) {
			splits.remove(i);
		}

		nf.setMaximumFractionDigits(2);
		LOG.info("Executing only " + nf.format(fraction * 100)
				+ "% of the map tasks");
	}

	/* ***************************************************************
	 * PRIVATE STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Convert an array of numbers into a comma-separated string with no
	 * whitespace
	 * 
	 * @param numbers
	 *            the numbers
	 * @return a comma-separated string
	 */
	private static String convertArrayToString(int[] numbers) {
		if (numbers.length == 0)
			return "";

		StringBuilder sb = new StringBuilder();
		sb.append(numbers[0]);
		for (int i = 1; i < numbers.length; ++i) {
			sb.append(',');
			sb.append(numbers[i]);
		}

		return sb.toString();
	}

	/**
	 * Returns a string representing which random fraction of the total tasks to
	 * profile
	 * 
	 * @param numTasks
	 *            the total number of task
	 * @param fraction
	 *            the fraction of tasks to profile
	 * @return which tasks to profile
	 */
	private static String sampleTasksToProfile(int numTasks, double fraction) {
		String tasks = convertArrayToString(sampleIntsFromDomain(0, numTasks,
				fraction));

		// We must add the next two task ids to capture the job setup and
		// cleanup tasks. Otherwise, the MR job will fail!
		tasks = tasks + "," + numTasks + "," + (numTasks + 1);

		return tasks;
	}

	/**
	 * Sample a list of integers from the domain specified by the min and max
	 * parameters.
	 * 
	 * Example: For input (0, 10, 0.5), this function will return an array with
	 * 5 random unique numbers between 0 inclusive and 10 exclusive. Example
	 * output: [0,3,5,6,8]
	 * 
	 * @param min
	 *            The min integer in the domain - inclusive
	 * @param max
	 *            The max integer in the domain - exclusive
	 * @param fraction
	 *            The sampling fraction
	 * @return an array of integers from the domain
	 */
	private static int[] sampleIntsFromDomain(int min, int max, double fraction) {

		// Error checking
		int domainSize = max - min;
		if (domainSize <= 0)
			throw new RuntimeException(
					"ERROR: Invalid domain range to sample from: min=" + min
							+ " max=" + max);

		if (fraction < 0 || fraction > 1)
			throw new RuntimeException("ERROR: Invalid sampling fraction: "
					+ fraction);

		// Calculate the number of samples
		int sampleSize = (int) Math.round(domainSize * fraction);
		if (sampleSize == 0)
			sampleSize = 1;

		// Create an array with all possible integers in the domain
		int[] domain = new int[domainSize];
		for (int i = 0; i < domainSize; ++i) {
			domain[i] = min + i;
		}

		// Shuffle by exchanging each element randomly
		Random rgen = new Random();
		for (int i = 0; i < domainSize; i++) {
			int rndPos = rgen.nextInt(domainSize);
			int temp = domain[i];
			domain[i] = domain[rndPos];
			domain[rndPos] = temp;
		}

		// Get the first 'sampleSize' numbers from the shuffled domain
		int[] samples = new int[sampleSize];
		for (int i = 0; i < sampleSize; ++i) {
			samples[i] = domain[i];
		}
		Arrays.sort(samples);

		return samples;
	}

}
