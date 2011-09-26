package edu.duke.starfish.profile.utils;

import static edu.duke.starfish.profile.utils.Constants.HADOOP_LOCAL_JOB_HISTORY;
import static edu.duke.starfish.profile.utils.Constants.HADOOP_LOG_DIR;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.duke.starfish.profile.profiler.Profiler;

/**
 * A simple class to gather history, task profiles, and data transfers after a
 * job completes execution. This class is useful when a job is not profiled, or
 * when the job is profiled without the bin/profile script.
 * 
 * @author hero
 */
public class GatherResults {

	/**
	 * Main class for gathering the results after a job completes execution.
	 * 
	 * Usage: bin/hadoop edu.duke.starfish.profile.utils.GatherResults <job_id>
	 * 
	 * Note: You must add the starfish-*-profiler.jar file in the classpath
	 * 
	 * @param args
	 *            the job id
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 1 && args.length != 2) {
			System.err.println("Usage 1: bin/hadoop "
					+ GatherResults.class.getName() + " <job_id>");
			System.err.println("Usage 2: bin/hadoop "
					+ GatherResults.class.getName()
					+ " <first_job_id> <last_job_id>");
			System.exit(-1);
		}

		// Load profiling properties
		Configuration conf = new Configuration();
		Profiler.loadProfilingSystemProperties(conf);
		String outputDir = conf.get(Profiler.PROFILER_OUTPUT_DIR);

		// Get the local Hadoop history directory
		String localHistory = conf.get(
				HADOOP_LOCAL_JOB_HISTORY,
				"file:///"
						+ new File(System.getProperty(HADOOP_LOG_DIR))
								.getAbsolutePath());
		Path localHistoryDir = new Path(localHistory);
		FileSystem fs = localHistoryDir.getFileSystem(conf);
		if (!fs.exists(localHistoryDir)) {
			System.err.println("ERROR: Unable to find the logs directory!");
			System.exit(-1);
		}

		// Get the first job id
		String baseJobId = null;
		int firstJobId = 0;
		Matcher matcher = Pattern.compile("(job_[0-9]+_)([0-9]+)").matcher(
				args[0]);
		if (matcher.find()) {
			baseJobId = matcher.group(1);
			firstJobId = Integer.parseInt(matcher.group(2));
		} else {
			System.err.println("Invalid job id: " + args[0]);
			System.exit(-1);
		}

		// Get the second job id
		int secondJobId = 0;
		if (args.length == 2 && args[1].length() != 0) {
			matcher = Pattern.compile(baseJobId + "([0-9]+)").matcher(args[1]);
			if (matcher.find()) {
				secondJobId = Integer.parseInt(matcher.group(1));
			} else {
				System.err.println("Invalid job id: " + args[1]);
				System.exit(-1);
			}
		} else {
			secondJobId = firstJobId;
		}

		NumberFormat nf = NumberFormat.getIntegerInstance();
		nf.setGroupingUsed(false);
		nf.setMinimumIntegerDigits(4);
		nf.setMaximumIntegerDigits(4);

		for (int id = firstJobId; id <= secondJobId; ++id) {

			// Get the local job configuration file
			String jobId = baseJobId + nf.format(id);
			Path confFile = new Path(localHistoryDir, jobId + "_conf.xml");

			if (fs.exists(confFile)) {
				// Gather the result files
				Configuration jobConf = new Configuration(conf);
				jobConf.addResource(confFile);
				Profiler.gatherJobExecutionFiles(jobConf, new File(outputDir));
				System.out.println("Gathered the execution files for " + jobId);

			} else {
				System.err
						.println("ERROR: Unable to find the configuration file "
								+ confFile.toString());
			}
		}

		System.out.println("Output location: " + outputDir);
	}

}
