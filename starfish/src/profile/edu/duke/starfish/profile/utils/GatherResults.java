package edu.duke.starfish.profile.utils;

import static edu.duke.starfish.profile.utils.Constants.HADOOP_LOCAL_JOB_HISTORY;
import static edu.duke.starfish.profile.utils.Constants.HADOOP_LOG_DIR;

import java.io.File;
import java.io.IOException;

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

		if (args.length != 1) {
			System.err.println("Usage: bin/hadoop "
					+ GatherResults.class.getName() + " <job_id>");
			System.exit(-1);
		}

		// Load profiling properties
		Configuration conf = new Configuration();
		Profiler.loadProfilingSystemProperties(conf);

		// Get the local Hadoop history directory
		String localHistory = conf.get(HADOOP_LOCAL_JOB_HISTORY, "file:///"
				+ new File(System.getProperty(HADOOP_LOG_DIR))
						.getAbsolutePath());
		Path localHistoryDir = new Path(localHistory);
		FileSystem fs = localHistoryDir.getFileSystem(conf);
		if (!fs.exists(localHistoryDir)) {
			System.err.println("ERROR: Unable to find the logs directory!");
			System.exit(-1);
		}

		// Get the local job configuration file
		Path confFile = new Path(localHistoryDir, args[0] + "_conf.xml");
		if (!fs.exists(confFile)) {
			System.err.println("ERROR: Unable to find the configuration file "
					+ confFile.toString());
			System.exit(-1);
		}

		// Gather the result files
		conf.addResource(confFile);
		String outputDir = conf.get(Profiler.PROFILER_OUTPUT_DIR);
		Profiler.gatherJobExecutionFiles(conf, outputDir);
	}

}
