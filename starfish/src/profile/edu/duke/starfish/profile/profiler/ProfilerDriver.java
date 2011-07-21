package edu.duke.starfish.profile.profiler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import edu.duke.starfish.profile.profiler.loaders.SysStatsLoader;
import edu.duke.starfish.profile.utils.GeneralUtils;
import edu.duke.starfish.profile.utils.ProfileUtils;
import edu.duke.starfish.profile.utils.XMLProfileParser;

/**
 * This is the main profiler driver class. It provides a basic terminal UI for
 * displaying information regarding past MR job executions.
 * 
 * <pre>
 * Usage:
 *  bin/hadoop jar starfish-*-profiler.jar &lt;parameters&gt;
 * 
 * The profiler parameters must be one of:
 *   -mode list_all   -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   -mode list_stats -results &lt;dir&gt; [-ouput &lt;file&gt;]
 * 
 *   -mode details   -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   -mode cluster   -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   -mode timeline  -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   -mode mappers   -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   -mode reducers  -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 * 
 *   -mode transfers_all -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   -mode transfers_map -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   -mode transfers_red -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *  
 *   -mode profile     -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   -mode profile_xml -job &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 *   
 *   -mode adjust    -job1 &lt;job_id&gt; -job2 &lt;job_id&gt; -results &lt;dir&gt; [-ouput &lt;file&gt;]
 * 
 *   -mode cpustats  -monitor &lt;dir&gt; -node <node_name> 
 *     [-job &lt;job_id&gt; -results &lt;dir&gt;] [-output &lt;file&gt;]
 *   -mode memstats  -monitor &lt;dir&gt; -node <node_name> 
 *     [-job &lt;job_id&gt; -results &lt;dir&gt;] [-output &lt;file&gt;]
 *   -mode iostats   -monitor &lt;dir&gt; -node <node_name> 
 *     [-job &lt;job_id&gt; -results &lt;dir&gt;] [-output &lt;file&gt;]
 * 
 * Description of execution modes:
 *   list_all      List all available jobs
 *   list_stats    List stats for all available jobs
 *   details       Display the details of a job
 *   cluster       Display the cluster information
 *   timeline      Generate timeline of tasks
 *   mappers       Display mappers information of a job
 *   reducers      Display reducers information of a job
 *   transfers_all Display all data transfers of a job
 *   transfers_map Display aggregated data transfers from maps
 *   transfers_red Display aggregated data transfers to reducers
 *   profile       Display the profile of a job
 *   profile_xml   Display the profile of a job in an XML format
 *   adjust        Adjusts the compression costs for two MR jobs
 *   cpustats      Display CPU stats of a node
 *   memstats      Display Memory stats of a node
 *   iostats       Display I/O stats of a node
 * 
 * Description of parameter flags:
 *   -mode &lt;option&gt;    The execution mode
 *   -job &lt;job_id&gt;     The job id of interest
 *   -results &lt;dir&gt;    The results directory generated from profiling
 *   -monitor &lt;dir&gt;    The directory with the monitoring files
 *   -node &lt;node_name$gt; The node name of interest (for monitor info)
 *   -job1 &lt;job_id&gt;    The job id for job run without compression
 *   -job2 &lt;job_id&gt;    The job id for job run with compression
 *   -output &lt;file&gt;    An optional file to write the output to
 *   -help                   Display detailed instructions
 * 
 * </pre>
 * 
 * @author hero
 */
public class ProfilerDriver {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Main parsing options
	private static String MODE = "mode";
	private static String JOB = "job";
	private static String RESULTS = "results";
	private static String MONITOR = "monitor";
	private static String NODE = "node";
	private static String JOB1 = "job1";
	private static String JOB2 = "job2";
	private static String OUTPUT = "output";
	private static String HELP = "help";

	// Mode options
	private static String LIST_ALL = "list_all";
	private static String LIST_STATS = "list_stats";
	private static String DETAILS = "details";
	private static String CLUSTER = "cluster";
	private static String TIMELINE = "timeline";
	private static String MAPPERS = "mappers";
	private static String REDUCERS = "reducers";
	private static String TRANSFERS_ALL = "transfers_all";
	private static String TRANSFERS_MAP = "transfers_map";
	private static String TRANSFERS_RED = "transfers_red";
	private static String PROFILE = "profile";
	private static String PROFILE_XML = "profile_xml";
	private static String ADJUST = "adjust";
	private static String CPU_STATS = "cpustats";
	private static String MEM_STATS = "memstats";
	private static String IO_STATS = "iostats";

	// Other constants
	private static String TAB = "\t";

	/* ***************************************************************
	 * MAIN DRIVER
	 * ***************************************************************
	 */

	/**
	 * Main profiling driver that exposes a basic UI for printing information
	 * about MR jobs (including task information, data transfers, and profiles)
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		// Get the input arguments
		CommandLine line = parseAndValidateInput(args);

		// Print out instructions details if asked for
		if (line.hasOption(HELP)) {
			printHelp(System.out);
			System.exit(0);
		}

		// Create an output stream if needed
		PrintStream out = null;
		if (line.hasOption(OUTPUT)) {
			try {
				File outFile = new File(line.getOptionValue(OUTPUT));
				if (outFile.exists()) {
					System.err.println("The output file '" + outFile.getName()
							+ "' already exists.");
					System.err.println("Please specify a new output file.");
					System.exit(-1);
				}
				out = new PrintStream(outFile);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			out = System.out;
		}

		// Create and initialize the logs manager
		MRJobLogsManager manager = new MRJobLogsManager();
		if (line.hasOption(RESULTS))
			manager.setResultsDir(line.getOptionValue(RESULTS));

		// Get the job, if any
		MRJobInfo mrJob = null;
		if (line.hasOption(JOB)) {
			String jobId = line.getOptionValue(JOB);
			mrJob = manager.getMRJobInfo(jobId);
			if (mrJob == null) {
				System.err.println("Unable to find a job with id " + jobId);
				System.exit(-1);
			}
		}

		// Process the options
		String mode = line.getOptionValue(MODE);
		if (mode.equals(LIST_ALL)) {
			// List the job basic information
			printMRJobSummaries(out, manager.getAllMRJobInfos());

		} else if (mode.equals(LIST_STATS)) {
			// List the job statistics
			for (MRJobInfo job : manager.getAllMRJobInfos()) {
				manager.loadTaskDetailsForMRJob(job);
			}

			printMRJobStatistics(out, manager.getAllMRJobInfos());

		} else if (mode.equals(DETAILS)) {
			// Print the job details
			if (manager.loadTaskDetailsForMRJob(mrJob)) {
				ProfileUtils.printMRJobDetails(out, mrJob);
			} else {
				System.err.println("Unable to load the task details for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

		} else if (mode.equals(CLUSTER)) {
			// Print the cluster configuration
			ClusterConfiguration cluster = manager
					.getClusterConfiguration(mrJob.getExecId());
			if (cluster != null) {
				cluster.printClusterConfiguration(out);
			} else {
				System.err.println("Unable to load the cluster info for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

		} else if (mode.equals(TRANSFERS_ALL) || mode.equals(TRANSFERS_MAP)
				|| mode.equals(TRANSFERS_RED)) {

			// Load the task details first
			if (!manager.loadTaskDetailsForMRJob(mrJob)) {
				System.err.println("Unable to load the task details for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

			if (mrJob.isMapOnly()) {
				// Map-only job => no transfers
				out.println("Job " + mrJob.getExecId()
						+ " is map-only, no data transfers occurred!");

			} else if (manager.loadDataTransfersForMRJob(mrJob)) {
				// Print the data transfers
				printDataTransfers(out, mrJob, mode);

			} else if (manager.loadProfilesForMRJob(mrJob)
					&& ProfileUtils.generateDataTransfers(mrJob,
							manager.getHadoopConfiguration(mrJob.getExecId()))) {
				// Print the estimated data transfers
				System.out.println("NOTE: Transfers estimated based "
						+ "on profile information!");
				printDataTransfers(out, mrJob, mode);

			} else {
				// No data transfers found
				System.err.println("Unable to load the data transfers for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

		} else if (mode.equals(PROFILE)) {
			// Print the profile
			if (manager.loadProfilesForMRJob(mrJob)) {
				mrJob.getProfile().printProfile(out, false);
			} else {
				System.err.println("Unable to load the profile for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

		} else if (mode.equals(PROFILE_XML)) {
			// Export the profile
			if (manager.loadProfilesForMRJob(mrJob)) {
				XMLProfileParser.exportJobProfile(mrJob.getProfile(), out);
			} else {
				System.err.println("Unable to load the profile for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

		} else if (mode.equals(ADJUST)) {

			// Get profile with uncompression
			String jobId1 = line.getOptionValue(JOB1);
			MRJobInfo rmJob1 = manager.getMRJobInfo(jobId1);
			if (rmJob1 == null || !manager.loadProfilesForMRJob(rmJob1)) {
				System.err.println("Unable to find a job with id " + jobId1);
				System.exit(-1);
			}

			// Get profile with compression
			String jobId2 = line.getOptionValue(JOB2);
			MRJobInfo rmJob2 = manager.getMRJobInfo(jobId2);
			if (rmJob2 == null || !manager.loadProfilesForMRJob(rmJob2)) {
				System.err.println("Unable to find a job with id " + jobId2);
				System.exit(-1);
			}

			// Adjust the profile
			MRJobProfile[] profResult = ProfileUtils
					.adjustProfilesForCompression(rmJob1.getOrigProfile(),
							rmJob2.getOrigProfile());

			// Export the adjusted profiles
			File jobProfDir = new File(line.getOptionValue(RESULTS),
					"job_profiles");
			File profile1XML = new File(jobProfDir, "adj_profile_"
					+ line.getOptionValue(JOB1) + ".xml");
			File profile2XML = new File(jobProfDir, "adj_profile_"
					+ line.getOptionValue(JOB2) + ".xml");

			XMLProfileParser.exportJobProfile(profResult[0], profile1XML);
			XMLProfileParser.exportJobProfile(profResult[1], profile2XML);

			out.println("Completed adjusting profiles for "
					+ line.getOptionValue(JOB1) + " and "
					+ line.getOptionValue(JOB2));

		} else if (mode.equals(MAPPERS)) {
			// Print the map timings
			if (manager.loadTaskDetailsForMRJob(mrJob)) {
				ProfileUtils.printMRMapInfo(out, mrJob.getMapTasks());
			} else {
				System.err.println("Unable to load the map timings for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

		} else if (mode.equals(REDUCERS)) {
			// Print the reduce timings
			if (manager.loadTaskDetailsForMRJob(mrJob)) {
				ProfileUtils.printMRReduceInfo(out, mrJob.getReduceTasks());
			} else {
				System.err.println("Unable to load the reduce timings for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

		} else if (mode.equals(TIMELINE)) {
			// Print the timeline
			if (manager.loadTaskDetailsForMRJob(mrJob)) {
				ProfileUtils.printMRJobTimeline(out, mrJob);
			} else {
				System.err.println("Unable to load the task timeline for job "
						+ mrJob.getExecId());
				System.exit(-1);
			}

		} else if (mode.equals(CPU_STATS)) {
			// Print the CPU statistics
			SysStatsLoader loader = new SysStatsLoader(
					line.getOptionValue(MONITOR));
			String nodeName = line.getOptionValue(NODE);
			boolean success = false;

			if (mrJob != null)
				success = loader.exportCPUStats(out, nodeName,
						mrJob.getStartTime(), mrJob.getEndTime());
			else
				success = loader.exportCPUStats(out, nodeName);

			if (!success) {
				System.err.println("Unable to export the CPU stats");
				System.exit(-1);
			}

		} else if (mode.equals(MEM_STATS)) {
			// Print the memory statistics
			SysStatsLoader loader = new SysStatsLoader(
					line.getOptionValue(MONITOR));
			String nodeName = line.getOptionValue(NODE);
			boolean success = false;

			if (mrJob != null)
				success = loader.exportMemoryStats(out, nodeName,
						mrJob.getStartTime(), mrJob.getEndTime());
			else
				success = loader.exportMemoryStats(out, nodeName);

			if (!success) {
				System.err.println("Unable to export the Memory stats");
				System.exit(-1);
			}

		} else if (mode.equals(IO_STATS)) {
			// Print the IO statistics
			SysStatsLoader loader = new SysStatsLoader(
					line.getOptionValue(MONITOR));
			String nodeName = line.getOptionValue(NODE);
			boolean success = false;

			if (mrJob != null)
				success = loader.exportIOStats(out, nodeName,
						mrJob.getStartTime(), mrJob.getEndTime());
			else
				success = loader.exportIOStats(out, nodeName);

			if (!success) {
				System.err.println("Unable to export the I/O stats");
				System.exit(-1);
			}
		}

		out.close();
	}

	/* ***************************************************************
	 * PRIVATE STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Specify properties of each profiler option
	 * 
	 * @return the options
	 */
	@SuppressWarnings("static-access")
	private static Options buildProfilerOptions() {
		// Build the options
		Option modeOption = OptionBuilder.withArgName("mode").hasArg()
				.withDescription("Execution mode options").create(MODE);
		Option jobOption = OptionBuilder.withArgName("job_id").hasArg()
				.withDescription("The job id of interest").create(JOB);

		Option resultsOption = OptionBuilder.withArgName("dir").hasArg()
				.withDescription("The results directory from profiling")
				.create(RESULTS);

		Option job1Option = OptionBuilder.withArgName("job_id").hasArg()
				.withDescription("The job id without compression").create(JOB1);
		Option job2Option = OptionBuilder.withArgName("job_id").hasArg()
				.withDescription("The job id with compression").create(JOB2);

		Option monitorOption = OptionBuilder.withArgName("dir").hasArg()
				.withDescription("The directoryt with the monitoring files")
				.create(MONITOR);
		Option nodeOption = OptionBuilder.withArgName("node_name").hasArg()
				.withDescription("The node name of interest").create(NODE);

		Option outputOption = OptionBuilder.withArgName("filepath").hasArg()
				.withDescription("An output file to print to").create(OUTPUT);
		Option helpOption = OptionBuilder.withArgName("help").create(HELP);

		// Declare the options
		Options opts = new Options();
		opts.addOption(modeOption);
		opts.addOption(jobOption);
		opts.addOption(resultsOption);
		opts.addOption(monitorOption);
		opts.addOption(nodeOption);
		opts.addOption(job1Option);
		opts.addOption(job2Option);
		opts.addOption(outputOption);
		opts.addOption(helpOption);

		return opts;
	}

	/**
	 * Builds and returns a tab-separated string with information about the
	 * transfer
	 * 
	 * @param transfer
	 *            the data transfer
	 * @return a tab-separated string
	 */
	private static String getDataTransferString(DataTransfer transfer) {
		StringBuilder sb = new StringBuilder();

		sb.append(transfer.getSource().getExecId());
		sb.append(TAB);
		sb.append(transfer.getDestination().getExecId());
		sb.append(TAB);
		sb.append(transfer.getUncomprData());
		sb.append(TAB);
		sb.append(transfer.getComprData());
		sb.append(TAB);
		sb.append(transfer.getDuration());

		return sb.toString();
	}

	/**
	 * Builds and returns a tab-separated string with statistical information
	 * about the MR job
	 * 
	 * @param mrJob
	 *            the MR job
	 * @param nf
	 *            the number format for the statistics
	 * @return a tab-separated string
	 */
	private static String getMRJobStatisticsString(MRJobInfo mrJob,
			NumberFormat nf) {
		StringBuilder sb = new StringBuilder();
		double avg, dev;

		sb.append(mrJob.getExecId());
		sb.append(TAB);
		sb.append(nf.format(mrJob.getDuration()));
		sb.append(TAB);
		sb.append(mrJob.getMapTasks().size());
		sb.append(TAB);
		avg = ProfileUtils.calculateDurationAverage(mrJob.getMapTasks());
		sb.append(nf.format(avg));
		sb.append(TAB);
		dev = ProfileUtils.calculateDurationDeviation(mrJob.getMapTasks());
		sb.append(nf.format(dev));
		sb.append(TAB);
		sb.append(nf.format((avg != 0) ? dev / avg : 0));
		sb.append(TAB);
		sb.append(mrJob.getReduceTasks().size());
		sb.append(TAB);
		avg = ProfileUtils.calculateDurationAverage(mrJob.getReduceTasks());
		sb.append(nf.format(avg));
		sb.append(TAB);
		dev = ProfileUtils.calculateDurationDeviation(mrJob.getReduceTasks());
		sb.append(nf.format(dev));
		sb.append(TAB);
		sb.append(nf.format((avg != 0) ? dev / avg : 0));

		return sb.toString();
	}

	/**
	 * Builds and returns a tab-separated string with summary information about
	 * the MR job
	 * 
	 * @param mrJob
	 *            the MR job
	 * @return a tab-separated string
	 */
	private static String getMRJobSummaryString(MRJobInfo mrJob) {
		StringBuilder sb = new StringBuilder();

		sb.append(mrJob.getExecId());
		sb.append(TAB);
		sb.append(mrJob.getName());
		sb.append(TAB);
		sb.append(mrJob.getStartTime());
		sb.append(TAB);
		sb.append(mrJob.getEndTime());
		sb.append(TAB);
		sb.append(GeneralUtils.getFormattedDuration(mrJob.getDuration()));
		sb.append(TAB);
		sb.append(mrJob.getStatus());

		return sb.toString();
	}

	/**
	 * Parse and validate the input arguments
	 * 
	 * @param args
	 *            the input arguments
	 * @return the parsed command line
	 */
	private static CommandLine parseAndValidateInput(String[] args) {
		// Make sure we have some
		if (args == null || args.length == 0) {
			printUsage(System.out);
			System.exit(0);
		}

		// Parse the arguments
		Options opts = buildProfilerOptions();
		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(opts, args, true);
		} catch (ParseException e) {
			System.err.println("Unable to parse the input arguments");
			System.err.println(e.getMessage());
			printUsage(System.err);
			System.exit(-1);
		}

		// Ensure we don't have any extra input arguments
		if (line.getArgs() != null && line.getArgs().length > 0) {
			System.err.println("Unsupported input arguments:");
			for (String arg : line.getArgs()) {
				System.err.println(arg);
			}
			printUsage(System.err);
			System.exit(-1);
		}

		// If the user asked for help, nothing else to do
		if (line.hasOption(HELP)) {
			return line;
		}

		// Error checking
		if (!line.hasOption(MODE)) {
			System.err.println("The 'mode' option is required");
			printUsage(System.err);
			System.exit(-1);
		}

		String mode = line.getOptionValue(MODE);

		// -mode {list_all|list_stats} -results <dir> [-ouput <file>]
		if (mode.equals(LIST_ALL) || mode.equals(LIST_STATS)) {
			if (!line.hasOption(RESULTS)) {
				System.err.println("The 'results' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
		}

		// -mode {details|cluster|timeline|mappers|reducers}
		// -job <job_id> -results <dir> [-ouput <file>]
		// -mode {transfers_all|transfers_map|transfers_red}
		// -job <job_id> -results <dir> [-ouput <file>]
		// -mode {profile|profile_xml} -job <job_id> -results <dir>
		// [-ouput <file>]
		else if (mode.equals(DETAILS) || mode.equals(CLUSTER)
				|| mode.equals(TIMELINE) || mode.equals(MAPPERS)
				|| mode.equals(REDUCERS) || mode.equals(TRANSFERS_ALL)
				|| mode.equals(TRANSFERS_MAP) || mode.equals(TRANSFERS_RED)
				|| mode.equals(PROFILE) || mode.equals(PROFILE_XML)) {
			if (!line.hasOption(JOB)) {
				System.err.println("The 'job' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
			if (!line.hasOption(RESULTS)) {
				System.err.println("The 'results' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
		}
		// -mode adjust -job1 <job_id> -job2 <job_id> -results <dir> [-ouput
		// <file>]
		else if (mode.equals(ADJUST)) {
			if (!line.hasOption(JOB1)) {
				System.err.println("The 'job1' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
			if (!line.hasOption(JOB2)) {
				System.err.println("The 'job2' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
			if (!line.hasOption(RESULTS)) {
				System.err.println("The 'results' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
		}
		// -mode {cpustats|memstats|iostats}
		// -monitor <dir> -node <node_name>
		// [-job <job_id> -results <dir>] [-output <file>]
		else if (mode.equals(CPU_STATS) || mode.equals(MEM_STATS)
				|| mode.equals(IO_STATS)) {
			if (!line.hasOption(MONITOR)) {
				System.err.println("The 'monitor' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
			if (!line.hasOption(NODE)) {
				System.err.println("The 'node' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
		} else {
			System.err.println("The mode option is not supported: " + mode);
			printUsage(System.err);
			System.exit(-1);
		}

		return line;
	}

	/**
	 * Print out information regarding the data transfers from the successful
	 * map attempts to the successful reduce attempts. Information includes the
	 * overall data transfers, aggregated transfers from the map attempts, and
	 * aggregated transfers to the reduce attempts.
	 * 
	 * @param out
	 *            The print stream to print at
	 * @param mrJob
	 *            the MR job of interest
	 * @param mode
	 *            the transfer mode (all, from mappers, or to reducers)
	 */
	private static void printDataTransfers(PrintStream out, MRJobInfo mrJob,
			String mode) {

		// Print the overall transfers
		if (mode.equals(TRANSFERS_ALL)) {
			out.println("Source (Map Attempt)\tDestination (Reduce Attempt)"
					+ "\tUncompressed Data Size (bytes)"
					+ "\tCompressed Data Size (bytes)\tDuration (ms)");
			for (DataTransfer transfer : mrJob.getDataTransfers()) {
				out.println(getDataTransferString(transfer));
			}
		}

		// Print the aggregated transfers per mapper
		if (mode.equals(TRANSFERS_MAP)) {
			long uncompr, compr, duration;
			out.println("Map Attempt\tUncompressed Data Size (bytes)"
					+ "\tCompressed Data Size (bytes)\tTotal Duration (ms)");

			for (MRMapAttemptInfo mrMap : mrJob
					.getMapAttempts(MRExecutionStatus.SUCCESS)) {

				uncompr = compr = duration = 0l;
				for (DataTransfer transfer : mrJob
						.getDataTransfersFromMap(mrMap)) {

					uncompr += transfer.getUncomprData();
					compr += transfer.getComprData();
					duration += transfer.getDuration();
				}

				out.println(mrMap.getExecId() + TAB + uncompr + TAB + compr
						+ TAB + duration);
			}
		}

		// Print the aggregated transfers per reducer
		if (mode.equals(TRANSFERS_RED)) {
			long uncompr, compr, duration;
			out.println("Reduce Attempt\tUncompressed Data Size (bytes)"
					+ "\tCompressed Data Size (bytes)\tTotal Duration (ms)");

			for (MRReduceAttemptInfo mrReduce : mrJob
					.getReduceAttempts(MRExecutionStatus.SUCCESS)) {

				uncompr = compr = duration = 0l;
				for (DataTransfer transfer : mrJob
						.getDataTransfersToReduce(mrReduce)) {

					uncompr += transfer.getUncomprData();
					compr += transfer.getComprData();
					duration += transfer.getDuration();
				}

				out.println(mrReduce.getExecId() + TAB + uncompr + TAB + compr
						+ TAB + duration);
			}
		}

	}

	/**
	 * Prints out a list with statistical information for the list of jobs:
	 * <ul>
	 * <li>job duration</li>
	 * <li>count, average, and deviation of map tasks timings</li>
	 * <li>count, average, and deviation of reduce tasks timings</li>
	 * </ul>
	 * 
	 * @param out
	 *            The print stream to print at
	 * @param mrJobs
	 *            the MR jobs of interest
	 */
	private static void printMRJobStatistics(PrintStream out,
			List<MRJobInfo> mrJobs) {

		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumFractionDigits(2);
		nf.setMaximumFractionDigits(2);

		out.println("Job Id\tDuration (ms)\t"
				+ "Map Count\tMap Avg\tMap Std Dev\tMap Var Coeff\t"
				+ "Reduce Count\tReduce Avg\tReduce Std Dev\tReduce Var Coeff");
		for (MRJobInfo mrJob : mrJobs) {
			out.println(getMRJobStatisticsString(mrJob, nf));
		}

	}

	/**
	 * Prints out a list with summary information for the list of jobs. It
	 * produces tabular data of the form
	 * "Job Id\tJob Name\tStart Time\tEnd Time\tDuration\tStatus"
	 * 
	 * @param out
	 *            The print stream to print at
	 * @param mrJobs
	 *            the MR jobs of interest
	 */
	private static void printMRJobSummaries(PrintStream out,
			List<MRJobInfo> mrJobs) {

		out.println("Job Id\tJob Name\tStart Time\tEnd Time\tDuration\tStatus");
		for (MRJobInfo mrJob : mrJobs) {
			out.println(getMRJobSummaryString(mrJob));
		}

	}

	/**
	 * Print the usage message.
	 * 
	 * @param out
	 *            stream to print the usage message to.
	 */
	private static void printUsage(PrintStream out) {

		out.println();
		out.println("Usage:");
		out.println("  bin/hadoop jar starfish_profiler.jar <parameters>");
		out.println();
		out.println("The profiler parameters must be "
				+ "one of the following six cases:");
		out.println("  -mode {list_all|list_stats}");
		out.println("    -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode {details|cluster|timeline" + "|mappers|reducers}");
		out.println("    -job <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode {transfers_all|transfers_map|transfers_red}");
		out.println("    -job <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode {profile|profile_xml}");
		out.println("    -job <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode adjust");
		out.println("    -job1 <job_id> -job2 <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode {cpustats|memstats|iostats}");
		out.println("    -monitor <dir> -node <node_name> ");
		out.println("    [-job <job_id> -results <dir>] [-output <file>]");
		out.println();
		out.println("  -help");
		out.println();
	}

	/**
	 * Prints out detailed instructions on how to use the profiler.
	 * 
	 * @param out
	 *            the print stream to print to
	 */
	private static void printHelp(PrintStream out) {

		out.println("Usage:");
		out.println(" bin/hadoop jar starfish_profiler.jar <parameters>");
		out.println();
		out.println("The profiler parameters must be one of:");
		out.println("  -mode list_all   -results <dir> [-ouput <file>]");
		out.println("  -mode list_stats -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode details     "
				+ "-job <job_id> -results <dir> [-ouput <file>]");
		out.println("  -mode cluster     "
				+ "-job <job_id> -results <dir> [-ouput <file>]");
		out.println("  -mode timeline    "
				+ "-job <job_id> -results <dir> [-ouput <file>]");
		out.println("  -mode mappers     "
				+ "-job <job_id> -results <dir> [-ouput <file>]");
		out.println("  -mode reducers    "
				+ "-job <job_id> -results <dir> [-ouput <file>]");
		out.println("  -mode transfers   "
				+ "-job <job_id> -results <dir> [-ouput <file>]");
		out.println("  -mode profile     "
				+ "-job <job_id> -results <dir> [-ouput <file>]");
		out.println("  -mode profile_xml "
				+ "-job <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode adjust  "
				+ "-job1 <job_id> -job2 <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode cpustats  -monitor <dir> -node <node_name> ");
		out.println("     [-job <job_id> -results <dir>] [-output <file>]");
		out.println("  -mode memstats  -monitor <dir> -node <node_name> ");
		out.println("     [-job <job_id> -results <dir>] [-output <file>]");
		out.println("  -mode iostats   -monitor <dir> -node <node_name> ");
		out.println("     [-job <job_id> -results <dir>] [-output <file>]");
		out.println();
		out.println("Description of execution modes:");
		out.println("  list_all     List all available jobs");
		out.println("  list_stats   List stats for all available jobs");
		out.println("  details      Display the details of a job");
		out.println("  cluster      Display the cluster information");
		out.println("  timeline     Generate timeline of tasks");
		out.println("  mappers      Display mappers information of a job");
		out.println("  reducers     Display reducers information of a job");
		out.println("  transfers_all Display all data transfers of a job");
		out.println("  transfers_map "
				+ "Display aggregated data transfers from maps");
		out.println("  transfers_red "
				+ "Display aggregated data transfers to reducers");
		out.println("  profile      Display the profile of a job");
		out.println("  profile_xml  "
				+ "Display the profile of a job in XML format");
		out.println("  adjust       Adjusts compression costs for two jobs");
		out.println("  cpustats     Display CPU stats of a node");
		out.println("  memstats     Display Memory stats of a node");
		out.println("  iostats      Display I/O stats of a node");
		out.println();
		out.println("Description of parameter flags:");
		out.println("  -mode <option>    The execution mode");
		out.println("  -job <job_id>     The job id of interest");
		out.println("  -results <dir>    "
				+ "The results directory generated from profiling");
		out.println("  -monitor <dir>    "
				+ "The directory with the monitoring files");
		out.println("  -job1 <file>      "
				+ "The job id for job run without compression");
		out.println("  -job2 <file>      "
				+ "The job id for job run with compression");
		out.println("  -node <node_name> "
				+ "The node name of interest (for monitor info)");
		out.println("  -output <file>    "
				+ "An optional file to write the output to");
		out.println("  -help             Display detailed instructions");
		out.println();

	}
}
