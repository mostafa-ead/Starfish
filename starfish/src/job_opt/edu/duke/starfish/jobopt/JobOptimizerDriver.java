package edu.duke.starfish.jobopt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.duke.starfish.jobopt.optimizer.JobOptimizer;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.utils.XMLClusterParser;
import edu.duke.starfish.profile.profiler.XMLProfileParser;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.FixedInputSpecsDataSetModel;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.data.RealAvgDataSetModel;
import edu.duke.starfish.whatif.data.XMLInputSpecsParser;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.BasicFIFOScheduler;
import edu.duke.starfish.whatif.scheduler.IWhatIfScheduler;

/**
 * The Job Optimizer Driver provides a basic terminal UI for finding the best
 * configuration for a Map-Reduce job.
 * 
 * <pre>
 * Usage:
 *  bin/hadoop jar starfish_job_optimizer.jar &lt;parameters&gt;
 * 
 *  The optimizer's parameters must be one of:
 *  
 *   -profile &lt;file&gt; -conf &lt;file&gt;
 *      [-mode {full|smart_full|rrs|smart_rrs}]
 *      [-scheduler {basic|advanced} -output &lt;file&gt;]
 *   
 *   -profile &lt;file&gt; -input &lt;file&gt; -cluster &lt;file&gt;
 *      [-mode {full|smart_full|rrs|smart_rrs}]
 *      [-conf &lt;file&gt; -scheduler {basic|advanced} -output &lt;file&gt;]
 *   
 *   -help
 * 
 * Description of optimization modes:
 *   full        The optimizer enumerates the full parameter space
 *   smart_full  The optimizer uses domain knowledge and the full space
 *   rrs         The optimizer uses Recursive Random Search (RRS)
 *   smart_rrs   The optimizer uses domain knowledge and the RRS
 * 
 * Description of parameter flags:
 *   -profile &lt;file&gt;  The job profile (XML file)
 *   -conf &lt;file&gt;     The job configuration file (XML file)
 *   -input &lt;file&gt;    The input specifications file (XML file)
 *   -cluster &lt;file&gt;  The cluster specifications file (XML file)
 *   -mode &lt;option&gt;   The optimization mode
 *   -scheduler       The task scheduler to use (basic, advanced)
 *   -output &lt;file&gt;   An optional file to write the output to
 *   -help            Display detailed instructions
 * 
 * Configuration options for all modes:
 * <ul>
 * <li>starfish.job.optimizer.exclude.parameters ()</li>
 * </ul>
 * 
 * Configuration options for mode 'full' and 'smart_full':
 * <ul>
 *   <li>starfish.job.optimizer.num.values.per.param (2)</li>
 *   <li>starfish.job.optimizer.use.random.values (false)</li>
 * </ul>
 *   
 * Configuration options for mode 'rrs' and 'smart_rrs':
 * <ul>
 *   <li>starfish.job.optimizer.explore.confidence.prob (0.99)</li>
 *   <li>starfish.job.optimizer.explore.percentile (0.1)</li>
 *   <li>starfish.job.optimizer.exploit.confidence.prob (0.99)</li>
 *   <li>starfish.job.optimizer.exploit.expected.value (0.8)</li>
 *   <li>starfish.job.optimizer.exploit.reduction.ratio (0.5)</li>
 *   <li>starfish.job.optimizer.exploit.termination.size (0.001)</li>
 * </ul>
 * 
 * 
 * </pre>
 * 
 * @author hero
 */
public class JobOptimizerDriver {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Main parsing options
	private static String PROFILE = "profile";
	private static String CONF = "conf";
	private static String INPUT = "input";
	private static String CLUSTER = "cluster";
	private static String MODE = "mode";
	private static String SCHEDULER = "scheduler";
	private static String OUTPUT = "output";
	private static String HELP = "help";

	// Mode options
	private static final String OPT_FULL = "full";
	private static final String OPT_SMART_FULL = "smart_full";
	private static final String OPT_RRS = "rrs";
	private static final String OPT_SMART_RRS = "smart_rrs";

	// Scheduler options
	private static final String SCH_BASIC = "basic";
	private static final String SCH_ADVANCED = "advanced";

	private static final Log LOG = LogFactory.getLog(JobOptimizerDriver.class);

	/* ***************************************************************
	 * MAIN DRIVER
	 * ***************************************************************
	 */

	/**
	 * Main Job Optimizer driver that exposes a basic UI for finding the best
	 * configuration parameters
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// Get the input arguments
		CommandLine line = parseAndValidateInput(args);

		// Print out instructions details if asked for
		if (line.hasOption(HELP)) {
			printUsage(System.out, true);
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

		// Get the configuration file
		Configuration conf = null;
		if (line.hasOption(CONF)) {
			conf = new Configuration(true);
			conf.addResource(new Path(line.getOptionValue(CONF)));
		} else {
			conf = new Configuration(false);
		}

		// Get the data input specifications
		DataSetModel dataModel = null;
		if (line.hasOption(INPUT)) {
			List<MapInputSpecs> specs = XMLInputSpecsParser
					.importMapInputSpecs(new File(line.getOptionValue(INPUT)));
			dataModel = new FixedInputSpecsDataSetModel(specs);
		} else {
			dataModel = new RealAvgDataSetModel();
		}

		// Get the cluster information
		ClusterConfiguration cluster = null;
		if (line.hasOption(CLUSTER)) {
			cluster = XMLClusterParser.importCluster(new File(line
					.getOptionValue(CLUSTER)));
		} else {
			cluster = new ClusterConfiguration(conf);
		}

		// Get the task scheduler
		IWhatIfScheduler scheduler = null;
		if (line.hasOption(SCHEDULER)) {
			scheduler = JobOptimizer.getTaskScheduler(line
					.getOptionValue(SCHEDULER));
		} else {
			scheduler = new BasicFIFOScheduler();
		}

		// Get the job profile
		MRJobProfile sourceProf = XMLProfileParser.importJobProfile(new File(
				line.getOptionValue(PROFILE)));

		// Get the job profile oracle
		JobProfileOracle jobOracle = new JobProfileOracle(sourceProf);

		// Get the appropriate optimizer
		String mode = OPT_SMART_RRS;
		if (line.hasOption(MODE)) {
			mode = line.getOptionValue(MODE);
		}
		JobOptimizer optimizer = JobOptimizer.getJobOptimizer(mode, jobOracle,
				dataModel, cluster, scheduler);

		// Find the best configuration
		long start = System.currentTimeMillis();
		Configuration bestConf = optimizer.findBestConfiguration(conf, false);
		long end = System.currentTimeMillis();
		LOG.info("Job optimization time (ms): " + (end - start));
		try {
			bestConf.writeXml(out);
			out.println();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/* ***************************************************************
	 * PRIVATE STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Specify properties of each optimizer option
	 * 
	 * @return the options
	 */
	@SuppressWarnings("static-access")
	private static Options buildOptimizerOptions() {

		// Build the options
		Option modeOption = OptionBuilder.withArgName(MODE).hasArg()
				.withDescription("Optimization mode options").create(MODE);
		Option profileOption = OptionBuilder.withArgName(PROFILE).hasArg()
				.withDescription("The job profile file").create(PROFILE);
		Option confOption = OptionBuilder.withArgName(CONF).hasArg()
				.withDescription("The job configuration file").create(CONF);
		Option schedulerOption = OptionBuilder.withArgName(SCHEDULER).hasArg()
				.withDescription("The taskScheduler").create(SCHEDULER);
		Option inputOption = OptionBuilder.withArgName(INPUT).hasArg()
				.withDescription("The input specifications file").create(INPUT);
		Option clusterOption = OptionBuilder.withArgName(CLUSTER).hasArg()
				.withDescription("The cluster specifications file").create(
						CLUSTER);

		Option outputOption = OptionBuilder.withArgName("filepath").hasArg()
				.withDescription("An output file to print to").create(OUTPUT);
		Option helpOption = OptionBuilder.withArgName("help").create(HELP);

		// Declare the options
		Options opts = new Options();
		opts.addOption(modeOption);
		opts.addOption(profileOption);
		opts.addOption(confOption);
		opts.addOption(schedulerOption);
		opts.addOption(inputOption);
		opts.addOption(clusterOption);
		opts.addOption(outputOption);
		opts.addOption(helpOption);

		return opts;
	}

	/**
	 * Ensure the file exists otherwise exit the application
	 * 
	 * @param fileName
	 *            the file name
	 */
	private static void ensureFileExists(String fileName) {
		File file = new File(fileName);
		if (!file.exists()) {
			System.err.println("The file does not exist: "
					+ file.getAbsolutePath());
			System.exit(-1);
		}
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
		Options opts = buildOptimizerOptions();
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

		// -profile <file> -conf <file>
		// OR
		// -profile <file> -input <file> -cluster <file> [-conf <file>]

		// The profile option is required
		if (!line.hasOption(PROFILE)) {
			System.err.println("The 'profile' option is required");
			printUsage(System.err);
			System.exit(-1);
		} else {
			ensureFileExists(line.getOptionValue(PROFILE));
		}

		// Check for the options input and cluster
		if (line.hasOption(INPUT) && line.hasOption(CLUSTER)) {
			ensureFileExists(line.getOptionValue(INPUT));
			ensureFileExists(line.getOptionValue(CLUSTER));
		} else if (line.hasOption(INPUT) || line.hasOption(CLUSTER)) {
			System.err.println("The options 'input' and 'cluster' "
					+ "must appear together");
			printUsage(System.err);
			System.exit(-1);
		}

		// The conf is optional with options input and cluster
		if (line.hasOption(CONF)) {
			ensureFileExists(line.getOptionValue(CONF));
		} else if (!line.hasOption(INPUT) || !line.hasOption(CLUSTER)) {
			System.err.println("The 'conf' option is required");
			printUsage(System.err);
			System.exit(-1);
		}

		// The mode is optional. Available options:
		// {full|smart_full|rrs|smart_rrs}
		if (line.hasOption(MODE)) {
			String mode = line.getOptionValue(MODE);
			if (!mode.equals(OPT_FULL) && !mode.equals(OPT_SMART_FULL)
					&& !mode.equals(OPT_RRS) && !mode.equals(OPT_SMART_RRS)) {
				System.err.println("The mode option is not supported: " + mode);
				printUsage(System.err);
				System.exit(-1);
			}

		}

		// The scheduler is optional. Available options: {basic|advanced}
		if (line.hasOption(SCHEDULER)) {
			if (!line.getOptionValue(SCHEDULER).equals(SCH_BASIC)
					&& !line.getOptionValue(SCHEDULER).equals(SCH_ADVANCED)) {
				System.err.println("The only supported scheduler options "
						+ "are 'basic' and 'advanced'");
				printUsage(System.err);
				System.exit(-1);
			}
		}

		return line;
	}

	/**
	 * Print the usage message.
	 * 
	 * @param out
	 *            stream to print the usage message to.
	 */
	private static void printUsage(PrintStream out) {
		printUsage(out, false);
	}

	/**
	 * Print the usage message.
	 * 
	 * @param out
	 *            stream to print the usage message to.
	 * @param details
	 *            flag to print more details
	 */
	private static void printUsage(PrintStream out, boolean details) {

		out.println();
		out.println("Usage:");
		out.println(" bin/hadoop jar starfish_job_optimizer.jar <parameters>");
		out.println("");
		out.println(" The optimizer's parameters must be one of:");
		out.println("   -profile <file> -conf <file>");
		out.println("       [-mode {full|smart_full|rrs|smart_rrs}]");
		out.println("       [-scheduler {basic|advanced} -output <file>]");
		out.println("");
		out.println("   -profile <file> -input <file> -cluster <file>");
		out.println("       [-mode {full|smart_full|rrs|smart_rrs}]");
		out.println("       [-conf <file> "
				+ "-scheduler {basic|advanced} -output <file>]");
		out.println("");
		out.println("  -help");
		out.println("");
		out.println("Description of optimization modes:");
		out.println("  full        "
				+ "The optimizer enumerates the full parameter space");
		out.println("  smart_full  "
				+ "The optimizer uses domain knowledge and the full space");
		out.println("  rrs         "
				+ "The optimizer uses Recursive Random Search (RRS)");
		out.println("  smart_rrs   "
				+ "The optimizer uses domain knowledge and the RRS");
		out.println("");
		out.println("Description of parameter flags:");
		out.println("  -profile <file>  " + "The job profile (XML file)");
		out.println("  -conf <file>     "
				+ "The job configuration file (XML file)");
		out.println("  -input <file>    "
				+ "The input specifications file (XML file)");
		out.println("  -cluster <file>  "
				+ "The cluster specifications file (XML file)");
		out.println("  -mode <option>   " + "The optimization mode");
		out.println("  -scheduler       "
				+ "The task scheduler to use (basic, advanced)");
		out.println("  -output <file>   "
				+ "An optional file to write the output to");
		out.println("  -help            " + "Display detailed instructions");
		out.println();

		if (details) {
			out.println("Configuration options for all modes");
			out.println("  starfish.job.optimizer.exclude.parameters ()");
			out.println("");
			out
					.println("Configuration options for mode 'rrs' and 'smart_rrs':");
			out.println("  starfish.job.optimizer.num.values.per.param (2)");
			out.println("  starfish.job.optimizer.use.random.values (false)");
			out.println("");
			out
					.println("Configuration options for mode 'rrs' and 'smart_rrs':");
			out.println("");
			out
					.println("  starfish.job.optimizer.explore.confidence.prob (0.99)");
			out.println("  starfish.job.optimizer.explore.percentile (0.1)");
			out
					.println("  starfish.job.optimizer.exploit.confidence.prob (0.99)");
			out
					.println("  starfish.job.optimizer.exploit.expected.value (0.8)");
			out
					.println("  starfish.job.optimizer.exploit.reduction.ratio (0.5)");
			out
					.println("  starfish.job.optimizer.exploit.termination.size (0.001)");
			out.println("");
		}
	}

}
