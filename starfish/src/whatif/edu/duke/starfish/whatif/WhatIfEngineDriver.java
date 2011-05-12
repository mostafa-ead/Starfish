package edu.duke.starfish.whatif;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.utils.XMLClusterParser;
import edu.duke.starfish.profile.profiler.XMLProfileParser;
import edu.duke.starfish.whatif.WhatIfEngine.WhatIfQuestion;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.FixedInputSpecsDataSetModel;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.data.RealAvgDataSetModel;
import edu.duke.starfish.whatif.data.XMLInputSpecsParser;

/**
 * The What-if Engine Driver provides a basic terminal UI for answering what-if
 * questions.
 * 
 * <pre>
 * Usage:
 *  bin/hadoop jar starfish_whatif.jar &lt;parameters&gt;
 * 
 * The what-if parameters must be one of:
 *   -mode {job_time|details|profile|timeline|mappers|reducers}
 *        -profile &lt;file&gt; -conf &lt;file&gt; [-output &lt;file&gt;]
 *   
 *   -mode {job_time|details|profile|timeline|mappers|reducers}
 *        -profile &lt;file&gt; -input &lt;file&gt; -cluster &lt;file&gt;
 *        [-conf &lt;file&gt; -output &lt;file&gt;]
 *   
 *   -mode {cluster_info|cluster_xml} [-ouput &lt;file&gt;]
 *   
 *   -mode input_specs -conf &lt;file&gt; [-ouput &lt;file&gt;]
 *   
 *   -help
 * 
 * Description of execution modes:
 *   job_time     Display the execution time of the predicted job
 *   details      Display the statistics of the predicted job
 *   profile      Display the predicted profile of the job
 *   timeline     Display the timeline of the predicted job
 *   mappers      Display the mappers of the predicted job
 *   reducers     Display the reducers of the predicted job
 *   cluster_info Display the cluster information
 *   cluster_xml  Display the cluster information as XML
 *   input_specs  Display the input specifications as XML
 * 
 * Description of parameter flags:
 *   -mode &lt;option&gt;   The execution mode
 *   -profile &lt;file&gt;  The job profile (XML file)
 *   -conf &lt;file&gt;     The job configuration file (XML file)
 *   -input &lt;file&gt;    The input specifications file (XML file)
 *   -cluster &lt;file&gt;  The cluster specifications file (XML file)
 *   -output &lt;file&gt;   An optional file to write the output to
 *   -help            Display detailed instructions
 * 
 * </pre>
 * 
 * @author hero
 * 
 */
public class WhatIfEngineDriver {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Main parsing options
	private static String MODE = "mode";
	private static String PROFILE = "profile";
	private static String CONF = "conf";
	private static String INPUT = "input";
	private static String CLUSTER = "cluster";
	private static String OUTPUT = "output";
	private static String HELP = "help";

	// Mode options
	private static String CLUSTER_INFO = "cluster_info";
	private static String CLUSTER_XML = "cluster_xml";
	private static String INPUT_SPECS = "input_specs";

	/* ***************************************************************
	 * MAIN DRIVER
	 * ***************************************************************
	 */

	/**
	 * Main what-if driver that exposes a basic UI for printing information
	 * about predicted MR jobs
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

		// Process the cluster option
		String mode = line.getOptionValue(MODE);
		if (mode.equals(CLUSTER_INFO) || mode.equals(CLUSTER_XML)) {
			Configuration conf = new Configuration(true);
			ClusterConfiguration cluster = new ClusterConfiguration(conf);
			if (mode.equals(CLUSTER_INFO))
				cluster.printClusterConfiguration(out);
			else
				XMLClusterParser.exportCluster(cluster, out);
			out.close();
			return;
		}

		// Process the input specs option
		if (mode.equals(INPUT_SPECS)) {
			Configuration conf = new Configuration(true);
			conf.addResource(new Path(line.getOptionValue(CONF)));
			DataSetModel model = new RealAvgDataSetModel();
			XMLInputSpecsParser.exportMapInputSpecs(model
					.generateMapInputSpecs(conf), out);
			out.close();
			return;
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

		// Get the job profile
		MRJobProfile sourceProf = XMLProfileParser.importJobProfile(new File(
				line.getOptionValue(PROFILE)));

		// Answer the what-if question
		WhatIfEngine.answerWhatIfQuestion(WhatIfQuestion.getQuestion(mode),
				sourceProf, dataModel, cluster, conf, out);

		out.close();
	}

	/* ***************************************************************
	 * PRIVATE STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Specify properties of each what-if option
	 * 
	 * @return the options
	 */
	@SuppressWarnings("static-access")
	private static Options buildWhatifOptions() {

		// Build the options
		Option modeOption = OptionBuilder.withArgName(MODE).hasArg()
				.withDescription("Execution mode options").create(MODE);
		Option profileOption = OptionBuilder.withArgName(PROFILE).hasArg()
				.withDescription("The job profile file").create(PROFILE);
		Option confOption = OptionBuilder.withArgName(CONF).hasArg()
				.withDescription("The job configuration file").create(CONF);
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
		Options opts = buildWhatifOptions();
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

		// -mode {job_time|details|profile|timeline|mappers|reducers}
		// -profile <file> -conf <file> [-ouput <file>]
		// OR
		// -mode {full|smart_full|rrs|smart_rrs}
		// -profile <file> -input <file> -cluster <file>
		// [-conf <file> -ouput <file>]
		if (WhatIfQuestion.isValid(mode)) {

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

		} else if (mode.equals(CLUSTER_INFO) || mode.equals(CLUSTER_XML)) {
			// -mode {cluster_info|cluster_xml}

		} else if (mode.equals(INPUT_SPECS)) {
			// -mode input_specs -conf <file>

			if (!line.hasOption(CONF)) {
				System.err.println("The 'conf' option is required");
				printUsage(System.err);
				System.exit(-1);
			} else {
				ensureFileExists(line.getOptionValue(CONF));
			}

		} else {
			System.err.println("The mode option is not supported: " + mode);
			printUsage(System.err);
			System.exit(-1);
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
	 *            flag whether to print out details or not
	 */
	private static void printUsage(PrintStream out, boolean details) {

		out.println();
		out.println("Usage:");
		out.println(" bin/hadoop jar starfish_whatif.jar <parameters>");
		out.println("");
		out.println("The profiler parameters must be one of:");
		out
				.println("  -mode {job_time|details|profile|timeline|mappers|reducers}");
		out.println("       -profile <file> -conf <file> [-output <file>]");
		out.println("");
		out
				.println("  -mode {job_time|details|profile|timeline|mappers|reducers}");
		out.println("       -profile <file> -input <file> -cluster <file>");
		out.println("       [-conf <file> -output <file>]");
		out.println("");
		out.println("  -mode {cluster_info|cluster_xml} [-ouput <file>]");
		out.println("");
		out.println("  -mode input_specs -conf <file> [-ouput <file>]");
		out.println("");
		out.println("  -help");
		out.println("");

		// Stop here if no details requested
		if (details == false)
			return;

		out.println("Description of execution modes:");
		out.println("  job_time     "
				+ "Display the execution time of the predicted job");
		out.println("  details      "
				+ "Display the statistics of the predicted job");
		out.println("  profile      Display the predicted profile of the job");
		out.println("  timeline     Display the timeline of the predicted job");
		out.println("  mappers      Display the mappers of the predicted job");
		out.println("  reducers     Display the reducers of the predicted job");
		out.println("  cluster_info Display the cluster information");
		out.println("  cluster_xml  Display the cluster information as XML");
		out.println("  input_specs  Display the input specifications as XML");
		out.println("");
		out.println("Description of parameter flags:");
		out.println("  -mode <option>   " + "The execution mode");
		out.println("  -profile <file>  " + "The job profile (XML file)");
		out.println("  -conf <file>     "
				+ "The job configuration file (XML file)");
		out.println("  -input <file>    "
				+ "The input specifications file (XML file)");
		out.println("  -cluster <file>  "
				+ "The cluster specifications file (XML file)");
		out.println("  -output <file>   "
				+ "An optional file to write the output to");
		out.println("  -help            " + "Display detailed instructions");
		out.println();

	}
}
