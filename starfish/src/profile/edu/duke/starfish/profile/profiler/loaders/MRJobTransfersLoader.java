package edu.duke.starfish.profile.profiler.loaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;

/**
 * Represents the data transfers for a Map-Reduce job. The data transfers are
 * gathered from the logs in a lazy way i.e. only when the user tries to access
 * them.
 * 
 * @author hero
 */
public class MRJobTransfersLoader {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// PRIVATE MEMBERS
	private MRJobInfo mrJob; // The Map-Reduce job
	private String inputDir; // The userlogs or the transfers directory
	private boolean loadedData; // Whether or not the data has been loaded

	// CONSTANTS FOR PARSING
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss,SSS");

	private static final String SYSLOG = "syslog";
	private static final String TRANSFERS = "transfers_";

	private static final String INFO_SHUFFLING = "INFO org.apache.hadoop.mapred.ReduceTask: Shuffling";
	private static final String INFO_READ_SHUFFLE = "INFO org.apache.hadoop.mapred.ReduceTask: Read";
	private static final String INFO_FAILED_SHUFFLE = "INFO org.apache.hadoop.mapred.ReduceTask: Failed to shuffle from";

	private static final Pattern SHUFFLE_PATTERN = Pattern
			.compile("([\\d-:, ]+) INFO .* (\\d+) bytes \\((\\d+) raw bytes\\) .* from ([\\w\\d_]+)");
	private static final Pattern READ_PATTERN = Pattern
			.compile("([\\d-:, ]+) INFO .* for ([\\w\\d_]+)");
	private static final Pattern FAILED_PATTERN = Pattern
			.compile("([\\d-:, ]+) INFO .* from ([\\w\\d_]+)");

	/**
	 * Constructor
	 * 
	 * @param mrJob
	 *            the Map-Reduce job
	 * @param inputDir
	 *            the path to the userlogs or transfers directory
	 */
	public MRJobTransfersLoader(MRJobInfo mrJob, String inputDir) {
		this.mrJob = mrJob;
		this.inputDir = inputDir;
		this.loadedData = false;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the Map-Reduce job
	 */
	public MRJobInfo getMrJob() {
		return mrJob;
	}

	/**
	 * @return the path to the input directory
	 */
	public String getInputDir() {
		return inputDir;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Load all data transfers from the logs
	 * 
	 * @return true if all data loaded successfully
	 */
	public boolean loadDataTransfers(MRJobInfo mrJob) {

		if (!this.mrJob.getExecId().equalsIgnoreCase(mrJob.getExecId())) {
			return false;
		} else if (loadedData && this.mrJob == mrJob) {
			return true;
		}

		// Load the data
		this.mrJob = mrJob;

		// Check the userlogs directory
		File filesDir = new File(inputDir);
		if (!filesDir.isDirectory()) {
			System.err.println(filesDir.getAbsolutePath()
					+ " is not a directory!");
			return false;
		}

		// Parse the syslog for each successful reduce attempt
		boolean success = false;
		List<DataTransfer> reducerTransfer;
		for (MRReduceAttemptInfo mrReduceAttempt : mrJob
				.getReduceAttempts(MRExecutionStatus.SUCCESS)) {

			try {
				reducerTransfer = parseReducerSyslog(filesDir, mrReduceAttempt);
				if (reducerTransfer != null) {
					success = true;
					mrJob.addDataTransfers(reducerTransfer);
				}
			} catch (ParseException e) {
				e.printStackTrace();
				return false;
			}
		}

		// Loaded all data
		loadedData = success;
		return success;
	} // End loadDataTransfers

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Parses the syslog for a reduce attempt and constructs a list with all the
	 * data transfers to this reduce attempt. Only successful data transfers are
	 * included in the result.
	 * 
	 * @param logsDir
	 *            the userlogs or transfers directory with the log files
	 * @param mrReduceAttempt
	 *            the destination reduce attempt
	 * @return a list of data transfers to the reduce attempt
	 * @throws ParseException
	 */
	private List<DataTransfer> parseReducerSyslog(File logsDir,
			MRReduceAttemptInfo mrReduceAttempt) throws ParseException {

		// Build the syslog file path, which will be one of two options:
		// (a) filesDir/attemptDir/syslog
		// (b) filesDir/transfers_attempt_id
		File syslog = null;
		File attemptDir = new File(logsDir, mrReduceAttempt.getExecId());
		if (attemptDir.isDirectory())
			syslog = new File(attemptDir, SYSLOG);
		else
			syslog = new File(logsDir, TRANSFERS + mrReduceAttempt.getExecId());

		// Return null if the file does not exist
		if (!syslog.exists())
			return null;

		// Open the file for reading
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(syslog));
		} catch (FileNotFoundException e) {
			System.err.println("Unable to find file: "
					+ syslog.getAbsolutePath());
			return null;
		}

		// Keep track of the transfers as we parse the log
		HashMap<String, DataTransfer> startedTransfers = new HashMap<String, DataTransfer>();
		HashMap<String, DataTransfer> emptyTransfers = new HashMap<String, DataTransfer>();
		List<DataTransfer> completedTransfers = new ArrayList<DataTransfer>();

		try {
			// Parse the log line-by-line
			String line = br.readLine();
			while (line != null) {

				if (line.contains(INFO_SHUFFLING)) {

					// A new transfer is beginning
					Matcher matcher = SHUFFLE_PATTERN.matcher(line);
					if (matcher.find()) {
						// Find the transfer source (map task attempt)
						String mapAttemptId = matcher.group(4);
						MRMapAttemptInfo mrMapAttempt = mrJob
								.findMRMapAttempt(mapAttemptId);
						if (mrMapAttempt == null) {
							System.err.println("The map attempt id "
									+ mapAttemptId
									+ " was not found in the job");
							return null;
						}

						// Create the data transfer
						DataTransfer transfer = new DataTransfer(mrMapAttempt,
								mrReduceAttempt, Long.parseLong(matcher
										.group(3)), Long.parseLong(matcher
										.group(2)));
						transfer.setStartTime(DATE_FORMAT.parse(matcher
								.group(1)));

						// Keep track of this started data transfer
						startedTransfers.put(mapAttemptId, transfer);

						// Keep track of the transfers with no data
						if (transfer.getUncomprData() == 2) {
							emptyTransfers.put(mapAttemptId, transfer);
						}
					} else {
						System.err.println("Unexpected line format from: ");
						System.err.println(line);
					}

				} else if (line.contains(INFO_READ_SHUFFLE)) {

					// A transfer completed
					Matcher matcher = READ_PATTERN.matcher(line);
					if (matcher.find()) {
						// Find the data transfer in the map
						String mapAttemptId = matcher.group(2);
						if (emptyTransfers.containsKey(mapAttemptId)) {
							// Found an empty transfer, remove it from the cache
							emptyTransfers.remove(mapAttemptId);
							startedTransfers.remove(mapAttemptId);

						} else if (startedTransfers.containsKey(mapAttemptId)) {
							DataTransfer dataTransfer = startedTransfers
									.get(mapAttemptId);
							dataTransfer.setEndTime(DATE_FORMAT.parse(matcher
									.group(1)));

							// Mark the data transfer as completed
							startedTransfers.remove(mapAttemptId);
							completedTransfers.add(dataTransfer);
						} else {
							System.err.println("The map attempt id "
									+ mapAttemptId
									+ " has not been seen before");
							return null;
						}
					} else {
						System.err.println("Unexpected line format from: ");
						System.err.println(line);
					}

				} else if (line.contains(INFO_FAILED_SHUFFLE)) {

					// A transfer failed
					Matcher matcher = FAILED_PATTERN.matcher(line);
					if (matcher.find()) {
						// Find and remove the data transfer from the map
						String mapAttemptId = matcher.group(2);
						if (startedTransfers.containsKey(mapAttemptId)) {
							startedTransfers.remove(mapAttemptId);
							emptyTransfers.remove(mapAttemptId);
						} else {
							System.err.println("The map attempt id "
									+ mapAttemptId
									+ " has not been seen before");
							return null;
						}
					} else {
						System.err.println("Unexpected line format from: ");
						System.err.println(line);
					}
				}

				// Read the next line
				line = br.readLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		return completedTransfers;
	} // End parseReducerSyslog

}
