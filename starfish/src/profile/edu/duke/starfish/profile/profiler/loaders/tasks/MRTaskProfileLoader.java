package edu.duke.starfish.profile.profiler.loaders.tasks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;

/**
 * Base class for a task profile. This class is responsible for parsing the
 * BTrace profile files and calculating all the profile information for a task.
 * 
 * The subclasses must implement the method "loadExecutionProfile" which is
 * meant to calculate the job's statistics, cost factors, and phase timings
 * based on the listing of all the profile records from the BTrace profile file.
 * 
 * @author hero
 */
public abstract class MRTaskProfileLoader {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// DATA MEMBERS
	protected MRTaskProfile profile; // The task profile
	protected Configuration conf; // The hadoop configuration
	protected String profileFile; // the path to the profile file

	private EnumMap<ProfileToken, ArrayList<ProfileRecord>> records;
	private boolean loaded; // Whether the file is loaded or not

	// CONSTANTS
	protected static final List<ProfileRecord> EMPTY_RECORDS = new ArrayList<ProfileRecord>(
			0);
	protected static final String TAB = "\t";
	protected static final double DEFAULT_COMPR_RATIO = 0.3;
	protected static final double NS_PER_MS = 1000000d;

	// CONSTANTS USED FOR PROFILE PARSING
	protected static final String TOTAL_RUN = "TOTAL_RUN";
	protected static final String SETUP = "SETUP";
	protected static final String CLEANUP = "CLEANUP";
	protected static final String READ = "READ";
	protected static final String WRITE = "WRITE";
	protected static final String COMPRESS = "COMPRESS";
	protected static final String UNCOMPRESS = "UNCOMPRESS";
	protected static final String MAP = "MAP";
	protected static final String REDUCE = "REDUCE";
	protected static final String COMBINE = "COMBINE";
	protected static final String PARTITION_OUTPUT = "PARTITION_OUTPUT";
	protected static final String SERIALIZE_OUTPUT = "SERIALIZE_OUTPUT";
	protected static final String SORT_AND_SPILL = "SORT_AND_SPILL";
	protected static final String QUICK_SORT = "QUICK_SORT";
	protected static final String SORT_COUNT = "SORT_COUNT";
	protected static final String KEY_BYTE_COUNT = "KEY_BYTE_COUNT";
	protected static final String VALUE_BYTE_COUNT = "VALUE_BYTE_COUNT";
	protected static final String UNCOMPRESS_BYTE_COUNT = "UNCOMPRESS_BYTE_COUNT";
	protected static final String COMPRESS_BYTE_COUNT = "COMPRESS_BYTE_COUNT";
	protected static final String TRANSFER_COST = "TRANSFER_COST";
	protected static final String TOTAL_MERGE = "TOTAL_MERGE";
	protected static final String READ_WRITE = "READ_WRITE";
	protected static final String READ_WRITE_COUNT = "READ_WRITE_COUNT";
	protected static final String COPY_MAP_DATA = "COPY_MAP_DATA";
	protected static final String MERGE_MAP_DATA = "MERGE_MAP_DATA";
	protected static final String MERGE_IN_MEMORY = "MERGE_IN_MEMORY";
	protected static final String MERGE_TO_DISK = "MERGE_TO_DISK";
	protected static final String STARTUP_MEM = "STARTUP_MEM";
	protected static final String SETUP_MEM = "SETUP_MEM";
	protected static final String MAP_MEM = "MAP_MEM";
	protected static final String REDUCE_MEM = "REDUCE_MEM";
	protected static final String CLEANUP_MEM = "CLEANUP_MEM";

	/**
	 * Constructor
	 * 
	 * @param profile
	 *            the task profile to load
	 * @param conf
	 *            the hadoop configuration
	 * @param profileFile
	 *            the path to the btrace profile file
	 */
	public MRTaskProfileLoader(MRTaskProfile profile, Configuration conf,
			String profileFile) {
		this.profile = profile;
		this.conf = conf;
		this.profileFile = profileFile;
		this.records = new EnumMap<ProfileToken, ArrayList<ProfileRecord>>(
				ProfileToken.class);
		this.loaded = false;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the task profile
	 */
	public MRTaskProfile getProfile() {
		if (!loaded)
			loadExecutionProfile(profile);

		return profile;
	}

	/**
	 * @return the hadoop configuration
	 */
	public Configuration getConfiguration() {
		return conf;
	}

	/**
	 * @return the profile file
	 */
	public String getProfileFile() {
		return profileFile;
	}

	/**
	 * Load the execution profile
	 * 
	 * @param profile
	 *            the task profile to load
	 * @return true if the loading was successful
	 */
	public boolean loadExecutionProfile(MRTaskProfile profile) {

		if (!this.profile.getTaskId().equalsIgnoreCase(profile.getTaskId())) {
			return false;
		} else if (loaded && this.profile == profile) {
			return true;
		}

		// Load the profile
		this.profile = profile;
		loaded = true;

		try {
			if (parseProfileFile()) {
				return loadExecutionProfile();
			} else {
				return false;
			}
		} catch (ProfileFormatException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	/* ***************************************************************
	 * ABSTRACT METHODS
	 * ***************************************************************
	 */

	/**
	 * Load the execution profile
	 * 
	 * @throws ProfileFormatException
	 */
	protected abstract boolean loadExecutionProfile()
			throws ProfileFormatException;

	/* ***************************************************************
	 * PROTECTED METHODS
	 * ***************************************************************
	 */

	/**
	 * Find and return the profile records for this particular token.
	 * 
	 * @param token
	 *            the profile token of interest
	 * @return a list of profile records
	 */
	protected List<ProfileRecord> getProfileRecords(ProfileToken token) {
		return records.get(token);
	}

	/**
	 * This method is for records that come in groups. It aggregates the values
	 * of the profile records for a particular position in each group.
	 * 
	 * @param records
	 *            the profile records
	 * @param groupSize
	 *            the number of records in each group
	 * @param pos
	 *            the position in the group
	 * @return the aggregate value
	 */
	protected long aggregateRecordValues(List<ProfileRecord> records,
			int groupSize, int pos) {
		long aggr = 0l;

		for (int i = pos; i < records.size(); i += groupSize) {
			aggr += records.get(i).getValue();
		}

		return aggr;
	}

	/**
	 * This method is for records that come in groups. It averages the values of
	 * the profile records for a particular position in each group.
	 * 
	 * @param records
	 *            the profile records
	 * @param groupSize
	 *            the number of records in each group
	 * @param pos
	 *            the position in the group
	 * @return the aggregate value
	 */
	protected double averageRecordValues(List<ProfileRecord> records,
			int groupSize, int pos) {
		double aggr = 0d;
		int numGroups = 0;

		for (int i = pos; i < records.size(); i += groupSize) {
			aggr += records.get(i).getValue();
			++numGroups;
		}

		return aggr / numGroups;
	}

	/**
	 * This method is for records that come in groups. It calculates the average
	 * of the ratios (records[pos1] / records[pos2]) that occur in the groups of
	 * the records.
	 * 
	 * @param records
	 *            the profile records
	 * @param groupSize
	 *            the number of records in each group
	 * @param pos1
	 *            the numerator position
	 * @param pos2
	 *            the denominator position
	 * @return the average of the ratios
	 */
	protected double averageRecordValueRatios(List<ProfileRecord> records,
			int groupSize, int pos1, int pos2) {
		double sumRatios = 0d;
		int numGroups = 0;

		for (int i = 0; i < records.size(); i += groupSize) {
			sumRatios += records.get(i + pos1).getValue()
					/ (double) records.get(i + pos2).getValue();
			++numGroups;
		}

		return sumRatios / numGroups;
	}

	/**
	 * This method is for records that come in groups. It calculates the average
	 * of the ratios ((records[pos1] - records[pos2]) / records[pos2]) that
	 * occur in the groups of the records.
	 * 
	 * @param records
	 *            the profile records
	 * @param groupSize
	 *            the number of records in each group
	 * @param pos1
	 *            the first numerator position
	 * @param pos2
	 *            the second numerator position to subtract
	 * @param pos3
	 *            the denominator position
	 * @return the average of the ratios
	 */
	protected double averageProfileValueDiffRatios(List<ProfileRecord> records,
			int groupSize, int pos1, int pos2, int pos3) {
		double sumRatios = 0d;
		int numGroups = 0;

		for (int i = 0; i < records.size(); i += groupSize) {
			sumRatios += (records.get(i + pos1).getValue() - records.get(
					i + pos2).getValue())
					/ (double) records.get(i + pos3).getValue();
			++numGroups;
		}

		return sumRatios / numGroups;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Parses the profile file and creates a list of all the profile records
	 * grouped based on the profile tokens
	 * 
	 * @return true if successful
	 * @throws ProfileFormatException
	 *             if the file is not correctly formatted
	 */
	private boolean parseProfileFile() throws ProfileFormatException {

		// Open the file for reading
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(new File(profileFile)));
		} catch (FileNotFoundException e) {
			System.err.println("Unable to find file: " + profileFile);
			return false;
		}

		// Parse the log line-by-line and gather the profile records
		String line = "";
		String[] tokens;
		ProfileToken token;
		try {
			line = br.readLine();
			while (line != null) {

				// Check for and ignore a memory profile
				tokens = line.split(TAB);
				if (tokens[0].equals(SETUP))
					return false;

				// Create the profile records
				token = ProfileToken.valueOf(tokens[0]);
				if (!records.containsKey(token))
					records.put(token, new ArrayList<ProfileRecord>());

				records.get(token).add(
						new ProfileRecord(token, tokens[1], Long
								.parseLong(tokens[2])));

				line = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} catch (RuntimeException e) {
			throw new ProfileFormatException("Invalid profile line: " + line, e);
		}

		return true;
	}

	/* ***************************************************************
	 * HELPER CLASSES
	 * ***************************************************************
	 */

	/**
	 * Represent a profiling record of the form <Token Process Value>. A typical
	 * example is a timing for a task phase. eg. <MAPPER CLEANUP 28635>. The
	 * timing is in terms of nanoseconds.
	 * 
	 * The natural order of profile records is based only on the token. Hence,
	 * when a list of ProfileRecord objects is sorted, the elements are
	 * essentially grouped together based on the tokens.
	 * 
	 * @author hero
	 */
	public static class ProfileRecord implements Comparable<ProfileRecord> {

		/* ***************************************************************
		 * DATA MEMBERS
		 * ***************************************************************
		 */

		private ProfileToken token;
		private String process;
		private long value;

		/**
		 * Constructor
		 * 
		 * @param phase
		 * @param process
		 * @param value
		 */
		public ProfileRecord(ProfileToken phase, String process, long value) {
			this.token = phase;
			this.process = process;
			this.value = value;
		}

		/* ***************************************************************
		 * OVERRIDEN METHODS
		 * ***************************************************************
		 */

		@Override
		public int compareTo(ProfileRecord other) {
			return this.token.compareTo(other.token);
		}

		@Override
		public int hashCode() {
			return token.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof ProfileRecord))
				return false;
			ProfileRecord other = (ProfileRecord) obj;
			return token.equals(other.getPhase());
		}

		@Override
		public String toString() {
			return "ProfileRecord [token=" + token + ", process=" + process
					+ ", value=" + value + "]";
		}

		/* ***************************************************************
		 * PUBLIC METHODS
		 * ***************************************************************
		 */

		/**
		 * @return the token
		 */
		public ProfileToken getPhase() {
			return token;
		}

		/**
		 * @return the process
		 */
		public String getProcess() {
			return process;
		}

		/**
		 * @return the value
		 */
		public long getValue() {
			return value;
		}

	}

	/**
	 * Enumerates the different tokens used in the profile records as produces
	 * by the btrace scripts. They loosely correspond to MR task sub-phases.
	 * 
	 * @author hero
	 */
	public static enum ProfileToken {

		TASK, // General task information
		MAP, // The map phase in the map task
		SPILL, // The spill phase in the map task (sort plus spill)
		MERGE, // The merge phase in the map task
		SHUFFLE, // The shuffle phase in the reduce task
		SORT, // The sort phase in the reduce task
		REDUCE, // The reduce phase in the reduce task
		MEMORY, // The memory traces
	}

	/**
	 * An exception to wrap a problem with the format of the BTrace profile file
	 * 
	 * @author hero
	 */
	public static class ProfileFormatException extends Exception {

		private static final long serialVersionUID = 6908355768995089420L;

		/**
		 * Constructor with message
		 * 
		 * @param message
		 */
		public ProfileFormatException(String message) {
			super(message);
		}

		/**
		 * Constructor with cause
		 * 
		 * @param cause
		 */
		public ProfileFormatException(String message, Throwable cause) {
			super(message, cause);
		}

	}

}
