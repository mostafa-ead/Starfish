package edu.duke.starfish.profile.profiler.loaders.tasks;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

/**
 * This class represents the profile for a single reduce attempt. It contains
 * all the logic for calculating the task's statistics and costs given lists of
 * profile records.
 * 
 * @author hero
 */
public class MRReduceProfileLoader extends MRTaskProfileLoader {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Lists with the records from the profile file
	private List<ProfileRecord> shuffleRecords;
	private List<ProfileRecord> reduceRecords;
	private List<ProfileRecord> sortRecords;
	private List<ProfileRecord> mergeRecords;

	// CONSTANTS FOR THE SHUFFLE PHASE
	private static final int NUM_SHUFFLE_PHASES = 4;
	private static final int POS_SHUFFLE_UNCOMPR_BYTE_COUNT = 0;
	private static final int POS_SHUFFLE_COMPR_BYTE_COUNT = 1;
	private static final int POS_SHUFFLE_COPY_MAP_OUTPUT = 2;
	private static final int POS_SHUFFLE_UNCOMPRESS = 3;

	// CONSTANTS FOR THE MERGE-IN-SHUFFLE PHASE
	private static final int NUM_MERGE_PHASES = 7;
	private static final int POS_MERGE_MERGE = 0;
	private static final int POS_MERGE_READ_WRITE = 1;
	private static final int POS_MERGE_READ_WRITE_COUNT = 2;
	private static final int POS_MERGE_COMBINE = 3;
	private static final int POS_MERGE_WRITE = 4;
	private static final int POS_MERGE_UNCOMPRESS = 5;
	private static final int POS_MERGE_COMPRESS = 6;

	// CONSTANTS FOR THE SORT PHASE
	private static final int NUM_SORT_PHASES = 5;
	private static final int POS_SORT_MERGE = 0;
	private static final int POS_SORT_READ_WRITE = 1;
	private static final int POS_SORT_READ_WRITE_COUNT = 2;
	private static final int POS_SORT_UNCOMPRESS = 3;
	private static final int POS_SORT_COMPRESS = 4;

	// CONSTANTS FOR THE REDUCE PHASE
	private static final int NUM_REDUCE_PHASES = 16;
	private static final int POS_REDUCE_STARTUP_MEM = 0;
	private static final int POS_REDUCE_SETUP = 1;
	private static final int POS_REDUCE_SETUP_MEM = 2;
	private static final int POS_REDUCE_CLEANUP = 3;
	private static final int POS_REDUCE_CLEANUP_MEM = 4;
	private static final int POS_REDUCE_TOTAL_RUN = 5;
	private static final int POS_REDUCE_READ = 6;
	private static final int POS_REDUCE_UNCOMPRESS = 7;
	private static final int POS_REDUCE_REDUCE = 8;
	private static final int POS_REDUCE_WRITE = 9;
	private static final int POS_REDUCE_COMPRESS = 10;
	private static final int POS_REDUCE_KEY_BYTE_COUNT = 11;
	private static final int POS_REDUCE_VALUE_BYTE_COUNT = 12;
	private static final int POS_REDUCE_MEM = 13;
	private static final int POS_REDUCE_FINAL_WRITE = 14;
	private static final int POS_REDUCE_FINAL_COMPRESS = 15;

	/**
	 * Constructor
	 * 
	 * @param profile
	 *            the task profile
	 * @param conf
	 *            the hadoop configuration
	 * @param profileFile
	 *            the path to the profile file
	 */
	public MRReduceProfileLoader(MRReduceProfile profile, Configuration conf,
			String profileFile) {
		super(profile, conf, profileFile);

		// Initialize all records to be empty
		shuffleRecords = EMPTY_RECORDS;
		reduceRecords = EMPTY_RECORDS;
		sortRecords = EMPTY_RECORDS;
		mergeRecords = EMPTY_RECORDS;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	@Override
	protected boolean loadExecutionProfile() throws ProfileFormatException {
		// Get and validate the profile records
		if (!getAndValidateProfileRecords())
			return false;

		// Calculate all the profile information
		calculateStatsAndCosts();
		calculateTimings();

		return true;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Calculates the statistics and the cost factors of the profile
	 * 
	 * @throws ProfileFormatException
	 */
	private void calculateStatsAndCosts() throws ProfileFormatException {

		// Get some useful counters
		long hdfsBytesWritten = profile.getCounter(
				MRCounter.HDFS_BYTES_WRITTEN, 0l);
		long reduceInputGroups = profile.getCounter(
				MRCounter.REDUCE_INPUT_GROUPS, 0l);
		long reduceInputPairs = profile.getCounter(
				MRCounter.REDUCE_INPUT_RECORDS, 0l);
		long reduceOutputPairs = profile.getCounter(
				MRCounter.REDUCE_OUTPUT_RECORDS, 0l);

		// Calculate the number of shuffle bytes
		long shuffleBytes = aggregateRecordValues(shuffleRecords,
				NUM_SHUFFLE_PHASES, POS_SHUFFLE_COMPR_BYTE_COUNT);
		profile.addCounter(MRCounter.REDUCE_SHUFFLE_BYTES, shuffleBytes);

		// Calculate the number of reduce input bytes
		long reduceInputBytes = aggregateRecordValues(shuffleRecords,
				NUM_SHUFFLE_PHASES, POS_SHUFFLE_UNCOMPR_BYTE_COUNT);
		profile.addCounter(MRCounter.REDUCE_INPUT_BYTES, reduceInputBytes);

		// Calculate the number of reduce output bytes
		long reduceOutputBytes = 0;
		String outputFormat = conf.get(MR_OUTPUT_FORMAT_CLASS, MR_TOF);

		if (outputFormat.equals(MR_TOF) || outputFormat.equals(MR_SFOF)
				|| outputFormat.equals(MR_TSOF)
				|| outputFormat.equals(MR_SFTOF)) {
			// Equals keys + values + separator + newline
			reduceOutputBytes = reduceRecords.get(POS_REDUCE_KEY_BYTE_COUNT)
					.getValue()
					+ reduceRecords.get(POS_REDUCE_VALUE_BYTE_COUNT).getValue()
					+ 2 * reduceOutputPairs;
		} else if (outputFormat.equals(MR_TBOF)) {
			// Equals keys + values
			reduceOutputBytes = reduceRecords.get(POS_REDUCE_KEY_BYTE_COUNT)
					.getValue()
					+ reduceRecords.get(POS_REDUCE_VALUE_BYTE_COUNT).getValue();

			// Might not need this...
			profile.addCounter(MRCounter.HDFS_BYTES_WRITTEN, reduceOutputBytes);
		} else {
			// Equals HDFS output (without compression)
			reduceOutputBytes = (reduceRecords.get(POS_REDUCE_COMPRESS)
					.getValue() == 0) ? hdfsBytesWritten
					: (long) (hdfsBytesWritten / DEFAULT_COMPR_RATIO);
		}
		profile.addCounter(MRCounter.REDUCE_OUTPUT_BYTES, reduceOutputBytes);

		// Calculate and set the network cost
		profile.addCostFactor(MRCostFactors.NETWORK_COST,
				averageProfileValueDiffRatios(shuffleRecords,
						NUM_SHUFFLE_PHASES, POS_SHUFFLE_COPY_MAP_OUTPUT,
						POS_SHUFFLE_UNCOMPRESS, POS_SHUFFLE_COMPR_BYTE_COUNT));

		// Calculate and set the intermediate compression ratio and cost
		double comprRatio = 1;
		if (conf.getBoolean(MR_COMPRESS_MAP_OUT, false) == true) {
			comprRatio = averageRecordValueRatios(shuffleRecords,
					NUM_SHUFFLE_PHASES, POS_SHUFFLE_COMPR_BYTE_COUNT,
					POS_SHUFFLE_UNCOMPR_BYTE_COUNT);
			profile
					.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO,
							comprRatio);

			// Uncompress cost = time to uncompress / compressed size
			profile.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
					averageRecordValueRatios(shuffleRecords,
							NUM_SHUFFLE_PHASES, POS_SHUFFLE_UNCOMPRESS,
							POS_SHUFFLE_COMPR_BYTE_COUNT));

			// Compress cost = time to compress / uncompressed size
			long spilledPairs = profile.getCounter(MRCounter.SPILLED_RECORDS,
					0l);
			if (spilledPairs != 0 && reduceInputPairs != 0) {
				double readBytes = spilledPairs * reduceInputBytes
						/ reduceInputPairs;
				long compressTime = sortRecords.get(POS_SORT_COMPRESS)
						.getValue();
				if (mergeRecords != EMPTY_RECORDS)
					compressTime += aggregateRecordValues(mergeRecords,
							NUM_MERGE_PHASES, POS_MERGE_COMPRESS);

				if (readBytes != 0 && compressTime != 0) {
					profile.addCostFactor(
							MRCostFactors.INTERM_COMPRESS_CPU_COST,
							compressTime / readBytes);
				}
			}
		}

		// Calculate and set the reduce statistics
		if (reduceInputGroups != 0) {
			profile.addStatistic(MRStatistics.REDUCE_PAIRS_PER_GROUP,
					reduceInputPairs / (double) reduceInputGroups);
		}
		if (reduceInputBytes != 0) {
			profile.addStatistic(MRStatistics.REDUCE_SIZE_SEL,
					reduceOutputBytes / (double) reduceInputBytes);
		}
		if (reduceInputPairs != 0) {
			profile.addStatistic(MRStatistics.REDUCE_PAIRS_SEL,
					reduceOutputPairs / (double) reduceInputPairs);
		}

		// Calculate and set the reduce CPU cost
		if (reduceInputPairs != 0) {
			// Cost = pure reduce time / number of input records
			profile.addCostFactor(MRCostFactors.REDUCE_CPU_COST, (reduceRecords
					.get(POS_REDUCE_REDUCE).getValue() - reduceRecords.get(
					POS_REDUCE_WRITE).getValue())
					/ (double) reduceInputPairs);
		}

		// Calculate and set the output compression ratio and cost
		if (conf.getBoolean("mapred.output.compress", false) == true) {
			if (reduceOutputBytes != 0) {
				profile.addStatistic(MRStatistics.OUT_COMPRESS_RATIO,
						hdfsBytesWritten / (double) reduceOutputBytes);

				// Cost = time to compress / uncompressed size
				profile
						.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
								(reduceRecords.get(POS_REDUCE_COMPRESS)
										.getValue() + reduceRecords.get(
										POS_REDUCE_FINAL_COMPRESS).getValue())
										/ (double) reduceOutputBytes);
			}
		}

		// Calculate and set the local read I/O cost
		if (reduceInputBytes != 0) {
			profile.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST,
					reduceRecords.get(POS_REDUCE_READ).getValue()
							/ (comprRatio * reduceInputBytes));
		}

		// Calculate and set the local write I/O cost
		long fileBytesWritten = profile.getCounter(
				MRCounter.FILE_BYTES_WRITTEN, 0l);
		if (fileBytesWritten != 0l
				&& sortRecords.get(POS_SORT_READ_WRITE_COUNT).getValue() <= 1l) {

			long writeTime = sortRecords.get(POS_SORT_READ_WRITE).getValue()
					- sortRecords.get(POS_SORT_COMPRESS).getValue();
			if (mergeRecords != EMPTY_RECORDS)
				writeTime += aggregateRecordValues(mergeRecords,
						NUM_MERGE_PHASES, POS_MERGE_READ_WRITE)
						- aggregateRecordValues(mergeRecords, NUM_MERGE_PHASES,
								POS_MERGE_COMPRESS);

			profile.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, writeTime
					/ (double) fileBytesWritten);
		}

		double writeTime = (reduceRecords.get(POS_REDUCE_WRITE).getValue()
				+ reduceRecords.get(POS_REDUCE_FINAL_WRITE).getValue()
				- reduceRecords.get(POS_REDUCE_COMPRESS).getValue() - reduceRecords
				.get(POS_REDUCE_FINAL_COMPRESS).getValue());

		// Calculate and set the HDFS write I/O cost
		if (hdfsBytesWritten != 0) {
			// Cost = pure write time / bytes written to HDFS
			profile.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, writeTime
					/ (double) hdfsBytesWritten);
		} else if (reduceOutputBytes != 0) {
			// Cost = pure write time / bytes written by reducer
			profile.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, writeTime
					/ (double) reduceOutputBytes);
		}

		// Calculate and set the combiner statistics and costs
		if (conf.get(MR_COMBINE_CLASS) != null) {

			long combineInputPairs = profile.getCounter(
					MRCounter.COMBINE_INPUT_RECORDS, 0l);
			long combineOutputPairs = profile.getCounter(
					MRCounter.COMBINE_OUTPUT_RECORDS, 0l);

			if (combineInputPairs != 0) {
				profile.addStatistic(MRStatistics.COMBINE_PAIRS_SEL,
						combineOutputPairs / (double) combineInputPairs);
			}
		}

		// Calculate the merge CPU cost
		long spilledPairs = profile.getCounter(MRCounter.SPILLED_RECORDS,
				reduceInputPairs);
		if (spilledPairs > 0) {
			long mergeTime = sortRecords.get(POS_SORT_MERGE).getValue()
					- sortRecords.get(POS_SORT_READ_WRITE).getValue();
			if (mergeRecords != EMPTY_RECORDS)
				mergeTime += aggregateRecordValues(mergeRecords,
						NUM_MERGE_PHASES, POS_MERGE_MERGE)
						- aggregateRecordValues(mergeRecords, NUM_MERGE_PHASES,
								POS_MERGE_READ_WRITE);

			profile.addCostFactor(MRCostFactors.MERGE_CPU_COST, mergeTime
					/ (double) spilledPairs);
		}

		// Set the setup and cleanup costs
		profile.addCostFactor(MRCostFactors.SETUP_CPU_COST,
				(double) reduceRecords.get(POS_REDUCE_SETUP).getValue());
		profile.addCostFactor(MRCostFactors.CLEANUP_CPU_COST,
				(double) reduceRecords.get(POS_REDUCE_CLEANUP).getValue());

		// Calculate and set the memory statistics
		long startup_mem = reduceRecords.get(POS_REDUCE_STARTUP_MEM).getValue();
		long setup_mem = reduceRecords.get(POS_REDUCE_SETUP_MEM).getValue()
				- reduceRecords.get(POS_REDUCE_STARTUP_MEM).getValue();
		setup_mem = (setup_mem < 0l) ? 0l : setup_mem;
		long reduce_mem = reduceRecords.get(POS_REDUCE_MEM).getValue()
				- reduceRecords.get(POS_REDUCE_SETUP_MEM).getValue();
		reduce_mem = (reduce_mem < 0l) ? 0l : reduce_mem;
		long cleanup_mem = reduceRecords.get(POS_REDUCE_CLEANUP_MEM).getValue()
				- reduceRecords.get(POS_REDUCE_MEM).getValue();
		cleanup_mem = (cleanup_mem < 0l) ? 0l : cleanup_mem;

		profile.addStatistic(MRStatistics.STARTUP_MEM, (double) startup_mem);
		profile.addStatistic(MRStatistics.SETUP_MEM, (double) setup_mem);
		profile.addStatistic(MRStatistics.CLEANUP_MEM, (double) cleanup_mem);
		if (reduceInputPairs != 0)
			profile.addStatistic(MRStatistics.REDUCE_MEM_PER_RECORD, reduce_mem
					/ (double) reduceInputPairs);
		else
			profile.addStatistic(MRStatistics.REDUCE_MEM_PER_RECORD,
					(double) reduce_mem);

	}

	/**
	 * Calculates the phase timings
	 */
	private void calculateTimings() {

		// Calculate the shuffle timings
		double timeShuffle = aggregateRecordValues(shuffleRecords,
				NUM_SHUFFLE_PHASES, POS_SHUFFLE_COPY_MAP_OUTPUT);
		if (mergeRecords != EMPTY_RECORDS)
			timeShuffle += aggregateRecordValues(mergeRecords,
					NUM_MERGE_PHASES, POS_MERGE_MERGE);

		profile.addTiming(MRTaskPhase.SHUFFLE, timeShuffle / NS_PER_MS);

		// Calculate the sort timings
		double timeSort = sortRecords.get(POS_SORT_MERGE).getValue();
		profile.addTiming(MRTaskPhase.SORT, timeSort / NS_PER_MS);

		// Calculate the reduce timings
		profile.addTiming(MRTaskPhase.SETUP, reduceRecords
				.get(POS_REDUCE_SETUP).getValue()
				/ NS_PER_MS);

		profile
				.addTiming(MRTaskPhase.REDUCE,
						(reduceRecords.get(POS_REDUCE_READ).getValue()
								+ reduceRecords.get(POS_REDUCE_REDUCE)
										.getValue() - reduceRecords.get(
								POS_REDUCE_WRITE).getValue())
								/ NS_PER_MS);

		profile.addTiming(MRTaskPhase.WRITE, (reduceRecords.get(
				POS_REDUCE_WRITE).getValue() + reduceRecords.get(
				POS_REDUCE_FINAL_WRITE).getValue())
				/ NS_PER_MS);

		profile.addTiming(MRTaskPhase.CLEANUP, reduceRecords.get(
				POS_REDUCE_CLEANUP).getValue()
				/ NS_PER_MS);
	}

	/**
	 * Populates and validates the list with the profile records
	 */
	private boolean getAndValidateProfileRecords()
			throws ProfileFormatException {
		shuffleRecords = getProfileRecords(ProfileToken.SHUFFLE);
		if (!validateShuffleRecords(shuffleRecords))
			return false;

		reduceRecords = getProfileRecords(ProfileToken.REDUCE);
		if (!validateReduceRecords(reduceRecords))
			return false;

		sortRecords = getProfileRecords(ProfileToken.SORT);
		if (!validateSortRecords(sortRecords))
			return false;

		mergeRecords = getProfileRecords(ProfileToken.MERGE);
		if (mergeRecords != null) {
			if (!validateMergeRecords(mergeRecords))
				return false;
		} else
			mergeRecords = EMPTY_RECORDS;

		return true;
	}

	/**
	 * Validate the number and order of records in a shuffle phase. If a problem
	 * is detected, a ProfileFormatException is thrown.
	 * 
	 * @param records
	 *            the shuffle records to validate
	 * @return true if the profile records are accurate
	 * @throws ProfileFormatException
	 */
	private boolean validateShuffleRecords(List<ProfileRecord> records)
			throws ProfileFormatException {
		if (records == null)
			return false;

		if (records.size() % NUM_SHUFFLE_PHASES != 0) {
			throw new ProfileFormatException("Expected groups of "
					+ NUM_SHUFFLE_PHASES
					+ " records for the SHUFFLE phase for "
					+ this.profile.getTaskId());
		}

		int count = 0;
		for (int i = 0; i < records.size(); i += NUM_SHUFFLE_PHASES) {
			count += records.get(i + POS_SHUFFLE_UNCOMPR_BYTE_COUNT)
					.getProcess().equals(UNCOMPRESS_BYTE_COUNT) ? 0 : 1;
			count += records.get(i + POS_SHUFFLE_COMPR_BYTE_COUNT).getProcess()
					.equals(COMPRESS_BYTE_COUNT) ? 0 : 1;
			count += records.get(i + POS_SHUFFLE_COPY_MAP_OUTPUT).getProcess()
					.equals(COPY_MAP_DATA) ? 0 : 1;
			count += records.get(i + POS_SHUFFLE_UNCOMPRESS).getProcess()
					.equals(UNCOMPRESS) ? 0 : 1;
		}

		if (count != 0)
			throw new ProfileFormatException(
					"Incorrect sequence of records in SHUFFLE phase for "
							+ this.profile.getTaskId());

		// Remove the empty map outputs
		for (int i = 0; i < records.size(); i += NUM_SHUFFLE_PHASES) {
			if (records.get(i + POS_SHUFFLE_UNCOMPR_BYTE_COUNT).getValue() == 2) {
				// Remove all processes of this shuffle
				for (int j = 0; j < NUM_SHUFFLE_PHASES; ++j) {
					records.remove(i);
				}
				i -= NUM_SHUFFLE_PHASES;
			}
		}

		return true;
	}

	/**
	 * Validate the number and order of records in a merge-in-shuffle phase. If
	 * a problem is detected, a ProfileFormatException is thrown.
	 * 
	 * @param records
	 *            the merge records to validate
	 * @return true if the profile records are accurate
	 * @throws ProfileFormatException
	 */
	private boolean validateMergeRecords(List<ProfileRecord> records)
			throws ProfileFormatException {
		if (records == null)
			return false;

		if (records == null || records.size() % NUM_MERGE_PHASES != 0) {
			throw new ProfileFormatException("Expected groups of "
					+ NUM_MERGE_PHASES + " records for the MERGE phase for "
					+ this.profile.getTaskId());
		}

		int count = 0;
		for (int i = 0; i < records.size(); i += NUM_MERGE_PHASES) {
			count += (records.get(POS_MERGE_MERGE).getProcess().equals(
					MERGE_IN_MEMORY) || records.get(POS_MERGE_MERGE)
					.getProcess().equals(MERGE_TO_DISK)) ? 0 : 1;
			count += records.get(POS_MERGE_READ_WRITE).getProcess().equals(
					READ_WRITE) ? 0 : 1;
			count += records.get(POS_MERGE_READ_WRITE_COUNT).getProcess()
					.equals(READ_WRITE_COUNT) ? 0 : 1;
			count += records.get(POS_MERGE_COMBINE).getProcess()
					.equals(COMBINE) ? 0 : 1;
			count += records.get(POS_MERGE_WRITE).getProcess().equals(WRITE) ? 0
					: 1;
			count += records.get(POS_MERGE_UNCOMPRESS).getProcess().equals(
					UNCOMPRESS) ? 0 : 1;
			count += records.get(POS_MERGE_COMPRESS).getProcess().equals(
					COMPRESS) ? 0 : 1;
		}

		if (count != 0)
			throw new ProfileFormatException(
					"Incorrect sequence of records in MERGE phase for "
							+ this.profile.getTaskId());

		return true;
	}

	/**
	 * Validate the number and order of records in a sort phase. If a problem is
	 * detected, a ProfileFormatException is thrown.
	 * 
	 * @param records
	 *            the sort records to validate
	 * @return true if the profile records are accurate
	 * @throws ProfileFormatException
	 */
	private boolean validateSortRecords(List<ProfileRecord> records)
			throws ProfileFormatException {
		if (records == null)
			return false;

		if (records == null || records.size() != NUM_SORT_PHASES) {
			throw new ProfileFormatException("Expected " + NUM_SORT_PHASES
					+ " records for the SORT phase for "
					+ this.profile.getTaskId());
		}

		int count = 0;
		count += records.get(POS_SORT_MERGE).getProcess()
				.equals(MERGE_MAP_DATA) ? 0 : 1;
		count += records.get(POS_SORT_READ_WRITE).getProcess().equals(
				READ_WRITE) ? 0 : 1;
		count += records.get(POS_SORT_READ_WRITE_COUNT).getProcess().equals(
				READ_WRITE_COUNT) ? 0 : 1;
		count += records.get(POS_SORT_UNCOMPRESS).getProcess().equals(
				UNCOMPRESS) ? 0 : 1;
		count += records.get(POS_SORT_COMPRESS).getProcess().equals(COMPRESS) ? 0
				: 1;

		if (count != 0)
			throw new ProfileFormatException(
					"Incorrect sequence of records in SORT phase for "
							+ this.profile.getTaskId());

		return true;
	}

	/**
	 * Validate the number and order of records in a reduce phase. If a problem
	 * is detected, a ProfileFormatException is thrown.
	 * 
	 * @param records
	 *            the reduce records to validate
	 * @return true if the profile records are accurate
	 * @throws ProfileFormatException
	 */
	private boolean validateReduceRecords(List<ProfileRecord> records)
			throws ProfileFormatException {
		if (records == null)
			return false;

		if (records == null || records.size() != NUM_REDUCE_PHASES) {
			throw new ProfileFormatException("Expected " + NUM_REDUCE_PHASES
					+ " records for the REDUCE phase for "
					+ this.profile.getTaskId());
		}

		int count = 0;
		count += records.get(POS_REDUCE_STARTUP_MEM).getProcess().equals(
				STARTUP_MEM) ? 0 : 1;
		count += records.get(POS_REDUCE_SETUP).getProcess().equals(SETUP) ? 0
				: 1;
		count += records.get(POS_REDUCE_SETUP_MEM).getProcess().equals(
				SETUP_MEM) ? 0 : 1;
		count += records.get(POS_REDUCE_CLEANUP).getProcess().equals(CLEANUP) ? 0
				: 1;
		count += records.get(POS_REDUCE_CLEANUP_MEM).getProcess().equals(
				CLEANUP_MEM) ? 0 : 1;
		count += records.get(POS_REDUCE_TOTAL_RUN).getProcess().equals(
				TOTAL_RUN) ? 0 : 1;
		count += records.get(POS_REDUCE_READ).getProcess().equals(READ) ? 0 : 1;
		count += records.get(POS_REDUCE_UNCOMPRESS).getProcess().equals(
				UNCOMPRESS) ? 0 : 1;
		count += records.get(POS_REDUCE_REDUCE).getProcess().equals(REDUCE) ? 0
				: 1;
		count += records.get(POS_REDUCE_WRITE).getProcess().equals(WRITE) ? 0
				: 1;
		count += records.get(POS_REDUCE_COMPRESS).getProcess().equals(COMPRESS) ? 0
				: 1;
		count += records.get(POS_REDUCE_KEY_BYTE_COUNT).getProcess().equals(
				KEY_BYTE_COUNT) ? 0 : 1;
		count += records.get(POS_REDUCE_VALUE_BYTE_COUNT).getProcess().equals(
				VALUE_BYTE_COUNT) ? 0 : 1;
		count += records.get(POS_REDUCE_MEM).getProcess().equals(REDUCE_MEM) ? 0
				: 1;
		count += records.get(POS_REDUCE_FINAL_WRITE).getProcess().equals(WRITE) ? 0
				: 1;
		count += records.get(POS_REDUCE_FINAL_COMPRESS).getProcess().equals(
				COMPRESS) ? 0 : 1;

		if (count != 0)
			throw new ProfileFormatException(
					"Incorrect sequence of records in REDUCE phase for "
							+ this.profile.getTaskId());

		return true;
	}

}
