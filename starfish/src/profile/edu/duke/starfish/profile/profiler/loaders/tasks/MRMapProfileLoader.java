package edu.duke.starfish.profile.profiler.loaders.tasks;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

/**
 * This class represents the profile for a single map attempt. It contains all
 * the logic for calculating the task's statistics and costs given lists of
 * profile records.
 * 
 * @author hero
 */
public class MRMapProfileLoader extends MRTaskProfileLoader {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// List with the profile records
	private List<ProfileRecord> mapRecords;
	private List<ProfileRecord> spillRecords;
	private List<ProfileRecord> mergeRecords;

	// CONSTANTS FOR THE MAP PHASE
	private static final int NUM_MAP_PHASES = 21;
	private static final int POS_MAP_INPUT = 0;
	private static final int POS_MAP_STARTUP_MEM = 1;
	private static final int POS_MAP_SETUP = 2;
	private static final int POS_MAP_SETUP_MEM = 3;
	private static final int POS_MAP_CLEANUP = 4;
	private static final int POS_MAP_CLEANUP_MEM = 5;
	private static final int POS_MAP_TOTAL_RUN = 6;
	private static final int POS_MAP_READ = 7;
	private static final int POS_MAP_UNCOMPRESS = 8;
	private static final int POS_MAP_INPUT_K_BYTE_COUNT = 9;
	private static final int POS_MAP_INPUT_V_BYTE_COUNT = 10;
	private static final int POS_MAP_MAP = 11;
	private static final int POS_MAP_WRITE = 12;
	private static final int POS_MAP_COMPRESS = 13;
	private static final int POS_MAP_PARTITION_OUTPUT = 14;
	private static final int POS_MAP_SERIALIZE_OUTPUT = 15;
	private static final int POS_MAP_MEM = 16;
	private static final int POS_MAP_DIR_WRITE = 17;
	private static final int POS_MAP_DIR_COMPRESS = 18;
	private static final int POS_MAP_OUTPUT_K_BYTE_COUNT = 19;
	private static final int POS_MAP_OUTPUT_V_BYTE_COUNT = 20;

	// CONSTANTS FOR THE SPILL PHASE
	private static final int NUM_SPILL_PHASES = 8;
	private static final int POS_SPILL_SORT_AND_SPILL = 0;
	private static final int POS_SPILL_QUICK_SORT = 1;
	private static final int POS_SPILL_SORT_COUNT = 2;
	private static final int POS_SPILL_COMBINE = 3;
	private static final int POS_SPILL_WRITE = 4;
	private static final int POS_SPILL_COMPRESS = 5;
	private static final int POS_SPILL_UNCOMPRESS_BYTE_COUNT = 6;
	private static final int POS_SPILL_COMPRESS_BYTE_COUNT = 7;

	// CONSTANTS FOR THE MERGE PHASE
	private static final int NUM_MERGE_PHASES = 5;
	private static final int POS_MERGE_TOTAL_MERGE = 0;
	private static final int POS_MERGE_READ_WRITE = 1;
	private static final int POS_MERGE_READ_WRITE_COUNT = 2;
	private static final int POS_MERGE_UNCOMPRESS = 3;
	private static final int POS_MERGE_COMPRESS = 4;

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
	public MRMapProfileLoader(MRMapProfile profile, Configuration conf,
			String profileFile) {
		super(profile, conf, profileFile);

		// Initialize all lists to be empty
		mapRecords = EMPTY_RECORDS;
		spillRecords = EMPTY_RECORDS;
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
		long hdfsBytesRead = profile.getCounter(MRCounter.HDFS_BYTES_READ, 0l);
		long hdfsBytesWritten = profile.getCounter(
				MRCounter.HDFS_BYTES_WRITTEN, 0l);
		long mapInputBytes = profile.getCounter(MRCounter.MAP_INPUT_BYTES, 0l);
		long mapOutputBytes = profile
				.getCounter(MRCounter.MAP_OUTPUT_BYTES, 0l);
		long mapInputPairs = profile
				.getCounter(MRCounter.MAP_INPUT_RECORDS, 1l);
		long mapOutputPairs = profile.getCounter(MRCounter.MAP_OUTPUT_RECORDS,
				0l);

		// Calculate the number of map input bytes
		if (mapInputBytes == 0) {
			String inputFormat = conf.get(MR_INPUT_FORMAT_CLASS, MR_TIF);

			if (inputFormat.equals(MR_TIF) || inputFormat.equals(MR_SFTIF)
					|| inputFormat.equals(MR_WFIF)) {
				// Equals value size + newlines
				mapInputBytes = mapRecords.get(POS_MAP_INPUT_V_BYTE_COUNT)
						.getValue()
						+ mapInputPairs;
			} else if (inputFormat.equals(MR_SFIF)
					|| inputFormat.equals(MR_TSIF)
					|| inputFormat.equals(MR_KVTIF)
					|| inputFormat.equals(MR_KVTPIF)
					|| inputFormat.equals(MR_WFTPIF)) {
				// Equals key size + value size + separator + newline
				mapInputBytes = mapRecords.get(POS_MAP_INPUT_K_BYTE_COUNT)
						.getValue()
						+ mapRecords.get(POS_MAP_INPUT_V_BYTE_COUNT).getValue()
						+ 2 * mapInputPairs;
			} else if (inputFormat.equals(MR_TBIF)) {
				// Equals key size + value size
				mapInputBytes = mapRecords.get(POS_MAP_INPUT_K_BYTE_COUNT)
						.getValue()
						+ mapRecords.get(POS_MAP_INPUT_V_BYTE_COUNT).getValue();
				// Might not need this...
				profile.addCounter(MRCounter.HDFS_BYTES_READ, mapInputBytes);

			} else {
				// Equals HDFS input (without compression)
				mapInputBytes = (mapRecords.get(POS_MAP_UNCOMPRESS).getValue() == 0) ? hdfsBytesRead
						: (long) (hdfsBytesRead / DEFAULT_COMPR_RATIO);
			}

			profile.addCounter(MRCounter.MAP_INPUT_BYTES, mapInputBytes);
		}

		// Calculate the number of map output bytes
		if (mapOutputBytes == 0) {
			String outputFormat = conf.get(MR_OUTPUT_FORMAT_CLASS, MR_TOF);

			if (outputFormat.equals(MR_TOF) || outputFormat.equals(MR_SFOF)
					|| outputFormat.equals(MR_TSOF)) {
				// Equals keys + values + separator + newline
				mapOutputBytes = mapRecords.get(POS_MAP_OUTPUT_K_BYTE_COUNT)
						.getValue()
						+ mapRecords.get(POS_MAP_OUTPUT_V_BYTE_COUNT)
								.getValue() + 2 * mapOutputPairs;
			} else if (outputFormat.equals(MR_TBOF)) {
				// Equals keys + values ... I don't think we need separator +
				// newline because the keys and values go straight to HBase
				mapOutputBytes = mapRecords.get(POS_MAP_OUTPUT_K_BYTE_COUNT)
						.getValue()
						+ mapRecords.get(POS_MAP_OUTPUT_V_BYTE_COUNT)
								.getValue();

				// Might not need this...
				profile
						.addCounter(MRCounter.HDFS_BYTES_WRITTEN,
								mapOutputBytes);

			} else {
				// Equals HDFS output (without compression)
				mapOutputBytes = (mapRecords.get(POS_MAP_COMPRESS).getValue() == 0) ? hdfsBytesWritten
						: (long) (hdfsBytesWritten / DEFAULT_COMPR_RATIO);
			}

			profile.addCounter(MRCounter.MAP_OUTPUT_BYTES, mapOutputBytes);
		}

		// Calculate and set the map statistics
		if (mapInputBytes != 0) {
			profile.addStatistic(MRStatistics.MAP_SIZE_SEL, mapOutputBytes
					/ (double) mapInputBytes);
		}
		if (mapInputPairs != 0) {
			profile.addStatistic(MRStatistics.MAP_PAIRS_SEL, mapOutputPairs
					/ (double) mapInputPairs);
			profile.addStatistic(MRStatistics.INPUT_PAIR_WIDTH, mapInputBytes
					/ (double) mapInputPairs);
		}

		// Calculate and set the map CPU cost
		if (mapInputPairs != 0) {
			// Cost = pure map time / number of input records
			profile.addCostFactor(MRCostFactors.MAP_CPU_COST, (mapRecords.get(
					POS_MAP_MAP).getValue() - mapRecords.get(POS_MAP_WRITE)
					.getValue())
					/ (double) mapInputPairs);
		}

		// Calculate and cost HDFS read I/O Costs
		double readTime = mapRecords.get(POS_MAP_READ).getValue()
				- mapRecords.get(POS_MAP_UNCOMPRESS).getValue();

		// Calculate and cost HDFS read I/O Costs
		if (hdfsBytesRead != 0) {
			// Cost = pure read time / bytes read from HDFS
			profile.addCostFactor(MRCostFactors.READ_HDFS_IO_COST, readTime
					/ (double) hdfsBytesRead);
		} else if (mapInputBytes != 0) {
			// Cost = pure read time / bytes read into Map
			profile.addCostFactor(MRCostFactors.READ_HDFS_IO_COST, readTime
					/ (double) mapInputBytes);
		}

		// Calculate and set the input compression ratio and cost
		// if (mapRecords.get(POS_MAP_UNCOMPRESS).getValue() != 0) {
		if (mapInputBytes > hdfsBytesRead) {
			if (mapInputBytes != 0) {
				profile.addStatistic(MRStatistics.INPUT_COMPRESS_RATIO,
						hdfsBytesRead / (double) mapInputBytes);
			}

			if (hdfsBytesRead != 0) {
				// Cost = time to uncompress / compressed size
				profile.addCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST,
						mapRecords.get(POS_MAP_UNCOMPRESS).getValue()
								/ (double) hdfsBytesRead);
			}
		}

		int numReducers = conf.getInt(MR_RED_TASKS, 1);
		if (numReducers == 0) {
			// Map-only task

			// Calculate and set the output compression ratio and cost
			if (conf.getBoolean(MR_COMPRESS_OUT, false) == true) {
				if (mapOutputBytes != 0) {
					profile.addStatistic(MRStatistics.OUT_COMPRESS_RATIO,
							hdfsBytesWritten / (double) mapOutputBytes);

					// Cost = time to compress / uncompressed size
					profile
							.addCostFactor(
									MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
									(mapRecords.get(POS_MAP_COMPRESS)
											.getValue() + mapRecords.get(
											POS_MAP_DIR_COMPRESS).getValue())
											/ (double) mapOutputBytes);
				}
			}

			double writeTime = (mapRecords.get(POS_MAP_WRITE).getValue()
					+ mapRecords.get(POS_MAP_DIR_WRITE).getValue()
					- mapRecords.get(POS_MAP_COMPRESS).getValue() - mapRecords
					.get(POS_MAP_DIR_COMPRESS).getValue());

			// Calculate and set the HDFS write I/O cost
			if (hdfsBytesWritten != 0) {
				// Cost = pure write time / bytes written to HDFS
				profile.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST,
						writeTime / (double) hdfsBytesWritten);
			} else if (mapOutputBytes != 0) {
				// Cost = pure write time / bytes written by map
				profile.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST,
						writeTime / (double) mapOutputBytes);
			}

		} else {

			// Calculate and set the local I/O cost
			profile.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST,
					averageProfileValueDiffRatios(spillRecords,
							NUM_SPILL_PHASES, POS_SPILL_WRITE,
							POS_SPILL_COMPRESS, POS_SPILL_COMPRESS_BYTE_COUNT));

			// Calculate and set the combiner statistics and costs
			if (conf.get(MR_COMBINE_CLASS) != null) {
				// Get the counters
				long combineInputPairs = profile.getCounter(
						MRCounter.COMBINE_INPUT_RECORDS, 1l);
				long combineOutputPairs = profile.getCounter(
						MRCounter.COMBINE_OUTPUT_RECORDS, 1l);

				// Calculate and set the selectivities
				if (mapOutputBytes != 0) {
					profile.addStatistic(MRStatistics.COMBINE_SIZE_SEL,
							aggregateRecordValues(spillRecords,
									NUM_SPILL_PHASES,
									POS_SPILL_UNCOMPRESS_BYTE_COUNT)
									/ (double) mapOutputBytes);
				}

				if (combineInputPairs != 0) {
					profile.addStatistic(MRStatistics.COMBINE_PAIRS_SEL,
							combineOutputPairs / (double) combineInputPairs);
				}

				// Calculate and set the CPU cost for the combiner
				profile.addCostFactor(MRCostFactors.COMBINE_CPU_COST,
						averageProfileValueDiffRatios(spillRecords,
								NUM_SPILL_PHASES, POS_SPILL_COMBINE,
								POS_SPILL_WRITE, POS_SPILL_SORT_COUNT));
			}

			// Calculate and set the partition CPU cost
			if (mapOutputPairs != 0) {
				// Cost = time to partition / number of map output records
				profile.addCostFactor(MRCostFactors.PARTITION_CPU_COST,
						mapRecords.get(POS_MAP_PARTITION_OUTPUT).getValue()
								/ (double) mapOutputPairs);
			}

			// Calculate and set the serialization CPU cost
			if (mapOutputPairs != 0) {
				// Cost = time to serialize / number of map output records
				profile.addCostFactor(MRCostFactors.SERDE_CPU_COST, mapRecords
						.get(POS_MAP_SERIALIZE_OUTPUT).getValue()
						/ (double) mapOutputPairs);
			}

			// Calculate and set the sort CPU cost (cost per comparison)
			profile.addCostFactor(MRCostFactors.SORT_CPU_COST,
					averageSortCostInSpills(spillRecords, POS_SPILL_QUICK_SORT,
							POS_SPILL_SORT_COUNT, numReducers));

			// Calculate the number of merged pairs
			long combineOutputPairs = profile.getCounter(
					MRCounter.COMBINE_OUTPUT_RECORDS, 0l);
			long spilledPairs = profile.getCounter(MRCounter.SPILLED_RECORDS,
					mapOutputPairs);
			long outputPairs = ((combineOutputPairs == 0) ? mapOutputPairs
					: combineOutputPairs);
			long numMergedPairs = spilledPairs - outputPairs;

			// Calculate the merge CPU cost
			if (numMergedPairs > 0) {
				profile
						.addCostFactor(MRCostFactors.MERGE_CPU_COST,
								(mergeRecords.get(POS_MERGE_TOTAL_MERGE)
										.getValue() - mergeRecords.get(
										POS_MERGE_READ_WRITE).getValue())
										/ (double) numMergedPairs);
			}

			// Calculate and set the intermediate compression ratio and cost
			if (conf.getBoolean(MR_COMPRESS_MAP_OUT, false) == true) {
				profile.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO,
						averageRecordValueRatios(spillRecords,
								NUM_SPILL_PHASES,
								POS_SPILL_COMPRESS_BYTE_COUNT,
								POS_SPILL_UNCOMPRESS_BYTE_COUNT));

				// Compress cost = time to compress / uncompressed size
				profile.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST,
						averageRecordValueRatios(spillRecords,
								NUM_SPILL_PHASES, POS_SPILL_COMPRESS,
								POS_SPILL_UNCOMPRESS_BYTE_COUNT));

				// Uncompress cost = time to uncompress / compressed size
				double readBytes = ((spilledPairs / (double) outputPairs) - 1)
						* aggregateRecordValues(spillRecords, NUM_SPILL_PHASES,
								POS_SPILL_COMPRESS_BYTE_COUNT);
				if (outputPairs != 0
						&& readBytes > 0
						&& mergeRecords.get(POS_MERGE_UNCOMPRESS).getValue() != 0) {
					profile.addCostFactor(
							MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
							mergeRecords.get(POS_MERGE_UNCOMPRESS).getValue()
									/ readBytes);
				}
			}

			// Calculate and set spill-related counters
			profile.addCounter(MRCounter.MAP_NUM_SPILLS, spillRecords.size()
					/ (long) NUM_SPILL_PHASES);
			profile.addCounter(MRCounter.MAP_NUM_SPILL_MERGES, mergeRecords
					.get(POS_MERGE_READ_WRITE_COUNT).getValue()
					/ numReducers);
			profile.addCounter(MRCounter.MAP_RECS_PER_BUFF_SPILL,
					(long) averageRecordValues(spillRecords, NUM_SPILL_PHASES,
							POS_SPILL_SORT_COUNT));
			profile.addCounter(MRCounter.MAP_SPILL_SIZE,
					(long) averageRecordValues(spillRecords, NUM_SPILL_PHASES,
							POS_SPILL_COMPRESS_BYTE_COUNT));
		}

		// Set the setup and cleanup costs
		profile.addCostFactor(MRCostFactors.SETUP_CPU_COST, (double) mapRecords
				.get(POS_MAP_SETUP).getValue());
		profile.addCostFactor(MRCostFactors.CLEANUP_CPU_COST,
				(double) mapRecords.get(POS_MAP_CLEANUP).getValue());

		// Calculate and set the memory statistics
		long startup_mem = mapRecords.get(POS_MAP_STARTUP_MEM).getValue();
		long setup_mem = mapRecords.get(POS_MAP_SETUP_MEM).getValue()
				- mapRecords.get(POS_MAP_STARTUP_MEM).getValue();
		setup_mem = (setup_mem < 0l) ? 0l : setup_mem;
		long map_mem = mapRecords.get(POS_MAP_MEM).getValue()
				- mapRecords.get(POS_MAP_SETUP_MEM).getValue();
		map_mem = (map_mem < 0l) ? 0l : map_mem;
		long cleanup_mem = mapRecords.get(POS_MAP_CLEANUP_MEM).getValue()
				- mapRecords.get(POS_MAP_MEM).getValue();
		cleanup_mem = (cleanup_mem < 0l) ? 0l : cleanup_mem;

		int sortmb = conf.getInt(MR_SORT_MB, 100) << 20;
		startup_mem = (startup_mem > sortmb) ? startup_mem - sortmb : 0l;

		profile.addStatistic(MRStatistics.STARTUP_MEM, (double) startup_mem);
		profile.addStatistic(MRStatistics.SETUP_MEM, (double) setup_mem);
		profile.addStatistic(MRStatistics.CLEANUP_MEM, (double) cleanup_mem);
		if (mapInputPairs != 0)
			profile.addStatistic(MRStatistics.MAP_MEM_PER_RECORD, map_mem
					/ (double) mapInputPairs);
		else
			profile.addStatistic(MRStatistics.MAP_MEM_PER_RECORD,
					(double) map_mem);

		// Set the input file path
		String[] jobInputs = conf.getStrings(MR_INPUT_DIR);
		String mapInput = mapRecords.get(POS_MAP_INPUT).getProcess();
		((MRMapProfile) profile).setInputIndex(getMapInputPosition(jobInputs,
				mapInput));
	}

	/**
	 * Calculates the phase timings
	 */
	private void calculateTimings() {

		// Calculate the timings
		profile.addTiming(MRTaskPhase.SETUP, mapRecords.get(POS_MAP_SETUP)
				.getValue()
				/ NS_PER_MS);

		profile.addTiming(MRTaskPhase.READ, mapRecords.get(POS_MAP_READ)
				.getValue()
				/ NS_PER_MS);

		profile.addTiming(MRTaskPhase.MAP, (mapRecords.get(POS_MAP_MAP)
				.getValue() - mapRecords.get(POS_MAP_WRITE).getValue())
				/ NS_PER_MS);

		profile.addTiming(MRTaskPhase.CLEANUP, mapRecords.get(POS_MAP_CLEANUP)
				.getValue()
				/ NS_PER_MS);

		int numReducers = conf.getInt(MR_RED_TASKS, 1);
		if (numReducers == 0) {
			profile.addTiming(MRTaskPhase.WRITE, (mapRecords.get(POS_MAP_WRITE)
					.getValue() + mapRecords.get(POS_MAP_DIR_WRITE).getValue())
					/ NS_PER_MS);
		} else {
			profile.addTiming(MRTaskPhase.COLLECT, (mapRecords.get(
					POS_MAP_PARTITION_OUTPUT).getValue() + mapRecords.get(
					POS_MAP_SERIALIZE_OUTPUT).getValue())
					/ NS_PER_MS);

			if (spillRecords != EMPTY_RECORDS)
				profile.addTiming(MRTaskPhase.SPILL, aggregateRecordValues(
						spillRecords, NUM_SPILL_PHASES,
						POS_SPILL_SORT_AND_SPILL)
						/ NS_PER_MS);

			if (mergeRecords != EMPTY_RECORDS)
				profile.addTiming(MRTaskPhase.MERGE, mergeRecords.get(
						POS_MERGE_TOTAL_MERGE).getValue()
						/ NS_PER_MS);
		}
	}

	/**
	 * Calculates the average sort cost from all the spills. The cost from a
	 * single sort = time / (N * log_2 (N / R))
	 * 
	 * @param records
	 *            the spill records
	 * @param sortPos
	 *            the position of the sort timing
	 * @param countPos
	 *            the position of the sort count
	 * @param numReducers
	 *            the number of reducers
	 * @return the averages sort cost
	 */
	private double averageSortCostInSpills(List<ProfileRecord> records,
			int sortPos, int countPos, int numReducers) {
		double sumCosts = 0d;
		int numSpills = 0;
		double numRecsPerRed = 1d;

		for (int i = 0; i < records.size(); i += NUM_SPILL_PHASES) {
			numRecsPerRed = records.get(i + countPos).getValue()
					/ (double) numReducers;
			sumCosts += (records.get(i + sortPos).getValue() * Math.log(2))
					/ (records.get(i + countPos).getValue() * Math
							.log((numRecsPerRed < 2) ? 2 : numRecsPerRed));
			++numSpills;
		}

		return sumCosts / numSpills;
	}

	/**
	 * Populates and validates the lists with the profile records
	 * 
	 * @return true if the profile records are accurate
	 */
	private boolean getAndValidateProfileRecords()
			throws ProfileFormatException {
		boolean mapOnly = (conf.getInt(MR_RED_TASKS, 1) == 0);

		mapRecords = getProfileRecords(ProfileToken.MAP);
		if (!validateMapRecords(mapRecords))
			return false;

		if (!mapOnly) {
			// There are reducers => spill and merge happened
			spillRecords = getProfileRecords(ProfileToken.SPILL);
			if (!validateSpillRecords(spillRecords))
				return false;

			mergeRecords = getProfileRecords(ProfileToken.MERGE);
			if (!validateMergeRecords(mergeRecords))
				return false;
		}

		return true;
	}

	/**
	 * Get the position of a map input with respect to the job inputs. The idea
	 * is figure out the job input that was used that lead to this particular
	 * map input.
	 * 
	 * Example: A map input "/usr/root/joins/orders/orders.tbl.1" can belong to
	 * a job input "/usr/root/joins/orders" or "/usr/root/joins/orders/orders.*"
	 * or "/usr/root/joins/orders/orders.tbl.[1-2]
	 * 
	 * @param jobInputs
	 *            the job inputs
	 * @param mapInput
	 *            the map input
	 * @return the job input position
	 */
	private int getMapInputPosition(String[] jobInputs, String mapInput) {

		if (mapInput == null || jobInputs == null || jobInputs.length == 0)
			return 0;

		int pos = -1;
		int numMatches = 0;

		// Find the matching job input
		for (int i = 0; i < jobInputs.length; ++i) {
			if (mapInput.matches(ProfileUtils.convertGlobToRegEx(jobInputs[i],
					false))) {
				pos = i;
				++numMatches;
			}
		}

		// Check for unique match
		if (pos != -1 && numMatches == 1)
			return pos;

		// The most common case for multiple matches is when the name of a
		// directory is a substring of the name of another directory
		pos = -1;
		numMatches = 0;
		for (int i = 0; i < jobInputs.length; ++i) {
			if (mapInput.matches(ProfileUtils.convertGlobToRegEx(jobInputs[i]
					+ "/", false))) {
				pos = i;
				++numMatches;
			}
		}

		return (pos == -1) ? 0 : pos;
	}

	/**
	 * Validate the number and order of records in a map phase. If a problem is
	 * detected, a ProfileFormatException is thrown.
	 * 
	 * @param records
	 *            the map records to validate
	 * @return true if the profile records are accurate
	 * @throws ProfileFormatException
	 */
	private boolean validateMapRecords(List<ProfileRecord> records)
			throws ProfileFormatException {
		if (records == null)
			return false;

		if (records.size() != NUM_MAP_PHASES) {
			throw new ProfileFormatException("Expected " + NUM_MAP_PHASES
					+ " records for the MAP phase for "
					+ this.profile.getTaskId());
		}

		int count = 0;
		count += records.get(POS_MAP_STARTUP_MEM).getProcess().equals(
				STARTUP_MEM) ? 0 : 1;
		count += records.get(POS_MAP_SETUP).getProcess().equals(SETUP) ? 0 : 1;
		count += records.get(POS_MAP_SETUP_MEM).getProcess().equals(SETUP_MEM) ? 0
				: 1;
		count += records.get(POS_MAP_CLEANUP).getProcess().equals(CLEANUP) ? 0
				: 1;
		count += records.get(POS_MAP_CLEANUP_MEM).getProcess().equals(
				CLEANUP_MEM) ? 0 : 1;
		count += records.get(POS_MAP_TOTAL_RUN).getProcess().equals(TOTAL_RUN) ? 0
				: 1;
		count += records.get(POS_MAP_READ).getProcess().equals(READ) ? 0 : 1;
		count += records.get(POS_MAP_UNCOMPRESS).getProcess()
				.equals(UNCOMPRESS) ? 0 : 1;
		count += records.get(POS_MAP_INPUT_K_BYTE_COUNT).getProcess().equals(
				KEY_BYTE_COUNT) ? 0 : 1;
		count += records.get(POS_MAP_INPUT_V_BYTE_COUNT).getProcess().equals(
				VALUE_BYTE_COUNT) ? 0 : 1;
		count += records.get(POS_MAP_MAP).getProcess().equals(MAP) ? 0 : 1;
		count += records.get(POS_MAP_WRITE).getProcess().equals(WRITE) ? 0 : 1;
		count += records.get(POS_MAP_COMPRESS).getProcess().equals(COMPRESS) ? 0
				: 1;
		count += records.get(POS_MAP_PARTITION_OUTPUT).getProcess().equals(
				PARTITION_OUTPUT) ? 0 : 1;
		count += records.get(POS_MAP_SERIALIZE_OUTPUT).getProcess().equals(
				SERIALIZE_OUTPUT) ? 0 : 1;
		count += records.get(POS_MAP_MEM).getProcess().equals(MAP_MEM) ? 0 : 1;
		count += records.get(POS_MAP_DIR_WRITE).getProcess().equals(WRITE) ? 0
				: 1;
		count += records.get(POS_MAP_DIR_COMPRESS).getProcess()
				.equals(COMPRESS) ? 0 : 1;
		count += records.get(POS_MAP_OUTPUT_K_BYTE_COUNT).getProcess().equals(
				KEY_BYTE_COUNT) ? 0 : 1;
		count += records.get(POS_MAP_OUTPUT_V_BYTE_COUNT).getProcess().equals(
				VALUE_BYTE_COUNT) ? 0 : 1;

		if (count != 0)
			throw new ProfileFormatException(
					"Incorrect sequence of records in MAP phase for "
							+ this.profile.getTaskId());

		return true;
	}

	/**
	 * Validate the number and order of records in a spill phase. If a problem
	 * is detected, a ProfileFormatException is thrown.
	 * 
	 * @param records
	 *            the spill records to validate
	 * @return true if the profile records are accurate
	 * @throws ProfileFormatException
	 */
	private boolean validateSpillRecords(List<ProfileRecord> records)
			throws ProfileFormatException {
		if (records == null)
			return false;

		if (records.size() % NUM_SPILL_PHASES != 0) {
			throw new ProfileFormatException("Expected groups of "
					+ NUM_SPILL_PHASES + " records for the SPILL phase for "
					+ this.profile.getTaskId());
		}

		int count = 0;
		for (int i = 0; i < records.size(); i += NUM_SPILL_PHASES) {
			count += records.get(i + POS_SPILL_SORT_AND_SPILL).getProcess()
					.equals(SORT_AND_SPILL) ? 0 : 1;
			count += records.get(i + POS_SPILL_QUICK_SORT).getProcess().equals(
					QUICK_SORT) ? 0 : 1;
			count += records.get(i + POS_SPILL_SORT_COUNT).getProcess().equals(
					SORT_COUNT) ? 0 : 1;
			count += records.get(i + POS_SPILL_COMBINE).getProcess().equals(
					COMBINE) ? 0 : 1;
			count += records.get(i + POS_SPILL_WRITE).getProcess()
					.equals(WRITE) ? 0 : 1;
			count += records.get(i + POS_SPILL_COMPRESS).getProcess().equals(
					COMPRESS) ? 0 : 1;
			count += records.get(i + POS_SPILL_UNCOMPRESS_BYTE_COUNT)
					.getProcess().equals(UNCOMPRESS_BYTE_COUNT) ? 0 : 1;
			count += records.get(i + POS_SPILL_COMPRESS_BYTE_COUNT)
					.getProcess().equals(COMPRESS_BYTE_COUNT) ? 0 : 1;
		}

		if (count != 0)
			throw new ProfileFormatException(
					"Incorrect sequence of records in SPILL phase for "
							+ this.profile.getTaskId());

		return true;
	}

	/**
	 * Validate the number and order of records in a merge phase. If a problem
	 * is detected, a ProfileFormatException is thrown.
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

		if (records.size() != NUM_MERGE_PHASES) {
			throw new ProfileFormatException("Expected " + NUM_MERGE_PHASES
					+ " records for the MERGE phase for "
					+ this.profile.getTaskId());
		}

		int count = 0;
		count += records.get(POS_MERGE_TOTAL_MERGE).getProcess().equals(
				TOTAL_MERGE) ? 0 : 1;
		count += records.get(POS_MERGE_READ_WRITE).getProcess().equals(
				READ_WRITE) ? 0 : 1;
		count += records.get(POS_MERGE_READ_WRITE_COUNT).getProcess().equals(
				READ_WRITE_COUNT) ? 0 : 1;
		count += records.get(POS_MERGE_UNCOMPRESS).getProcess().equals(
				UNCOMPRESS) ? 0 : 1;
		count += records.get(POS_MERGE_COMPRESS).getProcess().equals(COMPRESS) ? 0
				: 1;

		if (count != 0)
			throw new ProfileFormatException(
					"Incorrect sequence of records in MERGE phase for "
							+ this.profile.getTaskId());

		return true;
	}

}
