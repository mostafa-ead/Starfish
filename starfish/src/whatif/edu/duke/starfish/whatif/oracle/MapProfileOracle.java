package edu.duke.starfish.whatif.oracle;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.whatif.data.MapInputSpecs;

/**
 * This class is used to make predictions on how a map profile will change based
 * on a set of configuration settings.
 * 
 * The profile consists of four set of parameters:
 * <ol>
 * <li>Statistics</li>
 * <li>Counters</li>
 * <li>Cost Factors</li>
 * <li>Timings</li>
 * </ol>
 * 
 * The four virtual sets must be predicted in the above order since there exist
 * some forward dependencies. For example, predicting Counters requires
 * Statistics, and predicting Timings requires Counters and Cost Factors.
 * 
 * @author hero
 */
public class MapProfileOracle extends TaskProfileOracle {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private MRMapProfile sourceProf; // The source profile for the predictions
	private MRMapProfile virtualProf; // Cache the last predicted profile
	private Configuration conf; // Cache the last configuration
	private MapInputSpecs inputSpecs; // Cache the last input specs

	// Cache some commonly used variables
	private boolean isMapOnly = false;
	private boolean useCombiner = false;
	private boolean useInputCompr = false;
	private boolean useIntermCompr = false;
	private boolean useOutputCompr = false;
	private int sortFactor = 10;

	private double adjCombinePairsSel = 1d;
	private double adjCombineSizeSel = 1d;

	// State needed for Timings predictions
	private long numMergedRecords = 0l;
	private long numCombineInMergeRecs = 0l;

	/**
	 * Constructor
	 * 
	 * @param sourceProf
	 *            the source profile to base the predictions off
	 */
	public MapProfileOracle(MRMapProfile sourceProf) {
		this.sourceProf = sourceProf;
		this.virtualProf = null;
		this.conf = null;
		this.inputSpecs = null;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the source profile
	 */
	public MRMapProfile getSourceProf() {
		return sourceProf;
	}

	/**
	 * Generate and return a virtual map profile representing how the map will
	 * behave under the provided configuration settings.
	 * 
	 * @param conf
	 *            the configuration settings
	 * @param inputSpecs
	 *            the input specifications
	 * @return a virtual map profile
	 */
	public MRMapProfile whatif(Configuration conf, MapInputSpecs inputSpecs) {

		if (sourceProf.isEmpty()) {
			throw new RuntimeException(
					"Unable to process a what-if request. The source map profile "
							+ sourceProf.getTaskId() + " is empty!");
		}

		this.conf = conf;
		this.inputSpecs = inputSpecs;

		this.virtualProf = new MRMapProfile(getVirtualTaskId(sourceProf
				.getTaskId()));
		virtualProf.setNumTasks(inputSpecs.getNumSplits());
		virtualProf.setInputIndex(inputSpecs.getInputIndex());

		initializeCommonVariables();

		calcVirtualMapStatistics();

		calcVirtualMapCounters();

		calcVirtualMapCosts();

		calcVirtualMapTimings();

		return this.virtualProf;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Initialize some common variables based on the given configuration
	 */
	private void initializeCommonVariables() {
		isMapOnly = (conf.getInt(MR_RED_TASKS, 1) == 0);
		useCombiner = conf.get(MR_COMBINE_CLASS) != null
				&& conf.getBoolean(STARFISH_USE_COMBINER, true);
		useInputCompr = inputSpecs.isCompressed();
		useIntermCompr = conf.getBoolean(MR_COMPRESS_MAP_OUT, false);
		useOutputCompr = isMapOnly && conf.getBoolean(MR_COMPRESS_OUT, false);
		sortFactor = conf.getInt(MR_SORT_FACTOR, DEF_SORT_FACTOR);

		numMergedRecords = 0l;
		numCombineInMergeRecs = 0l;

		if (useCombiner) {
			// Get counters from the source profile
			long outputPairs = sourceProf
					.getCounter(MRCounter.MAP_OUTPUT_RECORDS);
			long outputSize = sourceProf.getCounter(MRCounter.MAP_OUTPUT_BYTES);
			double numSpills = sourceProf.getCounter(MRCounter.MAP_NUM_SPILLS);

			// Adjust the combiner selectivities
			adjCombinePairsSel = sourceProf.getStatistic(
					MRStatistics.COMBINE_PAIRS_SEL, DEF_SEL_ONE)
					* Math.log(outputPairs / numSpills);
			adjCombineSizeSel = sourceProf.getStatistic(
					MRStatistics.COMBINE_SIZE_SEL, DEF_SEL_ONE)
					* Math.log(outputSize / numSpills);
		}
	}

	/**
	 * Calculate the map statistics for a virtual profile based on the source
	 * profile and the suggested configuration.
	 */
	private void calcVirtualMapStatistics() {

		// Set the statistics common between mappers and reducers
		calcVirtualTaskStatistics(sourceProf, virtualProf, conf);

		// The following statistics remain the same
		virtualProf.addStatistic(MRStatistics.INPUT_PAIR_WIDTH, sourceProf
				.getStatistic(MRStatistics.INPUT_PAIR_WIDTH, DEF_PAIR_WIDTH));
		virtualProf.addStatistic(MRStatistics.MAP_SIZE_SEL, sourceProf
				.getStatistic(MRStatistics.MAP_SIZE_SEL, DEF_SEL_ONE));
		virtualProf.addStatistic(MRStatistics.MAP_PAIRS_SEL, sourceProf
				.getStatistic(MRStatistics.MAP_PAIRS_SEL, DEF_SEL_ONE));

		// Set input compression statistics
		if (useInputCompr) {
			virtualProf.addStatistic(MRStatistics.INPUT_COMPRESS_RATIO,
					sourceProf.getStatistic(MRStatistics.INPUT_COMPRESS_RATIO,
							DEF_COMPRESS_RATIO));
		}

		// Set output compression statistics if map-only job
		if (useOutputCompr) {
			virtualProf.addStatistic(MRStatistics.OUT_COMPRESS_RATIO,
					sourceProf.getStatistic(MRStatistics.OUT_COMPRESS_RATIO,
							DEF_COMPRESS_RATIO));
		}

		// Set the memory statistics
		virtualProf
				.addStatistic(MRStatistics.MAP_MEM_PER_RECORD, sourceProf
						.getStatistic(MRStatistics.MAP_MEM_PER_RECORD,
								DEF_MEM_PER_REC));
	}

	/**
	 * Calculate the map counters for a virtual profile based on the source
	 * profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics have been set
	 * 
	 */
	private void calcVirtualMapCounters() {

		// Initialize some counters
		if (!isMapOnly) {
			virtualProf.addCounter(MRCounter.SPILLED_RECORDS, 0l);
			virtualProf.addCounter(MRCounter.COMBINE_INPUT_RECORDS, 0l);
			virtualProf.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, 0l);
			virtualProf.addCounter(MRCounter.FILE_BYTES_READ, 0l);
			virtualProf.addCounter(MRCounter.FILE_BYTES_WRITTEN, 0l);
			virtualProf.addCounter(MRCounter.MAP_NUM_SPILL_MERGES, 0l);
		}

		// Calculate the read-and-map related counters
		calcVirtualMapCountersReadMapPhase();

		if (!isMapOnly) {
			// Calculate the spilling-related counters
			calcVirtualMapCountersSpillPhase();
			long numSpills = virtualProf.getCounter(MRCounter.MAP_NUM_SPILLS);

			// Calculate the merging-related counters
			if (numSpills > 1) {
				if (numSpills <= (sortFactor * sortFactor)) {
					// Use a model-based approach to calculate the counters
					calcVirtualMapCountersMergePhase();

				} else {
					// Use a simulation-based approach to calculate the counters
					calcVirtualMapCountersSimMergePhase();
				}
			}
		}
	}

	/**
	 * Calculate the map counters related to the read and map phases for a
	 * virtual profile based on the source profile and the suggested
	 * configuration.
	 * 
	 * Assumptions: All virtualProf statistics have been set
	 * 
	 */
	private void calcVirtualMapCountersReadMapPhase() {

		// Calculate the map input counters
		double mapInputBytes = inputSpecs.getSize();
		if (useInputCompr)
			mapInputBytes /= virtualProf
					.getStatistic(MRStatistics.INPUT_COMPRESS_RATIO);
		double mapInputRecs = mapInputBytes
				/ virtualProf.getStatistic(MRStatistics.INPUT_PAIR_WIDTH);

		virtualProf.addCounter(MRCounter.HDFS_BYTES_READ, inputSpecs.getSize());
		virtualProf.addCounter(MRCounter.MAP_INPUT_BYTES, (long) mapInputBytes);
		virtualProf
				.addCounter(MRCounter.MAP_INPUT_RECORDS, (long) mapInputRecs);

		// Calculate the map output counters
		double mapOutBytes = mapInputBytes
				* virtualProf.getStatistic(MRStatistics.MAP_SIZE_SEL,
						DEF_SEL_ONE);
		double mapOutRecs = mapInputRecs
				* virtualProf.getStatistic(MRStatistics.MAP_PAIRS_SEL,
						DEF_SEL_ONE);

		if (mapOutBytes < 1 || mapOutRecs < 1) {
			// By default, always assume at least one record
			mapOutBytes = sourceProf.getCounter(MRCounter.MAP_OUTPUT_BYTES, 1l)
					/ (double) sourceProf.getCounter(
							MRCounter.MAP_OUTPUT_RECORDS, 1l);
			mapOutRecs = 1d;
		}

		virtualProf.addCounter(MRCounter.MAP_OUTPUT_BYTES, (long) mapOutBytes);
		virtualProf.addCounter(MRCounter.MAP_OUTPUT_RECORDS, (long) mapOutRecs);

		// Calculate the max number of unique groups produced by a mapper
		long maxSourceUniqueGroups = sourceProf.getCounter(
				MRCounter.MAP_MAX_UNIQUE_GROUPS, DEF_MAX_UNIQUE_GROUPS);
		double maxUniqueGroups = mapOutRecs * maxSourceUniqueGroups
				/ sourceProf.getCounter(MRCounter.MAP_OUTPUT_RECORDS, 1l);
		virtualProf.addCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS, (long) Math
				.ceil(maxUniqueGroups));

		// Calculate the number of bytes written on HDFS
		if (isMapOnly) {
			if (useOutputCompr)
				mapOutBytes *= virtualProf
						.getStatistic(MRStatistics.OUT_COMPRESS_RATIO);
			virtualProf.addCounter(MRCounter.HDFS_BYTES_WRITTEN,
					(long) mapOutBytes);
		}
	}

	/**
	 * Calculate the map counters related to the spill phase for a virtual
	 * profile based on the source profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics have been set
	 * 
	 */
	private void calcVirtualMapCountersSpillPhase() {

		long mapOutBytes = virtualProf.getCounter(MRCounter.MAP_OUTPUT_BYTES);
		long mapOutRecs = virtualProf.getCounter(MRCounter.MAP_OUTPUT_RECORDS);
		double mapOutRecWidth = mapOutBytes / (double) mapOutRecs;

		// Calculate the number of records in the output buffer
		long maxSerPairs = (long) Math
				.floor(((conf.getInt(MR_SORT_MB, DEF_SORT_MB) * 1024 * 1024)
						* (1 - conf.getFloat(MR_SORT_REC_PERC,
								DEF_SORT_REC_PERC)) * conf.getFloat(
						MR_SPILL_PERC, DEF_SPILL_PERC))
						/ mapOutRecWidth);
		long maxAccPairs = (long) Math.floor(((conf.getInt(MR_SORT_MB,
				DEF_SORT_MB) * 1024 * 1024) * conf.getFloat(MR_SORT_REC_PERC,
				DEF_SORT_REC_PERC))
				* conf.getFloat(MR_SPILL_PERC, DEF_SPILL_PERC) / 16.0);
		long maxSpillBufferPairs = Math.min(Math.min(maxSerPairs, maxAccPairs),
				mapOutRecs);

		// Calculate and set the average buffer size and records
		long numSpills = (long) Math.ceil(mapOutRecs
				/ (double) maxSpillBufferPairs);
		long avgSpillBufferPairs = Math.round(mapOutRecs / (double) numSpills);
		long avgSpillBufferSize = Math.round(mapOutBytes / (double) numSpills);

		if (avgSpillBufferPairs == 0 || avgSpillBufferSize == 0) {
			avgSpillBufferPairs = 1l;
			avgSpillBufferSize = (long) mapOutRecWidth;
		}

		virtualProf.addCounter(MRCounter.MAP_RECS_PER_BUFF_SPILL,
				avgSpillBufferPairs);
		virtualProf.addCounter(MRCounter.MAP_BUFF_SPILL_SIZE,
				avgSpillBufferSize);

		// Calculate the spill counters
		double spillFilePairs = avgSpillBufferPairs;
		double spillFileSize = avgSpillBufferSize;

		if (useCombiner) {
			// Calculate the spills counters after the combiner
			spillFilePairs = avgSpillBufferPairs * adjCombinePairsSel
					/ Math.log(avgSpillBufferPairs);
			spillFileSize = avgSpillBufferSize * adjCombineSizeSel
					/ Math.log(avgSpillBufferSize);
		}

		if (useIntermCompr)
			spillFileSize *= virtualProf
					.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO);

		virtualProf.addCounter(MRCounter.MAP_NUM_SPILLS, numSpills);
		virtualProf.addCounter(MRCounter.MAP_RECORDS_PER_SPILL,
				(long) spillFilePairs);
		virtualProf.addCounter(MRCounter.MAP_SPILL_SIZE, (long) spillFileSize);

		// Calculate the number of records to spill
		long numSpilledRecords = (long) Math.round(numSpills * spillFilePairs);
		virtualProf.addCounter(MRCounter.SPILLED_RECORDS, numSpilledRecords);

		// Set the combiner counters
		if (useCombiner) {
			virtualProf.addCounter(MRCounter.COMBINE_INPUT_RECORDS, mapOutRecs);
			virtualProf.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS,
					numSpilledRecords);
		}

		// Calculate the number of bytes spilled during the spill phase
		long bytesWritten = (long) Math.round(numSpills * spillFileSize);
		virtualProf.addCounter(MRCounter.FILE_BYTES_WRITTEN, bytesWritten);
	}

	/**
	 * Calculate the map counters related to the merge phase for a virtual
	 * profile based on the source profile and the suggested configuration.
	 * 
	 * Warning: This method should only be called when 1 < numSpills <= factor^2
	 * 
	 * Assumptions: All virtualProf statistics have been set
	 * 
	 */
	private void calcVirtualMapCountersMergePhase() {

		// Calculate the number of spill merges
		long numSpills = virtualProf.getCounter(MRCounter.MAP_NUM_SPILLS);
		virtualProf.addCounter(MRCounter.MAP_NUM_SPILL_MERGES,
				getNumSpillMerges(numSpills, sortFactor));

		// Determine if the combiner will be used during the final merge pass
		boolean useCombinerInFinalMerge = false;
		if (useCombiner) {
			long numSpillsFinalMerge = (numSpills < sortFactor) ? numSpills
					: sortFactor;
			if (numSpillsFinalMerge >= conf.getInt(MR_NUM_SPILLS_COMBINE,
					DEF_NUM_SPILLS_FOR_COMB)) {
				// The combiner will be used during the final merge round
				useCombinerInFinalMerge = true;
			}
		}

		// Calculate the number of additional spilled records during merging
		long numSpilledRecords = virtualProf
				.getCounter(MRCounter.SPILLED_RECORDS);
		long numRecsPerSpill = virtualProf
				.getCounter(MRCounter.MAP_RECORDS_PER_SPILL);
		long numIntermSpills = getNumIntermSpillReads(numSpills, sortFactor);
		numSpilledRecords += numIntermSpills * numRecsPerSpill;
		numMergedRecords = numSpilledRecords;

		// Calculate the number of records going through the combiner
		if (useCombinerInFinalMerge) {
			numCombineInMergeRecs = numSpills * numRecsPerSpill;

			long combineOutRecsFinalMerge = (long) (numCombineInMergeRecs
					* adjCombinePairsSel / Math.log(numCombineInMergeRecs));
			combineOutRecsFinalMerge = Math.max(combineOutRecsFinalMerge,
					numRecsPerSpill);
			if (virtualProf.getCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS) < numCombineInMergeRecs) {
				combineOutRecsFinalMerge = Math
						.max(combineOutRecsFinalMerge, virtualProf
								.getCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS));
			}

			// Calculate the combiner counters
			long combineInRecs = virtualProf
					.getCounter(MRCounter.COMBINE_INPUT_RECORDS)
					+ numCombineInMergeRecs;
			long combineOutRecs = virtualProf
					.getCounter(MRCounter.COMBINE_OUTPUT_RECORDS)
					+ combineOutRecsFinalMerge;

			virtualProf.addCounter(MRCounter.COMBINE_INPUT_RECORDS,
					combineInRecs);
			virtualProf.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS,
					combineOutRecs);

			// Calculate the number of spilled records
			numSpilledRecords += combineOutRecsFinalMerge;
		} else {
			numSpilledRecords += numSpills * numRecsPerSpill;
		}
		virtualProf.addCounter(MRCounter.SPILLED_RECORDS, numSpilledRecords);

		// Calculate the number of bytes read from the local file system
		long numBytesPerSpill = virtualProf
				.getCounter(MRCounter.MAP_SPILL_SIZE);
		long bytesRead = (numIntermSpills + numSpills) * numBytesPerSpill;
		virtualProf.addCounter(MRCounter.FILE_BYTES_READ, bytesRead);

		// Calculate the number of bytes written to the local file system
		long bytesWritten = virtualProf
				.getCounter(MRCounter.FILE_BYTES_WRITTEN);
		bytesWritten += numIntermSpills * numBytesPerSpill;
		if (useCombinerInFinalMerge) {
			// Calculate the number of bytes produced by the combiner
			double bytesWrittenFinalMerge = numSpills * numBytesPerSpill
					* adjCombineSizeSel
					/ Math.log(numSpills * numBytesPerSpill);

			bytesWrittenFinalMerge = Math.max(bytesWrittenFinalMerge,
					numBytesPerSpill);
			if (virtualProf.getCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS) < numCombineInMergeRecs) {
				// Calculate the max size produced by the combiner
				long mapOutBytes = virtualProf
						.getCounter(MRCounter.MAP_OUTPUT_BYTES);
				long mapOutRecs = virtualProf
						.getCounter(MRCounter.MAP_OUTPUT_RECORDS);
				double maxOutput = (mapOutBytes / (double) mapOutRecs)
						* virtualProf
								.getCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS);

				bytesWrittenFinalMerge = Math.max(bytesWrittenFinalMerge,
						maxOutput);
			}

			bytesWritten += bytesWrittenFinalMerge;
		} else {
			bytesWritten += numSpills * numBytesPerSpill;
		}
		virtualProf.addCounter(MRCounter.FILE_BYTES_WRITTEN, bytesWritten);
	}

	/**
	 * Calculate the map counters related to the merge phase for a virtual
	 * profile based on the source profile and the suggested configuration,
	 * using simulation.
	 * 
	 * Assumptions: All virtualProf statistics have been set
	 * 
	 */
	private void calcVirtualMapCountersSimMergePhase() {

		// Set up the simulation
		MergeSimulator merger = new MergeSimulator();
		merger.addSegments(virtualProf.getCounter(MRCounter.MAP_NUM_SPILLS),
				virtualProf.getCounter(MRCounter.MAP_SPILL_SIZE), virtualProf
						.getCounter(MRCounter.MAP_RECORDS_PER_SPILL));

		// Enable the combiner, if any
		if (useCombiner) {
			merger.enableCombiner(conf.getInt(MR_NUM_SPILLS_COMBINE,
					DEF_NUM_SPILLS_FOR_COMB), adjCombineSizeSel,
					adjCombinePairsSel);
		}

		// Perform the simulation
		merger.simulateMerge(sortFactor);

		// Set the counters
		virtualProf.addCounter(MRCounter.MAP_NUM_SPILL_MERGES, merger
				.getNumMergePasses());
		virtualProf.addCounter(MRCounter.SPILLED_RECORDS, virtualProf
				.getCounter(MRCounter.SPILLED_RECORDS, 0l)
				+ merger.getSpilledRecords());
		virtualProf.addCounter(MRCounter.FILE_BYTES_READ, virtualProf
				.getCounter(MRCounter.FILE_BYTES_READ, 0l)
				+ merger.getBytesRead());
		virtualProf.addCounter(MRCounter.FILE_BYTES_WRITTEN, virtualProf
				.getCounter(MRCounter.FILE_BYTES_WRITTEN, 0l)
				+ merger.getBytesWritten());
		numMergedRecords = merger.getMergedRecords();

		if (useCombiner) {
			numCombineInMergeRecs = merger.getCombineInRecs();
			virtualProf.addCounter(MRCounter.COMBINE_INPUT_RECORDS, virtualProf
					.getCounter(MRCounter.COMBINE_INPUT_RECORDS)
					+ merger.getCombineInRecs());
			virtualProf.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS,
					virtualProf.getCounter(MRCounter.COMBINE_OUTPUT_RECORDS)
							+ merger.getCombineOutRecs());
		}
	}

	/**
	 * Calculate the map costs for a virtual profile based on the source profile
	 * and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics and counters have been set
	 * 
	 */
	private void calcVirtualMapCosts() {

		// Set the statistics common between mappers and reducers
		calcVirtualTaskCosts(sourceProf, virtualProf, conf);

		// Set input compression cost
		if (useInputCompr) {
			virtualProf.addCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST,
					sourceProf.getCostFactor(
							MRCostFactors.INPUT_UNCOMPRESS_CPU_COST,
							DEF_COST_CPU_UNCOMPRESS));

			if (virtualProf
					.getCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST) == 0d)
				virtualProf.addCostFactor(
						MRCostFactors.INPUT_UNCOMPRESS_CPU_COST,
						DEF_COST_CPU_UNCOMPRESS);
		}

		// Set output compression cost
		if (useOutputCompr) {
			virtualProf.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
					sourceProf.getCostFactor(
							MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
							DEF_COST_CPU_COMPRESS));

			if (virtualProf
					.getCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST) == 0d)
				virtualProf.addCostFactor(
						MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
						DEF_COST_CPU_COMPRESS);
		}
	}

	/**
	 * Calculate the map timings for a virtual profile based on the source
	 * profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics, counters, and costs have been
	 * set
	 * 
	 */
	private void calcVirtualMapTimings() {

		// Calculate and set SETUP, READ, MAP, CLEANUP [, WRITE]
		calcVirtualMapTimingsReadMapPhase();

		if (!isMapOnly) {
			// Calculate and set COLLECT and SPILL
			calcVirtualMapTimingsSpillPhase();

			// Calculate and set MERGE
			calcVirtualMapTimingsMergePhase();
		}
	}

	/**
	 * Calculate the map timings related to the read and map phases for a
	 * virtual profile based on the source profile and the suggested
	 * configuration.
	 * 
	 * Assumptions: All virtualProf statistics, counters, and costs have been
	 * set
	 * 
	 */
	private void calcVirtualMapTimingsReadMapPhase() {

		// Calculate and set SETUP
		virtualProf.addTiming(MRTaskPhase.SETUP, sourceProf.getTiming(
				MRTaskPhase.SETUP, 0d));

		// Calculate and set READ
		double bytesRead = virtualProf.getCounter(MRCounter.HDFS_BYTES_READ);
		double readCPU = 0d;
		if (useInputCompr)
			readCPU = bytesRead
					* virtualProf
							.getCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST);
		double readIO = bytesRead
				* virtualProf.getCostFactor(MRCostFactors.READ_HDFS_IO_COST);
		virtualProf.addTiming(MRTaskPhase.READ, (readCPU + readIO) / NS_PER_MS);

		// Calculate and set MAP
		double mapCPU = virtualProf.getCounter(MRCounter.MAP_INPUT_RECORDS)
				* virtualProf.getCostFactor(MRCostFactors.MAP_CPU_COST);
		virtualProf.addTiming(MRTaskPhase.MAP, mapCPU / NS_PER_MS);

		// Calculate and set CLEANUP
		virtualProf.addTiming(MRTaskPhase.CLEANUP, sourceProf.getTiming(
				MRTaskPhase.CLEANUP, 0d));

		if (isMapOnly) {
			// Calculate and set WRITE
			double writeCPU = 0d;
			if (useOutputCompr)
				writeCPU = virtualProf.getCounter(MRCounter.MAP_OUTPUT_BYTES)
						* virtualProf
								.getCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST);
			double writeIO = virtualProf
					.getCounter(MRCounter.HDFS_BYTES_WRITTEN)
					* virtualProf
							.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST);
			virtualProf.addTiming(MRTaskPhase.WRITE, (writeCPU + writeIO)
					/ NS_PER_MS);
		}
	}

	/**
	 * Calculate the map timings related to the collect and spill phases for a
	 * virtual profile based on the source profile and the suggested
	 * configuration.
	 * 
	 * Assumptions: All virtualProf statistics, counters, and costs have been
	 * set
	 * 
	 */
	private void calcVirtualMapTimingsSpillPhase() {

		// Get useful counters
		long mapOutRecs = virtualProf.getCounter(MRCounter.MAP_OUTPUT_RECORDS);
		long writesFromSpill = virtualProf.getCounter(MRCounter.MAP_NUM_SPILLS)
				* virtualProf.getCounter(MRCounter.MAP_SPILL_SIZE);

		// Calculate and set COLLECT (partition + serialization)
		double collectCPU = mapOutRecs
				* virtualProf.getCostFactor(MRCostFactors.PARTITION_CPU_COST)
				+ mapOutRecs
				* virtualProf.getCostFactor(MRCostFactors.SERDE_CPU_COST);
		virtualProf.addTiming(MRTaskPhase.COLLECT, collectCPU / NS_PER_MS);

		// CPU cost for SPILL
		double numRecsPerRed = virtualProf
				.getCounter(MRCounter.MAP_RECS_PER_BUFF_SPILL)
				/ (double) conf.getInt(MR_RED_TASKS, 1);
		double sortCPU = mapOutRecs
				* Math.log((numRecsPerRed < 10) ? 10 : numRecsPerRed)
				* virtualProf.getCostFactor(MRCostFactors.SORT_CPU_COST);

		double combineCPU = 0d;
		if (useCombiner)
			combineCPU = mapOutRecs
					* virtualProf.getCostFactor(MRCostFactors.COMBINE_CPU_COST);

		double comprCPU = 0d;
		if (useIntermCompr)
			comprCPU = writesFromSpill
					* virtualProf
							.getCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST)
					/ virtualProf
							.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO);

		// IO cost for SPILL
		double spillIO = writesFromSpill
				* virtualProf.getCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST);

		// Calculate and set SPILL (sort + combine + compress)
		virtualProf.addTiming(MRTaskPhase.SPILL, (sortCPU + combineCPU
				+ comprCPU + spillIO)
				/ NS_PER_MS);
	}

	/**
	 * Calculate the map timings related to the merge phase for a virtual
	 * profile based on the source profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics, counters, and costs have been
	 * set
	 * 
	 */
	private void calcVirtualMapTimingsMergePhase() {

		// Get useful counters
		long localReads = virtualProf.getCounter(MRCounter.FILE_BYTES_READ);
		long localWrites = virtualProf.getCounter(MRCounter.FILE_BYTES_WRITTEN);
		long writesFromSpill = virtualProf.getCounter(MRCounter.MAP_NUM_SPILLS)
				* virtualProf.getCounter(MRCounter.MAP_SPILL_SIZE);

		// CPU cost
		double uncomprCPU = 0;
		double comprCPU = 0d;
		if (useIntermCompr) {
			uncomprCPU = localReads
					* virtualProf
							.getCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST);
			comprCPU = (localWrites - writesFromSpill)
					* virtualProf
							.getCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST)
					/ virtualProf
							.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO);
		}

		double mergeCPU = numMergedRecords
				* virtualProf.getCostFactor(MRCostFactors.MERGE_CPU_COST);

		double combineCPU = 0d;
		if (useCombiner)
			combineCPU = numCombineInMergeRecs
					* virtualProf.getCostFactor(MRCostFactors.COMBINE_CPU_COST);

		// IO cost
		double mergeReadIO = localReads
				* virtualProf.getCostFactor(MRCostFactors.READ_LOCAL_IO_COST);
		double mergeWriteIO = (localWrites - writesFromSpill)
				* virtualProf.getCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST);

		// Calculate and set MERGE
		virtualProf.addTiming(MRTaskPhase.MERGE, (uncomprCPU + mergeCPU
				+ combineCPU + comprCPU + mergeReadIO + mergeWriteIO)
				/ NS_PER_MS);
	}

}
