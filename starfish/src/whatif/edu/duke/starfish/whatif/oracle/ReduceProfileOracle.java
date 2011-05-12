package edu.duke.starfish.whatif.oracle;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;
import edu.duke.starfish.whatif.data.ReduceShuffleSpecs;

/**
 * This class is used to make predictions on how a reduce profile will change
 * based on a set of configuration settings.
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
public class ReduceProfileOracle extends TaskProfileOracle {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private MRReduceProfile sourceProf; // The source profile for predictions
	private MRReduceProfile virtualProf; // Cache the last predicted profile
	private Configuration conf; // Cache the last configuration
	private ReduceShuffleSpecs shuffleSpecs; // Cache the last shuffle specs

	// Cache state from the shuffle phase
	private double mergedRecordsInShuffle;
	private long bytesReadInMergeInShuffle;
	private long bytesWrittenInMergeInShuffle;

	// Cache state from the sort phase
	private double mergedRecordsInSort;
	private long bytesReadInMergeInSort;
	private long bytesWrittenInMergeInSort;
	private long bytesReadInReduce;

	// Cache some commonly used variables
	private boolean useCombiner = false;
	private boolean useIntermCompr = false;
	private boolean useOutputCompr = false;

	/**
	 * Constructor
	 * 
	 * @param sourceProf
	 *            the source profile to base the predictions off
	 */
	public ReduceProfileOracle(MRReduceProfile sourceProf) {
		this.sourceProf = sourceProf;
		this.virtualProf = null;
		this.conf = null;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the source profile
	 */
	public MRReduceProfile getSourceProf() {
		return sourceProf;
	}

	/**
	 * Generate and return a virtual reduce profile representing how the map
	 * will behave under the provided configuration settings.
	 * 
	 * @param conf
	 *            the configuration settings
	 * @param shuffleSpecs
	 *            the shuffle specifications
	 * @return a virtual map profile
	 */
	public MRReduceProfile whatif(Configuration conf,
			ReduceShuffleSpecs shuffleSpecs) {

		if (sourceProf.isEmpty()) {
			throw new RuntimeException(
					"Unable to process a what-if request. The source reduce profile "
							+ sourceProf.getTaskId() + " is empty!");
		}

		this.virtualProf = new MRReduceProfile(getVirtualTaskId(sourceProf
				.getTaskId()));
		virtualProf.setNumTasks(shuffleSpecs.getNumReducers());
		this.conf = conf;
		this.shuffleSpecs = shuffleSpecs;

		initializeCommonVariables();

		calcVirtualReduceStatistics();

		calcVirtualReduceCounters();

		calcVirtualReduceCosts();

		calcVirtualReduceTimings();

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
		mergedRecordsInShuffle = 0d;
		bytesReadInMergeInShuffle = 0l;
		bytesWrittenInMergeInShuffle = 0l;

		mergedRecordsInSort = 0d;
		bytesReadInMergeInSort = 0l;
		bytesWrittenInMergeInSort = 0l;
		bytesReadInReduce = 0l;

		useCombiner = conf.get(MR_COMBINE_CLASS) != null
				&& conf.getBoolean(STARFISH_USE_COMBINER, true);
		useIntermCompr = conf.getBoolean(MR_COMPRESS_MAP_OUT, false);
		useOutputCompr = conf.getBoolean(MR_COMPRESS_OUT, false);
	}

	/**
	 * Calculate the reduce statistics for a virtual profile based on the source
	 * profile and the suggested configuration.
	 * 
	 */
	private void calcVirtualReduceStatistics() {

		// Set the statistics common between mappers and reducers
		calcVirtualTaskStatistics(sourceProf, virtualProf, conf);

		// Set the reduce statistics
		virtualProf.addStatistic(MRStatistics.REDUCE_PAIRS_PER_GROUP,
				sourceProf.getStatistic(MRStatistics.REDUCE_PAIRS_PER_GROUP,
						DEF_SEL_ONE));
		virtualProf.addStatistic(MRStatistics.REDUCE_SIZE_SEL, sourceProf
				.getStatistic(MRStatistics.REDUCE_SIZE_SEL, DEF_SEL_ONE));
		virtualProf.addStatistic(MRStatistics.REDUCE_PAIRS_SEL, sourceProf
				.getStatistic(MRStatistics.REDUCE_PAIRS_SEL, DEF_SEL_ONE));

		// Set output compression statistics
		if (useOutputCompr) {
			virtualProf.addStatistic(MRStatistics.OUT_COMPRESS_RATIO,
					sourceProf.getStatistic(MRStatistics.OUT_COMPRESS_RATIO,
							DEF_COMPRESS_RATIO));
		}

		// Set the memory statistics
		virtualProf.addStatistic(MRStatistics.REDUCE_MEM_PER_RECORD, sourceProf
				.getStatistic(MRStatistics.REDUCE_MEM_PER_RECORD,
						DEF_MEM_PER_REC));
	}

	/**
	 * Calculate the reduce counters for a virtual profile based on the source
	 * profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics have been set
	 * 
	 */
	private void calcVirtualReduceCounters() {

		virtualProf.addCounter(MRCounter.COMBINE_INPUT_RECORDS, 0l);
		virtualProf.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, 0l);

		calcVirtualReduceCountersShuffleSortPhase();

		calcVirtualReduceCountersReducePhase();
	}

	/**
	 * Calculate the reduce counters related to the shuffle and sort phases for
	 * a virtual profile based on the source profile and the suggested
	 * configuration.
	 * 
	 * Assumptions: All virtualProf statistics have been set
	 * 
	 */
	private void calcVirtualReduceCountersShuffleSortPhase() {

		// Get the data shuffled to a single reduce
		virtualProf.addCounter(MRCounter.REDUCE_SHUFFLE_BYTES, shuffleSpecs
				.getSize());

		// Calculate segment information (segment = output partition from 1 map)
		int numMappers = shuffleSpecs.getNumMappers();
		double segmentComprSize = shuffleSpecs.getSize() / (double) numMappers;
		double segmentUncomprSize = segmentComprSize;
		if (useIntermCompr)
			segmentUncomprSize /= virtualProf
					.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO);
		double segmentPairs = shuffleSpecs.getRecords() / (double) numMappers;

		// We will incrementally keep track of the combiner and spilled counters
		double combineInRecs = 0d;
		double combineOutRecs = 0d;
		double numSpilledRecs = 0d;

		// The shuffled data are placed either in memory buffer or on disk
		long taskMem = ProfileUtils.getTaskMemory(conf);
		double shuffleBufferSize = conf.getFloat(MR_SHUFFLE_IN_BUFF_PERC,
				DEF_SHUFFLE_IN_BUFF_PERC)
				* taskMem;
		double mergeSizeThr = conf.getFloat(MR_SHUFFLE_MERGE_PERC,
				DEF_SHUFFLE_MERGE_PERC)
				* shuffleBufferSize;
		long inMemMergeThr = conf.getLong(MR_INMEM_MERGE, DEF_INMEM_MERGE);

		// When buffer reaches size of mergeSizeThr or number of segments
		// exceeds InMemMergeThr, segments are merged and spilled to disk
		// creating shuffle files.
		double numSegInShuffleFile = 0d;
		double shuffleFileSize = 0d;
		double shuffleFilePairs = 0d;
		long numShuffleFiles = 0l;
		long numSegmentsInMem = 0l;

		if (segmentUncomprSize < 0.25 * shuffleBufferSize) {
			// The segments are small and go to memory
			numSegInShuffleFile = mergeSizeThr / segmentUncomprSize;

			if (Math.ceil(numSegInShuffleFile) * segmentUncomprSize <= shuffleBufferSize)
				numSegInShuffleFile = Math.ceil(numSegInShuffleFile);
			else
				numSegInShuffleFile = Math.floor(numSegInShuffleFile);

			if (numSegInShuffleFile > inMemMergeThr)
				numSegInShuffleFile = inMemMergeThr;

			// Calculate info about the shuffle files
			shuffleFileSize = numSegInShuffleFile * segmentComprSize;
			shuffleFilePairs = numSegInShuffleFile * segmentPairs;
			if (useCombiner) {
				shuffleFileSize *= virtualProf
						.getStatistic(MRStatistics.COMBINE_SIZE_SEL);
				shuffleFilePairs *= virtualProf
						.getStatistic(MRStatistics.COMBINE_PAIRS_SEL);
			}

			numShuffleFiles = (long) Math.floor(numMappers
					/ numSegInShuffleFile);
			numSegmentsInMem = numMappers % (long) numSegInShuffleFile;

			// Calculate combiner and spilled records counters
			if (numShuffleFiles > 0l) {
				combineInRecs += numShuffleFiles * numSegInShuffleFile
						* segmentPairs;
				if (useCombiner)
					combineOutRecs += combineInRecs
							* virtualProf
									.getStatistic(MRStatistics.COMBINE_PAIRS_SEL);
				else
					combineOutRecs += combineInRecs;

				numSpilledRecs += combineOutRecs;
				mergedRecordsInShuffle += combineInRecs;
			}

		} else {
			// The segments are large and go straight to disk
			numSegInShuffleFile = 1;
			shuffleFileSize = segmentComprSize;
			shuffleFilePairs = segmentPairs;
			numShuffleFiles = numMappers;
			numSegmentsInMem = 0;
		}

		// When the number of shuffle files on disk is greater than
		// (2*sortFactor-1), they are merged
		int sortFactor = conf.getInt(MR_SORT_FACTOR, DEF_SORT_FACTOR);
		long numShuffleMerges = 0l;
		if (numShuffleFiles >= 2 * sortFactor - 1) {
			numShuffleMerges = 1l + (long) Math.floor(((numShuffleFiles - 2
					* sortFactor + 1) / (double) sortFactor));
		}

		// At the end of the shuffle files, a set of merged and unmerged shuffle
		// files will exist on disk (plus in-memory segments)
		long numMergedShufFiles = numShuffleMerges;
		double mergedShufFileSize = sortFactor * shuffleFileSize;
		double mergedShufFilePairs = sortFactor * shuffleFilePairs;

		long numUnmergShufFiles = numShuffleFiles
				- (sortFactor * numShuffleMerges);
		double unmergShufFileSize = shuffleFileSize;
		double unmergShufFilePairs = shuffleFilePairs;

		// Store the current state
		bytesReadInMergeInShuffle = (long) (numMergedShufFiles * mergedShufFileSize);
		bytesWrittenInMergeInShuffle = (long) (numShuffleFiles
				* shuffleFileSize + numMergedShufFiles * mergedShufFileSize);
		mergedRecordsInShuffle += numMergedShufFiles * mergedShufFilePairs;
		numSpilledRecs += numMergedShufFiles * mergedShufFilePairs;

		// SORT PHASE BEGINS
		// Evict segments from memory to satisfy memory constraint
		double maxSegmentBuffer = taskMem
				* conf.getFloat(MR_RED_IN_BUFF_PERC, DEF_RED_IN_BUFF_PERC);
		double currSegmentBuffer = numSegmentsInMem * segmentUncomprSize;
		long numSegmentsEvicted = 0;
		if (currSegmentBuffer > maxSegmentBuffer) {
			numSegmentsEvicted = (long) Math
					.ceil((currSegmentBuffer - maxSegmentBuffer)
							/ segmentUncomprSize);
		}

		long numSegmentsRemainMem = numSegmentsInMem - numSegmentsEvicted;

		// Merging to disk will occur if the number of existing shuffle files on
		// disk are less than the sortFactor
		int numFilesMergedFromMem = 0;
		double filesFromMemSize = 0d;
		double filesFromMemPairs = 0d;
		if (numSegmentsEvicted > 0
				&& numMergedShufFiles + numUnmergShufFiles < sortFactor) {
			numFilesMergedFromMem = 1;
			filesFromMemSize = numSegmentsEvicted * segmentComprSize;
			filesFromMemPairs = numSegmentsEvicted * segmentPairs;

			// Update counters
			bytesWrittenInMergeInSort += filesFromMemSize;
			numSpilledRecs += filesFromMemPairs;
			mergedRecordsInSort += filesFromMemPairs;
		}

		if (numMergedShufFiles + numUnmergShufFiles + numFilesMergedFromMem > sortFactor) {
			// Perform intermediate merging
			MergeSimulator merger = new MergeSimulator();
			merger.addSegments(numMergedShufFiles, (long) mergedShufFileSize,
					(long) mergedShufFilePairs);
			merger.addSegments(numUnmergShufFiles, (long) unmergShufFileSize,
					(long) unmergShufFilePairs);
			merger.addSegments(numFilesMergedFromMem, (long) filesFromMemSize,
					(long) filesFromMemPairs);

			merger.addMemSegments(numSegmentsEvicted, (long) segmentComprSize,
					(long) segmentPairs);

			merger.simulateMerge(sortFactor, true);

			mergedRecordsInSort += merger.getMergedRecords();
			numSpilledRecs += merger.getSpilledRecords();
			bytesReadInMergeInSort += merger.getBytesRead();
			bytesWrittenInMergeInSort += merger.getBytesWritten();
		}

		// The input resides in memory and/or on disk
		double redInputOnDiskSize = numMergedShufFiles * mergedShufFileSize
				+ numUnmergShufFiles * unmergShufFileSize
				+ numFilesMergedFromMem * filesFromMemSize;
		double redInputOnDiskPairs = numMergedShufFiles * mergedShufFilePairs
				+ numUnmergShufFiles * unmergShufFilePairs
				+ numFilesMergedFromMem * filesFromMemPairs;

		// Store the current state
		bytesReadInReduce = (long) redInputOnDiskSize;

		// Calculate the reduce input counters
		double redInputSize = redInputOnDiskSize + numSegmentsRemainMem
				* segmentComprSize;
		if (useIntermCompr)
			redInputSize /= virtualProf
					.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO);
		double redInputPairs = redInputOnDiskPairs + numSegmentsRemainMem
				* segmentPairs;

		virtualProf.addCounter(MRCounter.REDUCE_INPUT_BYTES,
				(long) redInputSize);
		virtualProf.addCounter(MRCounter.REDUCE_INPUT_RECORDS,
				(long) redInputPairs);

		// Set the total counters for the combiner and spilled records
		if (useCombiner) {
			virtualProf.addCounter(MRCounter.COMBINE_INPUT_RECORDS,
					(long) combineInRecs);
			virtualProf.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS,
					(long) combineOutRecs);
		}
		virtualProf
				.addCounter(MRCounter.SPILLED_RECORDS, (long) numSpilledRecs);

		// Set the total file I/O
		virtualProf.addCounter(MRCounter.FILE_BYTES_READ,
				bytesReadInMergeInShuffle + bytesReadInMergeInSort
						+ bytesReadInReduce);
		virtualProf.addCounter(MRCounter.FILE_BYTES_WRITTEN,
				bytesWrittenInMergeInShuffle + bytesWrittenInMergeInSort);
	}

	/**
	 * Calculate the reduce counters related to the reduce phase for a virtual
	 * profile based on the source profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics have been set
	 * 
	 */
	private void calcVirtualReduceCountersReducePhase() {

		// Get the reduce input counters
		double redInputSize = virtualProf
				.getCounter(MRCounter.REDUCE_INPUT_BYTES);
		double redInputPairs = virtualProf
				.getCounter(MRCounter.REDUCE_INPUT_RECORDS);

		// Calculate the reduce output counters
		double redOutSize = redInputSize
				* virtualProf.getStatistic(MRStatistics.REDUCE_SIZE_SEL,
						DEF_SEL_ONE);
		double redOutPairs = redInputPairs
				* virtualProf.getStatistic(MRStatistics.REDUCE_PAIRS_SEL,
						DEF_SEL_ONE);

		virtualProf
				.addCounter(MRCounter.REDUCE_OUTPUT_BYTES, (long) redOutSize);
		virtualProf.addCounter(MRCounter.REDUCE_OUTPUT_RECORDS,
				(long) redOutPairs);

		// Set reduce input groups
		virtualProf.addCounter(MRCounter.REDUCE_INPUT_GROUPS,
				(long) (redInputPairs / virtualProf.getStatistic(
						MRStatistics.REDUCE_PAIRS_PER_GROUP,
						DEF_RED_PAIRS_PER_GROUP)));

		// Calculate and set the output size
		if (useOutputCompr)
			redOutSize *= virtualProf
					.getStatistic(MRStatistics.OUT_COMPRESS_RATIO);
		virtualProf.addCounter(MRCounter.HDFS_BYTES_WRITTEN, (long) redOutSize);
	}

	/**
	 * Calculate the reduce costs for a virtual profile based on the source
	 * profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics and counters have been set
	 * 
	 */
	private void calcVirtualReduceCosts() {

		// Set the statistics common between mappers and reducers
		calcVirtualTaskCosts(sourceProf, virtualProf, conf);

		// Set output compression cost
		if (useOutputCompr) {
			virtualProf.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
					sourceProf.getCostFactor(
							MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
							DEF_COST_CPU_COMPRESS));
		}
	}

	/**
	 * Calculate the reduce timings for a virtual profile based on the source
	 * profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics, counters, and costs have been
	 * set
	 * 
	 */
	private void calcVirtualReduceTimings() {

		// Calculate and set SHUFFLE, SORT
		calcVirtualReduceTimingsShuffleSortPhase();

		// Calculate and set SETUP, REDUCE, WRITE, CLEANUP
		calcVirtualReduceTimingsReducePhase();
	}

	/**
	 * Calculate the reduce timings related to the shuffle and sort phases for a
	 * virtual profile based on the source profile and the suggested
	 * configuration.
	 * 
	 * Assumptions: All virtualProf statistics, counters, and costs have been
	 * set
	 * 
	 */
	private void calcVirtualReduceTimingsShuffleSortPhase() {

		// Calculate and set SHUFFLE (network copy, uncompress, merge)
		double shuffleBytes = virtualProf
				.getCounter(MRCounter.REDUCE_SHUFFLE_BYTES);
		double netCost = shuffleBytes
				* virtualProf.getCostFactor(MRCostFactors.NETWORK_COST);

		double shuffleCPU = 0d;
		if (useIntermCompr)
			shuffleCPU = shuffleBytes
					* virtualProf
							.getCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST);

		double mergeTime = calcVirtualMergeTimings(mergedRecordsInShuffle,
				bytesReadInMergeInShuffle, bytesWrittenInMergeInShuffle);

		double combineCPU = 0d;
		if (useCombiner) {
			combineCPU = virtualProf
					.getCounter(MRCounter.COMBINE_INPUT_RECORDS)
					* virtualProf.getCostFactor(MRCostFactors.COMBINE_CPU_COST);
		}

		virtualProf.addTiming(MRTaskPhase.SHUFFLE, (netCost + shuffleCPU
				+ mergeTime + combineCPU)
				/ NS_PER_MS);

		// Calculate and set SORT (merge)
		mergeTime = calcVirtualMergeTimings(mergedRecordsInSort,
				bytesReadInMergeInSort, bytesWrittenInMergeInSort);
		virtualProf.addTiming(MRTaskPhase.SORT, mergeTime / NS_PER_MS);
	}

	/**
	 * Calculate the reduce timings related to the reduce phase for a virtual
	 * profile based on the source profile and the suggested configuration.
	 * 
	 * Assumptions: All virtualProf statistics, counters, and costs have been
	 * set
	 * 
	 */
	private void calcVirtualReduceTimingsReducePhase() {

		// Calculate and set SETUP
		virtualProf.addTiming(MRTaskPhase.SETUP, sourceProf.getTiming(
				MRTaskPhase.SETUP, 0d));

		// Calculate and set REDUCE
		double readIO = bytesReadInReduce
				* virtualProf.getCostFactor(MRCostFactors.READ_LOCAL_IO_COST);
		double reduceCPU = virtualProf
				.getCounter(MRCounter.REDUCE_INPUT_RECORDS)
				* virtualProf.getCostFactor(MRCostFactors.REDUCE_CPU_COST);
		virtualProf.addTiming(MRTaskPhase.REDUCE, (readIO + reduceCPU)
				/ NS_PER_MS);

		// Calculate and set WRITE
		double writeIO = virtualProf.getCounter(MRCounter.HDFS_BYTES_WRITTEN)
				* virtualProf.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST);

		double comprCPU = 0d;
		if (useOutputCompr)
			comprCPU = virtualProf.getCounter(MRCounter.REDUCE_OUTPUT_BYTES)
					* virtualProf
							.getCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST);
		virtualProf.addTiming(MRTaskPhase.WRITE, (writeIO + comprCPU)
				/ NS_PER_MS);

		// Calculate and set CLEANUP
		virtualProf.addTiming(MRTaskPhase.CLEANUP, sourceProf.getTiming(
				MRTaskPhase.CLEANUP, 0d));
	}

	/**
	 * Calculate the timings for merging. Merging can occur during the SHUFFLE
	 * phase or the SORT phase
	 * 
	 * @param mergedRecords
	 *            the number of records
	 * @param bytesRead
	 *            the bytes read during merging
	 * @param bytesWritten
	 *            the bytes written during merging
	 * @return the total time of merging
	 */
	private double calcVirtualMergeTimings(double mergedRecords,
			long bytesRead, long bytesWritten) {

		// Calculate the IO costs for MERGE
		double readIO = bytesRead
				* virtualProf.getCostFactor(MRCostFactors.READ_LOCAL_IO_COST);
		double writeIO = bytesWritten
				* virtualProf.getCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST);

		// Calculate the CPU costs for SORT
		double uncomprCPU = 0d;
		double comprCPU = 0d;
		if (useIntermCompr) {
			uncomprCPU = bytesRead
					* virtualProf
							.getCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST);
			comprCPU = bytesWritten
					* virtualProf
							.getCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST)
					/ virtualProf
							.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO);
		}

		double mergeCPU = mergedRecords
				* virtualProf.getCostFactor(MRCostFactors.MERGE_CPU_COST);

		return readIO + writeIO + uncomprCPU + comprCPU + mergeCPU;
	}

}
