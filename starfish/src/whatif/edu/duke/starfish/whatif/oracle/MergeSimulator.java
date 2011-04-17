package edu.duke.starfish.whatif.oracle;

import java.util.PriorityQueue;

/**
 * A simulator to perform merging of disk segments based on how Hadoop performs
 * merging.
 * 
 * @author hero
 */
public class MergeSimulator {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Simulation setup
	private PriorityQueue<Segment> segments;
	private long totalInputRecords;

	// Use of combiner for final merging phase
	private boolean useCombiner;
	private int numSpillsForCombine;
	private double combineSizeSel;
	private double combineRecSel;
	private long minNumUniqueValues;
	private long minSizeUniqueValues;

	// Memory segments
	private long numMemSegments;
	private long memSegmentSize;
	private long memSegmentRecs;

	// Simulation results
	private long numMergePasses;
	private long bytesRead;
	private long bytesWritten;
	private long spilledRecords;
	private long mergedRecords;
	private long combineInRecs;
	private long combineOutRecs;

	/**
	 * Default constructor
	 */
	public MergeSimulator() {
		segments = new PriorityQueue<Segment>();
		useCombiner = false;
		numMemSegments = 0;
		totalInputRecords = 0l;
	}

	/* ***************************************************************
	 * GETTER METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the numMergePasses
	 */
	public long getNumMergePasses() {
		return numMergePasses;
	}

	/**
	 * @return the bytesRead
	 */
	public long getBytesRead() {
		return bytesRead;
	}

	/**
	 * @return the bytesWritten
	 */
	public long getBytesWritten() {
		return bytesWritten;
	}

	/**
	 * @return the mergedRecords
	 */
	public long getMergedRecords() {
		return mergedRecords;
	}

	/**
	 * @return the spilledRecords
	 */
	public long getSpilledRecords() {
		return spilledRecords;
	}

	/**
	 * @return the combineInRecs
	 */
	public long getCombineInRecs() {
		return combineInRecs;
	}

	/**
	 * @return the combineOutRecs
	 */
	public long getCombineOutRecs() {
		return combineOutRecs;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add 'count' number of memory segments for merging of size 'size' bytes
	 * and 'record' number of records. Memory segments are always merged during
	 * the first merge pass of the other segments.
	 * 
	 * @param count
	 *            the number of segments to add
	 * @param size
	 *            the size of each segment
	 * @param records
	 *            the number of records of each segment
	 */
	public void addMemSegments(long count, long size, long records) {
		numMemSegments = count;
		memSegmentSize = size;
		memSegmentRecs = records;

		totalInputRecords += count * records;
	}

	/**
	 * Add 'count' number of segments for merging of size 'size' bytes and
	 * 'record' number of records.
	 * 
	 * @param count
	 *            the number of segments to add
	 * @param size
	 *            the size of each segment
	 * @param records
	 *            the number of records of each segment
	 */
	public void addSegments(long count, long size, long records) {
		for (int i = 0; i < count; ++i) {
			segments.add(new Segment(size, records));
		}
		totalInputRecords += count * records;
		minNumUniqueValues = Math.max(minNumUniqueValues, records);
		minSizeUniqueValues = Math.max(minSizeUniqueValues, size);
	}

	/**
	 * Enable the use of the combiner in the final merge round
	 * 
	 * @param numSpillsForCombine
	 *            the setting of "min.num.spills.for.combine"
	 * @param combineSizeSel
	 *            the size selectivity of the combiner
	 * @param combineRecSel
	 *            the record selectivity of the combiner
	 */
	public void enableCombiner(int numSpillsForCombine, double combineSizeSel,
			double combineRecSel) {
		this.useCombiner = true;
		this.numSpillsForCombine = numSpillsForCombine;
		this.combineSizeSel = combineSizeSel;
		this.combineRecSel = combineRecSel;

	}

	/**
	 * Simulates the merging process based on the provided sort factor and
	 * populates all the simulation counters
	 * 
	 * @param sortFactor
	 *            the sort factor
	 */
	public void simulateMerge(int sortFactor) {
		simulateMerge(sortFactor, false);
	}

	/**
	 * Simulates the merging process based on the provided sort factor and
	 * populates all the simulation counters
	 * 
	 * @param sortFactor
	 *            the sort factor
	 * @param skipFinalMerge
	 *            whether to skip the final merge or not
	 */
	public void simulateMerge(int sortFactor, boolean skipFinalMerge) {
		initialize();

		// Check if there is anything to merge
		if (segments.size() <= 1)
			return;

		int passNo = 1;
		while (segments.size() > sortFactor) {
			// Perform intermediate merge
			Segment merged = mergeSegments(sortFactor, passNo);

			// We have read and written the same amount of data
			bytesRead += merged.size;
			bytesWritten += merged.size;
			spilledRecords += merged.records;

			// Treat memory segments in a special way
			if (passNo == 1 && numMemSegments > 0) {
				merged.size += numMemSegments * memSegmentSize;
				merged.records += numMemSegments * memSegmentRecs;
				bytesWritten += numMemSegments * memSegmentSize;
				spilledRecords += numMemSegments * memSegmentRecs;
			}

			++passNo;
		}

		// The number of merged records is equivalent to the input records plus
		// the records spilled so far
		mergedRecords = totalInputRecords + spilledRecords;

		// Check if there is anything left to merge
		if (segments.size() <= 1 || skipFinalMerge)
			return;

		// Perform the final merge
		int numLeft = segments.size();
		Segment merged = mergeSegments(sortFactor, passNo);

		if (useCombiner && numLeft >= numSpillsForCombine) {
			// Apply the combiner
			combineInRecs = merged.records;
			combineOutRecs = (long) Math.max(merged.records * combineRecSel
					/ Math.log(merged.records), minNumUniqueValues);
			bytesRead += merged.size;
			bytesWritten += Math.max(merged.size * combineSizeSel
					/ Math.log(merged.size), minSizeUniqueValues);
			spilledRecords += combineOutRecs;
		} else {
			// Regular merging
			bytesRead += merged.size;
			bytesWritten += merged.size;
			spilledRecords += merged.records;
		}
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Determine the number of segments to merge. Assuming more than factor
	 * spills, the first pass will attempt to bring the total number of
	 * (numSpills - 1) to be divisible by the (sortFactor - 1) to minimize the
	 * number of merges.
	 * 
	 * @param numSegments
	 *            the total number of segments
	 * @param sortFactor
	 *            the sort factor
	 * @param passNo
	 *            the pass number
	 * @return the number of spills to merge
	 */
	private int getNumSegmentsToMerge(int numSegments, int sortFactor,
			int passNo) {

		if (numSegments <= sortFactor)
			return numSegments;

		if (passNo > 1)
			return sortFactor;

		int mod = (numSegments - 1) % (sortFactor - 1);
		if (mod == 0)
			return sortFactor;

		return mod + 1;
	}

	/**
	 * Initializes the simulation results
	 */
	private void initialize() {
		numMergePasses = 0;
		bytesRead = 0;
		bytesWritten = 0;
		mergedRecords = 0;
		spilledRecords = 0;
		combineInRecs = 0;
		combineOutRecs = 0;
		minNumUniqueValues = 0;
		minSizeUniqueValues = 0;
	}

	/**
	 * Merge a number of segments into a single merge
	 * 
	 * @param sortFactor
	 *            sort factor
	 * @param passNo
	 *            the merge pass number
	 * @return the merged segment
	 */
	private Segment mergeSegments(int sortFactor, int passNo) {

		++numMergePasses;
		Segment merged = new Segment();
		Segment toMerge = null;

		// Merge 'numSegments' segments into one segment
		int numSegments = getNumSegmentsToMerge(segments.size(), sortFactor,
				passNo);

		for (int i = 0; i < numSegments; ++i) {
			toMerge = segments.poll();
			merged.size += toMerge.size;
			merged.records += toMerge.records;
		}

		segments.add(merged);
		return merged;
	}

	/* ***************************************************************
	 * PRIVATE CLASS
	 * ***************************************************************
	 */

	/**
	 * A simple class that represents a segment on disk
	 * 
	 * @author hero
	 */
	private class Segment implements Comparable<Segment> {
		private long size;
		private long records;

		/**
		 * Default constructor
		 */
		public Segment() {
			this(0, 0);
		}

		/**
		 * Constructor
		 * 
		 * @param size
		 * @param records
		 */
		public Segment(long size, long records) {
			this.size = size;
			this.records = records;
		}

		@Override
		public int compareTo(Segment arg0) {
			if (this.size > arg0.size)
				return 1;
			else if (this.size < arg0.size)
				return -1;
			else
				return 0;
		}

	}
}
