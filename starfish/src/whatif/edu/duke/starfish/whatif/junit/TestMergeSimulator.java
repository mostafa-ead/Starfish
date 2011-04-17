package edu.duke.starfish.whatif.junit;

import junit.framework.TestCase;

import org.junit.Test;

import edu.duke.starfish.whatif.oracle.MergeSimulator;

/**
 * JUnit testing of MergeSimulator
 * 
 * @author hero
 */
public class TestMergeSimulator extends TestCase {

	/**
	 * Test method for
	 * {@link edu.duke.starfish.whatif.oracle.MergeSimulator#simulateMerge(int)}
	 * .
	 */
	@Test
	public void testSimulateMerge() {

		long size = 1024;
		long recs = 10;

		// For spills <= factor^2, we can calculate the outcome using formulas.
		// Hence, we compare the simulator against the expected result.
		for (int factor = 5; factor <= 20; factor += 5) {
			for (int spills = 2; spills <= factor * factor; ++spills) {
				MergeSimulator merger = new MergeSimulator();
				merger.addSegments(spills, size, recs);
				merger.simulateMerge(factor);

				long numInterm = getNumIntermSpillReads(spills, factor);

				assertEquals(getNumSpillMerges(spills, factor), merger
						.getNumMergePasses());
				assertEquals(numInterm * recs + spills * recs, merger
						.getSpilledRecords());
				assertEquals(numInterm * size + spills * size, merger
						.getBytesRead());
				assertEquals(numInterm * size + spills * size, merger
						.getBytesWritten());
			}
		}

		// Simulate for a case where spills > factor^2
		int spills = 26;
		int factor = 5;
		MergeSimulator merger = new MergeSimulator();
		merger.addSegments(spills, size, recs);
		merger.simulateMerge(factor);
		assertEquals(7, merger.getNumMergePasses());
		assertEquals(540, merger.getSpilledRecords());
		assertEquals(55296, merger.getBytesRead());
		assertEquals(55296, merger.getBytesWritten());

		// Simulate for a case with a combiner
		spills = 19;
		factor = 5;
		merger = new MergeSimulator();
		merger.addSegments(spills, size, recs);
		merger.enableCombiner(3, (long) (0.5 * Math.log(19 * 1024)),
				(long) (0.5 * Math.log(19 * 10)));
		merger.simulateMerge(factor);
		assertEquals(5, merger.getNumMergePasses());
		assertEquals(252, merger.getSpilledRecords());
		assertEquals(37888, merger.getBytesRead());
		assertEquals(26312, merger.getBytesWritten());
		assertEquals(190, merger.getCombineInRecs());
		assertEquals(72, merger.getCombineOutRecs());
	}

	private static long getNumIntermSpillReads(long numSpills, long sortFactor) {

		if (numSpills <= sortFactor)
			return 0;

		long firstMerge = getNumSpillsInFirstMerge(numSpills, sortFactor);
		return firstMerge + ((numSpills - firstMerge) / sortFactor)
				* sortFactor;
	}

	private static long getNumSpillMerges(long numSpills, long sortFactor) {
		if (numSpills == 1)
			return 0;
		else if (numSpills <= sortFactor)
			return 1;
		else
			return 2
					+ (numSpills - getNumSpillsInFirstMerge(numSpills,
							sortFactor)) / sortFactor;
	}

	private static long getNumSpillsInFirstMerge(long numSpills, long sortFactor) {
		if (numSpills <= sortFactor)
			return numSpills;

		long mod = (numSpills - 1) % (sortFactor - 1);
		if (mod == 0)
			return sortFactor;
		return mod + 1;
	}

}
