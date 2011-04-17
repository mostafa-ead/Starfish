package edu.duke.starfish.whatif.junit;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.whatif.data.ReduceShuffleSpecs;
import edu.duke.starfish.whatif.oracle.ReduceProfileOracle;

/**
 * JUnit testing for the ReduceProfileOracle class
 * 
 * @author hero
 * 
 */
public class TestReduceProfileOracle extends TestCase {
	/**
	 * Test method for
	 * {@link edu.duke.starfish.whatif.oracle.ReduceProfileOracle#whatif(Configuration, ReduceShuffleSpecs)}
	 * .
	 */
	@Test
	public void testWhatif() {
		// Terasort ---------------------------------------------------
		MRReduceProfile tsRedProf = SampleProfiles.getTeraSortReduceProfile();
		Configuration tsConf = SampleProfiles.getTeraSortConfiguration();
		ReduceProfileOracle tsOracle = new ReduceProfileOracle(tsRedProf);

		// Set the shuffle specs
		ReduceShuffleSpecs tsShuffle = new ReduceShuffleSpecs(5, 1, 14725775l,
				1000000l);

		// Generate the virtual profile
		MRReduceProfile tsVirtual = tsOracle.whatif(tsConf, tsShuffle);
		assertNotNull(tsVirtual);

		// Wordcount ---------------------------------------------------
		MRReduceProfile wcRedProf = SampleProfiles.getWordCountReduceProfile();
		Configuration wcConf = SampleProfiles.getWordCountConfiguration();
		ReduceProfileOracle wcOracle = new ReduceProfileOracle(wcRedProf);

		// Set the shuffle specs
		ReduceShuffleSpecs wcShuffle = new ReduceShuffleSpecs(15, 1, 424171l,
				120000l);

		// Generate the virtual profile
		MRReduceProfile wcVirtual = wcOracle.whatif(wcConf, wcShuffle);
		assertNotNull(wcVirtual);
	}
}
