package edu.duke.starfish.whatif.junit;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.oracle.MapProfileOracle;

/**
 * JUnit testing for the MapProfileOracle class
 * 
 * @author hero
 * 
 */
public class TestMapProfileOracle extends TestCase {

	/**
	 * Test method for
	 * {@link edu.duke.starfish.whatif.oracle.MapProfileOracle#whatif(Configuration, MapInputSpecs)}
	 * .
	 */
	@Test
	public void testWhatif() {
		// Terasort ---------------------------------------------------
		MRMapProfile tsMapProf = SampleProfiles.getTeraSortMapProfile();
		Configuration tsConf = SampleProfiles.getTeraSortConfiguration();
		MapProfileOracle tsOracle = new MapProfileOracle(tsMapProf);

		// Set the input specs
		MapInputSpecs tsInput = new MapInputSpecs(0, 5, 20000000l, false,
				DataLocality.DATA_LOCAL);

		// Generate the virtual profile
		MRMapProfile tsVirtual = tsOracle.whatif(tsConf, tsInput);
		assertNotNull(tsVirtual);

		// Wordcount ---------------------------------------------------
		MRMapProfile wcMapProf = SampleProfiles.getWordCountMapProfile();
		Configuration wcConf = SampleProfiles.getWordCountConfiguration();
		MapProfileOracle wcOracle = new MapProfileOracle(wcMapProf);

		// Set the input specs
		MapInputSpecs wcInput = new MapInputSpecs(0, 15, 21252750l, false,
				DataLocality.DATA_LOCAL);

		// Generate the virtual profile
		MRMapProfile wcVirtual = wcOracle.whatif(wcConf, wcInput);
		assertNotNull(wcVirtual);
	}

}
