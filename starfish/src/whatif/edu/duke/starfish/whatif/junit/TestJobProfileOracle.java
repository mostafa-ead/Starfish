package edu.duke.starfish.whatif.junit;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;

public class TestJobProfileOracle extends TestCase {

	/**
	 * Test method for
	 * {@link edu.duke.starfish.whatif.oracle.JobProfileOracle#whatif(Configuration, edu.duke.starfish.whatif.data.DataSetModel)}
	 */
	@Test
	public void testWhatif() {
		// TeraSort ---------------------------------------------------
		MRJobProfile tsJobProf = SampleProfiles.getTeraSortJobProfile();
		Configuration tsConf = SampleProfiles.getTeraSortConfiguration();
		SampleDataSetModel model = new SampleDataSetModel();

		// Set the input specs
		tsConf.setInt(SampleDataSetModel.NUM_MAPPERS, 5);
		tsConf.setLong(SampleDataSetModel.INPUT_SIZE, 20000000l);
		tsConf.setBoolean(SampleDataSetModel.INPUT_COMPR, false);
		tsConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, true);

		// Generate the virtual profile
		JobProfileOracle tsOracle = new JobProfileOracle(tsJobProf);
		MRJobProfile tsVirtual = tsOracle.whatif(tsConf, model);
		assertNotNull(tsVirtual);

		// Generate the virtual profile
		tsConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, false);
		JobProfileOracle tsOracle2 = new JobProfileOracle(tsJobProf);
		MRJobProfile tsVirtual2 = tsOracle2.whatif(tsConf, model);
		assertNotNull(tsVirtual2);
		assertTrue(tsVirtual.equals(tsVirtual2));

		// WordCount ---------------------------------------------------
		MRJobProfile wcJobProf = SampleProfiles.getWordCountJobProfile();
		Configuration wcConf = SampleProfiles.getWordCountConfiguration();

		// Set the input specs
		wcConf.setInt(SampleDataSetModel.NUM_MAPPERS, 15);
		wcConf.setLong(SampleDataSetModel.INPUT_SIZE, 21252750l);
		wcConf.setBoolean(SampleDataSetModel.INPUT_COMPR, false);
		wcConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, true);

		// Generate the virtual profile
		JobProfileOracle wcOracle = new JobProfileOracle(wcJobProf);
		MRJobProfile wcVirtual = wcOracle.whatif(wcConf, model);
		assertNotNull(wcVirtual);

		// Generate the virtual profile
		wcConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, false);
		JobProfileOracle wcOracle2 = new JobProfileOracle(wcJobProf);
		MRJobProfile wcVirtual2 = wcOracle2.whatif(wcConf, model);
		assertNotNull(wcVirtual2);
		assertTrue(wcVirtual.equals(wcVirtual2));
	}

}
