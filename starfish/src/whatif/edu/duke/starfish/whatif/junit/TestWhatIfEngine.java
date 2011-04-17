package edu.duke.starfish.whatif.junit;

import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.whatif.WhatIfEngine;
import edu.duke.starfish.whatif.WhatIfUtils;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.BasicFIFOScheduler;
import edu.duke.starfish.whatif.scheduler.BasicFIFOSchedulerForOptimizer;

public class TestWhatIfEngine extends TestCase {

	/**
	 * Test method for
	 * {@link edu.duke.starfish.whatif.WhatIfEngine#whatIfJobConfGetTime(Configuration)}
	 */
	@Test
	public void testWhatIfJobConfGetTime() {
		// Common elements
		ClusterConfiguration cluster = SampleProfiles.getClusterConfiguration();
		SampleDataSetModel model = new SampleDataSetModel();
		BasicFIFOSchedulerForOptimizer scheduler = new BasicFIFOSchedulerForOptimizer();

		// TeraSort ---------------------------------------------------
		MRJobProfile tsJobProf = SampleProfiles.getTeraSortJobProfile();
		Configuration tsConf = SampleProfiles.getTeraSortConfiguration();
		JobProfileOracle tsOracle = new JobProfileOracle(tsJobProf);

		// Set the input specs
		tsConf.setInt(SampleDataSetModel.NUM_MAPPERS, 5);
		tsConf.setLong(SampleDataSetModel.INPUT_SIZE, 20000000l);
		tsConf.setBoolean(SampleDataSetModel.INPUT_COMPR, false);
		tsConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, true);

		// Ask the what-if question
		WhatIfEngine tsWhatif = new WhatIfEngine(tsOracle, model, scheduler,
				cluster, tsConf);
		double tsExecTime = tsWhatif.whatIfJobConfGetTime(tsConf);
		assertEquals(141235.7295, tsExecTime, 0.001);

		// WordCount ---------------------------------------------------
		MRJobProfile wcJobProf = SampleProfiles.getWordCountJobProfile();
		Configuration wcConf = SampleProfiles.getWordCountConfiguration();
		JobProfileOracle wcOracle = new JobProfileOracle(wcJobProf);

		// Set the input specs
		wcConf.setInt(SampleDataSetModel.NUM_MAPPERS, 15);
		wcConf.setLong(SampleDataSetModel.INPUT_SIZE, 21252750l);
		wcConf.setBoolean(SampleDataSetModel.INPUT_COMPR, false);
		wcConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, true);

		// Ask the what-if question
		WhatIfEngine wcWhatif = new WhatIfEngine(wcOracle, model, scheduler,
				cluster, wcConf);
		double wcExecTime = wcWhatif.whatIfJobConfGetTime(wcConf);
		assertEquals(62274.1117, wcExecTime, 0.0001);

	}

	/**
	 * Test method for
	 * {@link edu.duke.starfish.whatif.WhatIfEngine#whatIfJobConfGetJobInfo(Configuration)}
	 */
	@Test
	public void testWhatIfJobConfGetJobInfo() {
		// Common elements
		ClusterConfiguration cluster = SampleProfiles.getClusterConfiguration();
		SampleDataSetModel model = new SampleDataSetModel();
		BasicFIFOScheduler scheduler = new BasicFIFOScheduler();

		// TeraSort ---------------------------------------------------
		MRJobProfile tsJobProf = SampleProfiles.getTeraSortJobProfile();
		Configuration tsConf = SampleProfiles.getTeraSortConfiguration();
		JobProfileOracle tsOracle = new JobProfileOracle(tsJobProf);

		// Set the input specs
		tsConf.setInt(SampleDataSetModel.NUM_MAPPERS, 5);
		tsConf.setLong(SampleDataSetModel.INPUT_SIZE, 20000000l);
		tsConf.setBoolean(SampleDataSetModel.INPUT_COMPR, false);
		tsConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, true);

		// Ask the what-if question
		WhatIfEngine tsWhatif = new WhatIfEngine(tsOracle, model, scheduler,
				cluster, tsConf);
		MRJobInfo tsJobInfo = tsWhatif.whatIfJobConfGetJobInfo(tsConf);
		assertEquals(142734.0, tsJobInfo.getDuration(), 0.000001);

		// Test generateMapInputSpecs()
		List<MapInputSpecs> tsSpecs = WhatIfUtils
				.generateMapInputSpecs(tsJobInfo);
		MapInputSpecs tsExpect = new MapInputSpecs(0, 5, 20000000l, false,
				DataLocality.DATA_LOCAL);
		assertEquals(1, tsSpecs.size());
		assertEquals(tsExpect, tsSpecs.get(0));

		// WordCount ---------------------------------------------------
		MRJobProfile wcJobProf = SampleProfiles.getWordCountJobProfile();
		Configuration wcConf = SampleProfiles.getWordCountConfiguration();
		JobProfileOracle wcOracle = new JobProfileOracle(wcJobProf);

		// Set the input specs
		wcConf.setInt(SampleDataSetModel.NUM_MAPPERS, 15);
		wcConf.setLong(SampleDataSetModel.INPUT_SIZE, 21252750l);
		wcConf.setBoolean(SampleDataSetModel.INPUT_COMPR, false);
		wcConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, true);

		// Ask the what-if question
		WhatIfEngine wcWhatif = new WhatIfEngine(wcOracle, model, scheduler,
				cluster, wcConf);
		MRJobInfo wcJobInfo = wcWhatif.whatIfJobConfGetJobInfo(wcConf);
		assertEquals(63771.0, wcJobInfo.getDuration(), 0.000001);

		// Test generateMapInputSpecs()
		List<MapInputSpecs> wcSpecs = WhatIfUtils
				.generateMapInputSpecs(wcJobInfo);
		MapInputSpecs wcExpect = new MapInputSpecs(0, 15, 21252750l, false,
				DataLocality.DATA_LOCAL);
		assertEquals(1, wcSpecs.size());
		assertEquals(wcExpect, wcSpecs.get(0));

	}

}
