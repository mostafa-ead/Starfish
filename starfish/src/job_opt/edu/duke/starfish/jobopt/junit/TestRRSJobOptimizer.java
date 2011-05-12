package edu.duke.starfish.jobopt.junit;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import edu.duke.starfish.jobopt.optimizer.FullEnumJobOptimizer;
import edu.duke.starfish.jobopt.optimizer.RRSJobOptimizer;
import edu.duke.starfish.jobopt.params.ParameterDescriptor;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.utils.Constants;
import edu.duke.starfish.whatif.junit.SampleDataSetModel;
import edu.duke.starfish.whatif.junit.SampleProfiles;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.BasicFIFOScheduler;
import edu.duke.starfish.whatif.scheduler.IWhatIfScheduler;

/**
 * Test the FullEnumJobOptimizer
 * 
 * @author hero
 */
public class TestRRSJobOptimizer extends TestCase {

	/**
	 * Test method for
	 * {@link edu.duke.starfish.jobopt.optimizer.FullEnumJobOptimizer#findBestConfiguration(Configuration)}
	 */
	@Test
	public void testWhatIfJobConfGetTime() {
		// Common elements
		ClusterConfiguration cluster = SampleProfiles.getClusterConfiguration();
		SampleDataSetModel model = new SampleDataSetModel();

		// TeraSort ---------------------------------------------------
		MRJobProfile tsJobProf = SampleProfiles.getTeraSortJobProfile();
		Configuration tsConf = SampleProfiles.getTeraSortConfiguration();
		JobProfileOracle tsOracle = new JobProfileOracle(tsJobProf);
		IWhatIfScheduler tsScheduler = new BasicFIFOScheduler();

		// Set the input specs
		tsConf.setInt(SampleDataSetModel.NUM_MAPPERS, 5);
		tsConf.setLong(SampleDataSetModel.INPUT_SIZE, 20000000l);
		tsConf.setBoolean(SampleDataSetModel.INPUT_COMPR, false);
		tsConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, true);

		// Find the best configuration
		tsConf.setBoolean(FullEnumJobOptimizer.USE_RANDOM_VALUES, false);
		tsConf.setInt(FullEnumJobOptimizer.NUM_VALUES_PER_PARAM, 2);

		ParameterDescriptor.setRandomSeed(23);
		RRSJobOptimizer tsOptimizer = new RRSJobOptimizer(tsOracle, model,
				cluster, tsScheduler);
		Configuration tsBestConf = tsOptimizer.findBestConfiguration(tsConf);

		assertNotNull(tsBestConf.get(Constants.MR_SORT_MB));
		assertNotNull(tsBestConf.get(Constants.MR_SPILL_PERC));
		assertNotNull(tsBestConf.get(Constants.MR_SORT_REC_PERC));
		assertNotNull(tsBestConf.get(Constants.MR_SORT_FACTOR));
		assertNotNull(tsBestConf.get(Constants.MR_RED_TASKS));
		assertNotNull(tsBestConf.get(Constants.MR_INMEM_MERGE));
		assertNotNull(tsBestConf.get(Constants.MR_SHUFFLE_IN_BUFF_PERC));
		assertNotNull(tsBestConf.get(Constants.MR_SHUFFLE_MERGE_PERC));
		assertNotNull(tsBestConf.getFloat(Constants.MR_RED_IN_BUFF_PERC, 0));

		// WordCount ---------------------------------------------------
		MRJobProfile wcJobProf = SampleProfiles.getWordCountJobProfile();
		Configuration wcConf = SampleProfiles.getWordCountConfiguration();
		JobProfileOracle wcOracle = new JobProfileOracle(wcJobProf);
		IWhatIfScheduler wcScheduler = new BasicFIFOScheduler();

		// Set the input specs
		wcConf.setInt(SampleDataSetModel.NUM_MAPPERS, 15);
		wcConf.setLong(SampleDataSetModel.INPUT_SIZE, 21252750l);
		wcConf.setBoolean(SampleDataSetModel.INPUT_COMPR, false);
		wcConf.getBoolean(SampleDataSetModel.USE_AVG_PROFILE, true);

		// Find the best configuration
		wcConf.setBoolean(FullEnumJobOptimizer.USE_RANDOM_VALUES, false);
		wcConf.setInt(FullEnumJobOptimizer.NUM_VALUES_PER_PARAM, 2);

		ParameterDescriptor.setRandomSeed(23);
		RRSJobOptimizer wcOptimizer = new RRSJobOptimizer(wcOracle, model,
				cluster, wcScheduler);
		Configuration wcBestConf = wcOptimizer.findBestConfiguration(wcConf);

		assertNotNull(wcBestConf.get(Constants.MR_SORT_MB));
		assertNotNull(wcBestConf.get(Constants.MR_SPILL_PERC));
		assertNotNull(wcBestConf.get(Constants.MR_SORT_REC_PERC));
		assertNotNull(wcBestConf.get(Constants.MR_SORT_FACTOR));
		assertNotNull(wcBestConf.get(Constants.MR_RED_TASKS));
		assertNotNull(wcBestConf.get(Constants.MR_INMEM_MERGE));
		assertNotNull(wcBestConf.get(Constants.MR_SHUFFLE_IN_BUFF_PERC));
		assertNotNull(wcBestConf.get(Constants.MR_SHUFFLE_MERGE_PERC));
		assertNotNull(wcBestConf.get(Constants.MR_RED_IN_BUFF_PERC));

	}

}
