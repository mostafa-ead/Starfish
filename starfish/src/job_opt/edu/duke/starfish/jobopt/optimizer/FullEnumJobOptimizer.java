package edu.duke.starfish.jobopt.optimizer;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.jobopt.space.ParamSpaceUtils;
import edu.duke.starfish.jobopt.space.ParameterSpace;
import edu.duke.starfish.jobopt.space.ParameterSpacePoint;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.whatif.WhatIfEngine;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.IWhatIfScheduler;

/**
 * A Job optimizer that enumerates the full parameter space in order to find the
 * best configuration parameter settings.
 * 
 * The user has two (optional) knobs to set in the conf:
 * <ol>
 * <li>starfish.job.optimizer.num.values.per.param - An integer number declaring
 * how many values to draw from each parameter range.</li>
 * <li>starfish.job.optimizer.use.random.values - A boolean flags to specify how
 * the values are selected from each parameter range. If set to 'true', then the
 * values are drawn randomly. If set to 'false', then the values are
 * equi-spaced.</li>
 * </ol>
 * 
 * @author hero
 */
public class FullEnumJobOptimizer extends JobOptimizer {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Constants
	public static final String USE_RANDOM_VALUES = "starfish.job.optimizer.use.random.values";
	public static final String NUM_VALUES_PER_PARAM = "starfish.job.optimizer.num.values.per.param";

	private static final Log LOG = LogFactory
			.getLog(FullEnumJobOptimizer.class);

	/**
	 * Constructor
	 * 
	 * @param jobOracle
	 *            the job profile oracle
	 * @param dataModel
	 *            the data set model
	 * @param cluster
	 *            the cluster setup
	 * @param scheduler
	 *            the scheduler
	 */
	public FullEnumJobOptimizer(JobProfileOracle jobOracle,
			DataSetModel dataModel, ClusterConfiguration cluster,
			IWhatIfScheduler scheduler) {
		super(jobOracle, dataModel, cluster, scheduler);
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @see edu.duke.starfish.jobopt.optimizer.JobOptimizer#findBestConfiguration(Configuration)
	 */
	@Override
	public Configuration findBestConfiguration(Configuration jobConf,
			boolean fullConf) {

		// Initialize the What-if Engine
		Configuration conf = new Configuration(jobConf);
		WhatIfEngine whatifEngine = new WhatIfEngine(jobOracle, dataModel,
				scheduler, cluster, conf);

		// Initialize the parameter space
		boolean useRandom = conf.getBoolean(USE_RANDOM_VALUES, false);
		int numValuesPerParam = conf.getInt(NUM_VALUES_PER_PARAM, 2);
		ParameterSpace space = ParamSpaceUtils.getFullParamSpace(conf);
		List<ParameterSpacePoint> points = space.getSpacePointGrid(useRandom,
				numValuesPerParam);

		// Log some stats
		LOG.debug("Number of parameters: " + space.getNumParameters());
		LOG.debug("Number of settings: " + points.size());

		// Find the best configuration
		ParameterSpacePoint bestPoint = findBestParameterSpacePoint(
				whatifEngine, points, conf);

		// Create the best MR job profile
		bestPoint.populateConfiguration(conf);
		bestMRJobProfile = jobOracle.whatif(conf, dataModel);
		bestRunningTime = whatifEngine.whatIfJobConfGetTime(conf);

		// Return the best configuration
		if (!fullConf) {
			conf = new Configuration(false);
			bestPoint.populateConfiguration(conf);
		}
		bestConf = conf;
		return conf;
	}

}
