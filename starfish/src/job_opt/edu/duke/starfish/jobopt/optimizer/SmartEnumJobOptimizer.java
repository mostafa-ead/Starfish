package edu.duke.starfish.jobopt.optimizer;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.jobopt.params.HadoopParameter;
import edu.duke.starfish.jobopt.params.IntegerParamDescriptor;
import edu.duke.starfish.jobopt.space.ParamSpaceUtils;
import edu.duke.starfish.jobopt.space.ParameterSpace;
import edu.duke.starfish.jobopt.space.ParameterSpacePoint;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.whatif.WhatIfEngine;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.IWhatIfScheduler;

/**
 * A Job optimizer that enumerates the parameter space using domain knowledge in
 * order to find the best configuration parameter settings.
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
public class SmartEnumJobOptimizer extends FullEnumJobOptimizer {

	private static final Log LOG = LogFactory
			.getLog(SmartEnumJobOptimizer.class);

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
	public SmartEnumJobOptimizer(JobProfileOracle jobOracle,
			DataSetModel dataModel, ClusterConfiguration cluster,
			IWhatIfScheduler scheduler) {
		super(jobOracle, dataModel, cluster, scheduler);
	}

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
		MRJobProfile virtualProf = jobOracle.whatif(conf, dataModel);

		// Initialize the parameter space for the map tasks
		ParameterSpace mapSpace = ParamSpaceUtils.getParamSpaceForMappers(conf);
		ParamSpaceUtils.adjustParameterDescriptors(mapSpace, cluster, conf,
				virtualProf);

		// Generate the grid of points
		boolean useRandom = conf.getBoolean(USE_RANDOM_VALUES, false);
		int numValuesPerParam = conf.getInt(NUM_VALUES_PER_PARAM, 2);
		List<ParameterSpacePoint> mapPoints = mapSpace.getSpacePointGrid(
				useRandom, numValuesPerParam);
		LOG.debug("Number of parameters: " + mapSpace.getNumParameters());
		LOG.debug("Number of settings: " + mapPoints.size());

		// Find the best point for the map tasks
		jobOracle.setIgnoreReducers(true);
		scheduler.setIgnoreReducers(true);
		ParameterSpacePoint optMapPoint = findBestParameterSpacePoint(
				whatifEngine, mapPoints, conf);
		optMapPoint.populateConfiguration(conf);

		// Initialize the parameter space for the reduce tasks
		ParameterSpace redSpace = ParamSpaceUtils
				.getParamSpaceForReducers(conf);
		ParamSpaceUtils.adjustParameterDescriptors(redSpace, cluster, conf,
				virtualProf);

		if (redSpace.containsParamDescriptor(HadoopParameter.RED_TASKS)) {
			IntegerParamDescriptor descr = (IntegerParamDescriptor) redSpace
					.getParameterDescriptor(HadoopParameter.RED_TASKS);
			LOG.debug("Reduce Task Domain: [" + descr.getMinValue() + ", "
					+ descr.getMaxValue() + "]");
		}

		// Generate the new grid of points
		List<ParameterSpacePoint> redPoints = redSpace.getSpacePointGrid(
				useRandom, numValuesPerParam);
		LOG.debug("Number of parameters: " + redSpace.getNumParameters());
		LOG.debug("Number of settings: " + redPoints.size());

		// Find the best point for the entire job
		jobOracle.setIgnoreReducers(false);
		scheduler.setIgnoreReducers(false);
		ParameterSpacePoint optRedPoint = findBestParameterSpacePoint(
				whatifEngine, redPoints, conf);

		// Create the best MR job profile
		optMapPoint.populateConfiguration(conf);
		optRedPoint.populateConfiguration(conf);
		bestMRJobProfile = jobOracle.whatif(conf, dataModel);
		bestRunningTime = whatifEngine.whatIfJobConfGetTime(conf);

		// Return the best configuration
		if (!fullConf) {
			conf = new Configuration(false);
			optMapPoint.populateConfiguration(conf);
			optRedPoint.populateConfiguration(conf);
		}
		bestConf = conf;
		return conf;
	}

}
