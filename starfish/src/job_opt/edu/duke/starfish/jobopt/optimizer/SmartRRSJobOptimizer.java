package edu.duke.starfish.jobopt.optimizer;

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
 * A Job optimizer that uses Recursive Random Search (RRS) and domain knowledge
 * in order to find the best configuration parameter settings.
 * 
 * For more information on RRS see RRSJobOptimizer.java.
 * 
 * @author hero
 */
public class SmartRRSJobOptimizer extends RRSJobOptimizer {

	private static final Log LOG = LogFactory
			.getLog(SmartRRSJobOptimizer.class);

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
	public SmartRRSJobOptimizer(JobProfileOracle jobOracle,
			DataSetModel dataModel, ClusterConfiguration cluster,
			IWhatIfScheduler scheduler) {
		super(jobOracle, dataModel, cluster, scheduler);
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see edu.duke.starfish.jobopt.optimizer.JobOptimizer#findBestConfiguration(Configuration)
	 */
	@Override
	public Configuration findBestConfiguration(Configuration jobConf,
			boolean fullConf) {

		// Initializations
		Configuration conf = new Configuration(jobConf);
		WhatIfEngine whatifEngine = new WhatIfEngine(jobOracle, dataModel,
				scheduler, cluster, conf);
		MRJobProfile virtualProf = jobOracle.whatif(conf, dataModel);

		// Initialize the map parameter space
		ParameterSpace space = ParamSpaceUtils.getParamSpaceForMappers(conf);
		ParamSpaceUtils.adjustParameterDescriptors(space, cluster, conf,
				virtualProf);

		// Perform RRS to find the best point in the map space
		jobOracle.setIgnoreReducers(true);
		scheduler.setIgnoreReducers(true);
		ParameterSpacePoint optMapPoint = performRecursiveRandomSearch(space,
				whatifEngine, conf);
		optMapPoint.populateConfiguration(conf);

		// Initialize the reduce parameter space
		space = ParamSpaceUtils.getParamSpaceForReducers(conf);
		ParamSpaceUtils.adjustParameterDescriptors(space, cluster, conf,
				virtualProf);

		if (space.containsParamDescriptor(HadoopParameter.RED_TASKS)) {
			IntegerParamDescriptor descr = (IntegerParamDescriptor) space
					.getParameterDescriptor(HadoopParameter.RED_TASKS);
			LOG.debug("Reduce Task Domain: [" + descr.getMinValue() + ", "
					+ descr.getMaxValue() + "]");
		}

		// Perform RRS to find the best point in the reduce space
		jobOracle.setIgnoreReducers(false);
		scheduler.setIgnoreReducers(false);
		ParameterSpacePoint optRedPoint = performRecursiveRandomSearch(space,
				whatifEngine, conf);

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
