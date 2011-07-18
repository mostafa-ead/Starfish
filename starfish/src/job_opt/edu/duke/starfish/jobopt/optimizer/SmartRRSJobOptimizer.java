package edu.duke.starfish.jobopt.optimizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.jobopt.params.HadoopParameter;
import edu.duke.starfish.jobopt.params.IntegerParamDescriptor;
import edu.duke.starfish.jobopt.rrs.RecursiveRandomSearch;
import edu.duke.starfish.jobopt.space.ParamSpaceUtils;
import edu.duke.starfish.jobopt.space.ParameterSpace;
import edu.duke.starfish.jobopt.space.ParameterSpacePoint;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
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
	 * @param scheduler
	 *            the scheduler
	 * @param cluster
	 *            the cluster setup
	 * @param conf
	 *            the current configuration settings
	 */
	public SmartRRSJobOptimizer(JobProfileOracle jobOracle,
			DataSetModel dataModel, IWhatIfScheduler scheduler,
			ClusterConfiguration cluster, Configuration conf) {
		super(jobOracle, dataModel, scheduler, cluster, conf);
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see edu.duke.starfish.jobopt.optimizer.JobOptimizer#optimizeInternal()
	 */
	@Override
	protected ParameterSpacePoint optimizeInternal() {

		// Initialize the map parameter space
		MRJobProfile virtualProf = jobOracle.whatif(currConf, dataModel);
		ParameterSpace space = ParamSpaceUtils
				.getParamSpaceForMappers(currConf);
		ParamSpaceUtils.adjustParameterDescriptors(space, cluster, currConf,
				virtualProf);

		// Perform RRS to find the best point in the map space
		jobOracle.setIgnoreReducers(true);
		scheduler.setIgnoreReducers(true);
		RecursiveRandomSearch<ParameterSpacePoint> rrs = 
			new RecursiveRandomSearch<ParameterSpacePoint>(currConf);
		ParameterSpacePoint optMapPoint = rrs.findBestSpacePoint(space, this);
		optMapPoint.populateConfiguration(currConf);

		// Initialize the reduce parameter space
		space = ParamSpaceUtils.getParamSpaceForReducers(currConf);
		ParamSpaceUtils.adjustParameterDescriptors(space, cluster, currConf,
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
		ParameterSpacePoint optRedPoint = rrs.findBestSpacePoint(space, this);

		// Add the best reduce param values and return
		optMapPoint.addParamValues(optRedPoint);
		return optMapPoint;
	}
}
