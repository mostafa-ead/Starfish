package edu.duke.starfish.jobopt.optimizer;

import java.util.ArrayList;
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
 * A Job optimizer that uses Recursive Random Search in order to find the best
 * configuration parameter settings.
 * 
 * Reference: A recursive random search algorithm for large-scale network
 * parameter configuration, Tao Ye and Shivkumar Kalyanaraman, Proceeding
 * SIGMETRICS '03, ACM
 * 
 * Parameters:
 * <ul>
 * <li>p = starfish.job.optimizer.explore.confidence.prob (0.99)</li>
 * <li>r = starfish.job.optimizer.explore.percentile (0.1)</li>
 * <li>q = starfish.job.optimizer.exploit.confidence.prob (0.99)</li>
 * <li>v = starfish.job.optimizer.exploit.expected.value (0.8)</li>
 * <li>c = starfish.job.optimizer.exploit.reduction.ratio (0.5)</li>
 * <li>st = starfish.job.optimizer.exploit.termination.size (0.001)</li>
 * </ul>
 * 
 * Pseudocode:
 * 
 * <pre>
 * Initialize exploration parameters p, r, n <- ln(1 − p)/ ln(1 − r) ;
 * Initialize exploitation parameters q, υ, c, st , l <- ln(1 − q)/ ln(1 − υ);
 * Take n random samples xi, i = 1 . . . n from parameter space D;
 * x0 <- arg min 1<=i<=n (f(xi)), yr <- f(x0), add f(x0) to the threshold set F;
 * i <- 0, exploit_flag <- 1, xopt <- x0 ;
 * while stopping criterion is not satisﬁed do
 *     if exploit_flag = 1 then
 *         // Exploit ﬂag is set, start exploitation process
 *         j <- 0, fc <- f(x0), xl <- x0 , ρ <- r;
 *         while ρ > st do
 *             Take a random sample x' from ND,ρ(xl);
 *             if f(x') < fc then
 *                 // Find a better point, re-align the center of sample space to the new point
 *                 xl <- x' , fc <- f(x');
 *                 j <- 0;
 *             else
 *                 j <- j + 1;
 *             endif
 *             if j = l then
 *                 // Fail to ﬁnd a better point, shrink the sample space
 *                 ρ <- c * ρ, j <- 0;
 *             endif
 *         endw
 *         exploit_flag <- 0, update xopt if f(xl) < f(xopt);
 *     endif
 *     Take a random sample x0 from D;
 *     if f(x0) < yr then
 *         // Find a promising point, set the ﬂag to exploit
 *         exploit_flag <- 1;
 *     endif
 *     if i = n then
 *         // Update the exploitation threshold every n samples in the parameter space
 *         Add min 1<=i<=n (f(xi)) to the threshold set F;
 *         yr <- mean(F), i <- 0;
 *     endif
 *     i <- i + 1;
 * endw
 * 
 * </pre>
 * 
 * @author hero
 */
public class RRSJobOptimizer extends JobOptimizer {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Constants
	public static final String RRS_EXPLORE_CONF_PROB = "starfish.job.optimizer.explore.confidence.prob";
	public static final String RRS_EXPLORE_PERC = "starfish.job.optimizer.explore.percentile";
	public static final String RRS_EXPLOIT_CONF_PROB = "starfish.job.optimizer.exploit.confidence.prob";
	public static final String RRS_EXPLOIT_EXP_VALUE = "starfish.job.optimizer.exploit.expected.value";
	public static final String RRS_EXPLOIT_RED_RATIO = "starfish.job.optimizer.exploit.reduction.ratio";
	public static final String RRS_EXPLOIT_TERM_SIZE = "starfish.job.optimizer.exploit.termination.size";

	// Default values
	public static final float RRS_DEF_EXPLORE_CONF_PROB = 0.99f;
	public static final float RRS_DEF_EXPLORE_PERC = 0.1f;
	public static final float RRS_DEF_EXPLOIT_CONF_PROB = 0.99f;
	public static final float RRS_DEF_EXPLOIT_EXP_VALUE = 0.8f;
	public static final float RRS_DEF_EXPLOIT_RED_RATIO = 0.5f;
	public static final float RRS_DEF_EXPLOIT_TERM_SIZE = 0.001f;

	private static final Log LOG = LogFactory.getLog(RRSJobOptimizer.class);

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
	public RRSJobOptimizer(JobProfileOracle jobOracle, DataSetModel dataModel,
			ClusterConfiguration cluster, IWhatIfScheduler scheduler) {
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

		// Initialize the space
		Configuration conf = new Configuration(jobConf);
		ParameterSpace space = ParamSpaceUtils.getFullParamSpace(conf);
		WhatIfEngine whatifEngine = new WhatIfEngine(jobOracle, dataModel,
				scheduler, cluster, conf);

		// Perform recursive random search to find the best point
		ParameterSpacePoint optPoint = performRecursiveRandomSearch(space,
				whatifEngine, conf);

		// Create the best MR job profile
		optPoint.populateConfiguration(conf);
		bestMRJobProfile = jobOracle.whatif(conf, dataModel);
		bestRunningTime = whatifEngine.whatIfJobConfGetTime(conf);

		// Return the best configuration
		if (!fullConf) {
			conf = new Configuration(false);
			optPoint.populateConfiguration(conf);
		}
		bestConf = conf;
		return conf;
	}

	/* ***************************************************************
	 * PROTECTED METHODS
	 * ***************************************************************
	 */

	/**
	 * Performs recursive random search to find the best point in the provided
	 * space
	 * 
	 * @param space
	 *            the parameter space
	 * @param whatifEngine
	 *            the What-if Engine
	 * @param conf
	 *            the job configuration
	 * @return the best parameter point
	 */
	protected ParameterSpacePoint performRecursiveRandomSearch(
			ParameterSpace space, WhatIfEngine whatifEngine, Configuration conf) {

		// Check for empty space
		if (space.getNumParameters() == 0)
			return new ParameterSpacePoint();

		// Initialize exploration parameters
		float p = conf.getFloat(RRS_EXPLORE_CONF_PROB,
				RRS_DEF_EXPLORE_CONF_PROB);
		float r = conf.getFloat(RRS_EXPLORE_PERC, RRS_DEF_EXPLORE_PERC);
		int n = (int) Math.round(Math.log(1 - p) / Math.log(1 - r));

		// Initialize exploitation parameters
		float q = conf.getFloat(RRS_EXPLOIT_CONF_PROB,
				RRS_DEF_EXPLOIT_CONF_PROB);
		float v = conf.getFloat(RRS_EXPLOIT_EXP_VALUE,
				RRS_DEF_EXPLOIT_EXP_VALUE);
		float c = conf.getFloat(RRS_EXPLOIT_RED_RATIO,
				RRS_DEF_EXPLOIT_RED_RATIO);
		float s_t = conf.getFloat(RRS_EXPLOIT_TERM_SIZE,
				RRS_DEF_EXPLOIT_TERM_SIZE);
		int l = (int) Math.round(Math.log(1 - q) / Math.log(1 - v));

		// Special case for very small spaces
		if (space.getNumUniqueSpacePoints() < n) {
			// Enumerate the full space
			List<ParameterSpacePoint> points = space
					.getSpacePointGrid(false, n);
			LOG.debug("Number of parameters: " + space.getNumParameters());
			LOG.debug("Number of settings: " + points.size());
			return findBestParameterSpacePoint(whatifEngine, points, conf);
		}

		// Take n random samples from the parameter space
		int countWhatIf = 0;
		ParameterSpacePoint[] x_array = new ParameterSpacePoint[n];
		double[] f_x_array = new double[n];
		for (int i = 0; i < n; ++i) {
			x_array[i] = space.getRandomSpacePoint();
			x_array[i].populateConfiguration(conf);
			f_x_array[i] = whatifEngine.whatIfJobConfGetTime(conf);
			++countWhatIf;
		}

		// Find the min point
		int minIndex = findMinIndex(f_x_array);
		ParameterSpacePoint x_0 = x_array[minIndex];
		double f_x_0 = f_x_array[minIndex];
		double y_r = f_x_0;

		// Initialize the threshold list
		ArrayList<Double> thresList = new ArrayList<Double>();
		thresList.add(f_x_0);

		// Initialize the optimal point
		ParameterSpacePoint x_opt = x_0;
		double f_x_opt = f_x_0;
		int lastCountOptChange = countWhatIf;

		// Calculate termination criteria as scaled exponential functions
		// of the number of parameters in the space
		int MAX_COUNT_WHAT_IF = (int) Math.ceil(150 * Math.pow(space
				.getNumParameters(), 1.2));
		int MAX_COUNT_OPT_CHANGE = (int) Math.ceil(80 * Math.pow(space
				.getNumParameters(), 1.2));

		// Start the main exploration search
		int i = 0;
		boolean exploit = true;

		while (countWhatIf < MAX_COUNT_WHAT_IF
				&& countWhatIf - lastCountOptChange < MAX_COUNT_OPT_CHANGE) {

			if (exploit) {
				// Start the exploitation process
				int j = 0;
				ParameterSpacePoint x_l = x_0;
				double f_x_l = f_x_0;
				ParameterSpacePoint x_prime;
				double f_x_prime = 0;
				float ro = r;

				while (ro > s_t) {
					x_prime = space.getRandomSpacePoint(x_l, ro);
					x_prime.populateConfiguration(conf);
					f_x_prime = whatifEngine.whatIfJobConfGetTime(conf);
					++countWhatIf;

					if (f_x_prime < f_x_l) {
						// Re-align the center of sample space
						x_l = x_prime;
						f_x_l = f_x_prime;
						j = 0;
					} else {
						++j;
					}

					if (j == l) {
						// Shrink the sample space
						ro *= c;
						j = 0;
					}
				} // End while

				// Update the optimal point
				exploit = false;
				if (f_x_l < f_x_opt) {
					x_opt = x_l;
					f_x_opt = f_x_l;
					lastCountOptChange = countWhatIf;
				}
			} // End exploitation

			// Take a new random space point
			x_0 = space.getRandomSpacePoint();
			x_0.populateConfiguration(conf);
			f_x_0 = whatifEngine.whatIfJobConfGetTime(conf);
			++countWhatIf;

			x_array[i] = x_0;
			f_x_array[i] = f_x_0;

			if (f_x_0 < y_r) {
				// Found a promising point, set the flag to exploit
				exploit = true;
			}

			++i;
			if (i == n) {
				// Update the exploitation threshold y_r
				minIndex = findMinIndex(f_x_array);
				thresList.add(f_x_array[minIndex]);
				y_r = findMean(f_x_array);
				i = 0;
			}
		} // End exploration

		// Log some stats
		LOG.debug("Number of parameters: " + space.getNumParameters());
		LOG.debug("Number of settings: " + countWhatIf);

		return x_opt;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Find the index of the smallest value in the array
	 * 
	 * @param values
	 *            the array of values
	 * @return the index of the smallest value
	 */
	private int findMinIndex(double[] values) {
		int minIndex = 0;
		double minValue = values[0];

		for (int i = 1; i < values.length; ++i) {
			if (values[i] < minValue) {
				minValue = values[i];
				minIndex = i;
			}
		}

		return minIndex;
	}

	/**
	 * Find the mean of the values in the array
	 * 
	 * @param values
	 *            array of values
	 * @return the mean
	 */
	private double findMean(double[] values) {
		double sum = 0;
		for (int i = 0; i < values.length; ++i) {
			sum += values[i];
		}

		return sum / values.length;
	}

}
