package edu.duke.starfish.jobopt.optimizer;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_COMBINE_CLASS;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.STARFISH_USE_COMBINER;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.duke.starfish.jobopt.space.ParameterSpacePoint;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profiler.XMLProfileParser;
import edu.duke.starfish.whatif.WhatIfEngine;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.RealAvgDataSetModel;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.BasicFIFOScheduler;
import edu.duke.starfish.whatif.scheduler.BasicFIFOSchedulerForOptimizer;
import edu.duke.starfish.whatif.scheduler.IWhatIfScheduler;

/**
 * This is the basis class for the job optimizer
 * 
 * @author hero
 */
public abstract class JobOptimizer {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private static final Log LOG = LogFactory.getLog(JobOptimizer.class);

	// Data members for the Optimizer
	protected JobProfileOracle jobOracle; // The job profile oracle
	protected DataSetModel dataModel; // The data model
	protected ClusterConfiguration cluster; // The cluster setup
	protected IWhatIfScheduler scheduler; // The task scheduler

	// Populated after the optimization process
	protected Configuration bestConf; // The best configuration
	protected MRJobProfile bestMRJobProfile; // The best MRJobProfile
	protected double bestRunningTime;

	// Optimizer types
	private static final String OPT_FULL = "full";
	private static final String OPT_SMART_FULL = "smart_full";
	private static final String OPT_RRS = "rrs";
	private static final String OPT_SMART_RRS = "smart_rrs";

	// Scheduler options
	private static final String SCH_BASIC = "basic";
	private static final String SCH_ADVANCED = "advanced";

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
	public JobOptimizer(JobProfileOracle jobOracle, DataSetModel dataModel,
			ClusterConfiguration cluster, IWhatIfScheduler scheduler) {
		this.jobOracle = jobOracle;
		this.dataModel = dataModel;
		this.cluster = cluster;
		this.scheduler = scheduler;
		this.bestMRJobProfile = null;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Get the best MR job configuration.
	 * 
	 * Warning: This method should only be called after findBestConfiguration()
	 * is called
	 * 
	 * @return the best MR job configuration
	 */
	public Configuration getBestConfiguration() {
		return bestConf;
	}

	/**
	 * Get the best MR job profile.
	 * 
	 * Warning: This method should only be called after findBestConfiguration()
	 * is called
	 * 
	 * @return the best MR job profile
	 */
	public MRJobProfile getBestMRJobProfile() {
		return bestMRJobProfile;
	}

	/**
	 * Get the best MR job running time (in ms).
	 * 
	 * Warning: This method should only be called after findBestConfiguration()
	 * is called
	 * 
	 * @return the best MR job running time
	 */
	public double getBestRunningTime() {
		return bestRunningTime;
	}

	/**
	 * Find the best configuration for the job
	 * 
	 * @param jobConf
	 *            the existing job configuration
	 * @return the best configuration
	 */
	public Configuration findBestConfiguration(Configuration jobConf) {
		return findBestConfiguration(jobConf, true);
	}

	/**
	 * Find the best configuration for the job
	 * 
	 * @param jobConf
	 *            the existing job configuration
	 * @param fullConf
	 *            whether to produce the full configuration or only the
	 *            optimized parameters
	 * @return the best configuration
	 */
	public abstract Configuration findBestConfiguration(Configuration jobConf,
			boolean fullConf);

	/* ***************************************************************
	 * PROTECTED METHODS
	 * ***************************************************************
	 */

	/**
	 * Find the best parameter space point from the collection of points using
	 * the provided What-if Engine and configuration
	 * 
	 * @param whatifEngine
	 *            the What-if Engine
	 * @param points
	 *            the parameter space points
	 * @param conf
	 *            the configuration
	 * @return the best parameter space point
	 */
	protected ParameterSpacePoint findBestParameterSpacePoint(
			WhatIfEngine whatifEngine, Collection<ParameterSpacePoint> points,
			Configuration conf) {

		// Find the best parameter space point
		double minTime = Double.MAX_VALUE;
		double currTime = 0;
		ParameterSpacePoint bestPoint = new ParameterSpacePoint();

		for (ParameterSpacePoint point : points) {

			// Ask the what-if question for each parameter space point
			point.populateConfiguration(conf);
			currTime = whatifEngine.whatIfJobConfGetTime(conf);

			if (currTime < minTime) {
				minTime = currTime;
				bestPoint = point;
			}
		}

		return bestPoint;
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Process a job optimization request. Given a job and the job profile file,
	 * this method will modify the job's configuration to use the recommended
	 * settings.
	 * 
	 * Note: The Hadoop parameter 'starfish.job.optimizer.type' should contain
	 * the type of the optimizer to use: rrs, smart_rrs, full, smart_full.
	 * 
	 * @param job
	 *            the MapReduce job
	 * @param profileFile
	 *            the job profile XML file
	 * @return true if the optimization succeeded
	 */
	public static boolean processJobOptimizationRequest(Job job,
			String profileFile) {

		// Find the best configuration
		Configuration bestConf = findBestJobConfiguration(job, profileFile);
		if (bestConf != null) {
			// Copy the optimal configurations
			Configuration conf = job.getConfiguration();
			for (Map.Entry<String, String> entry : bestConf) {
				conf.set(entry.getKey(), entry.getValue());
				LOG.info(entry.getKey() + " = " + entry.getValue());
			}

			// Special case for using the combiner
			if (conf.get(MR_COMBINE_CLASS) != null
					&& conf.getBoolean(STARFISH_USE_COMBINER, true) == false) {
				// There is a combiner but the optimizer said not to use it and
				// there is no way to remove a configuration setting.
				// So, create a copy, clear the current conf, and copy over all
				// the settings other than the combiner class
				Configuration copy = new Configuration(conf);
				conf.clear();
				for (Entry<String, String> entry : copy) {
					if (!entry.getKey().equals(MR_COMBINE_CLASS))
						conf.set(entry.getKey(), entry.getValue());
				}
			}

			return true;
		}

		return false;
	}

	/**
	 * Process a job recommendation request. Given a job and the job profile
	 * file, this method will return a configuration containing the recommended
	 * settings.
	 * 
	 * Note: The Hadoop parameter 'starfish.job.optimizer.type' should contain
	 * the type of the optimizer to use: rrs, smart_rrs, full, smart_full.
	 * 
	 * @param job
	 *            the MapReduce job
	 * @param profileFile
	 *            the job profile XML file
	 * @return true if the optimization succeeded
	 */
	public static boolean processJobRecommendationRequest(Job job,
			String profileFile) {

		// Find the best configuration
		Configuration bestConf = findBestJobConfiguration(job, profileFile);
		if (bestConf != null) {

			// Get the output location
			PrintStream out = null;
			String output = job.getConfiguration().get(
					"starfish.job.optimizer.output", "stdout");
			if (output.equals("stdout")) {
				out = System.out;
			} else if (output.equals("stderr")) {
				out = System.err;
			} else {
				File outFile = new File(output);
				try {
					out = new PrintStream(outFile);
				} catch (FileNotFoundException e) {
					LOG.error("Job optimization failed!", e);
					return false;
				}
			}

			// Print out the recommendation
			try {
				bestConf.writeXml(out);
				out.println();
			} catch (IOException e) {
				LOG.error("Job optimization failed!", e);
				return false;
			}

			return true;
		}

		return false;
	}

	/**
	 * Given a job and the job profile file, this method will return a
	 * configuration containing the recommended settings.
	 * 
	 * Note: The Hadoop parameter 'starfish.job.optimizer.type' should contain
	 * the type of the optimizer to use: rrs, smart_rrs, full, smart_full.
	 * 
	 * @param job
	 *            the MapReduce job
	 * @param profileFile
	 *            the job profile XML file
	 * @return the optimized configuration
	 */
	public static Configuration findBestJobConfiguration(Job job,
			String profileFile) {

		// Note: we must surround the entire method to catch all exceptions
		// because BTrace cannot catch them
		Configuration conf = job.getConfiguration();
		Configuration bestConf = null;
		try {
			// Create the default parameters for the Job Optimizer
			DataSetModel dataModel = new RealAvgDataSetModel();
			ClusterConfiguration cluster = new ClusterConfiguration(conf);
			MRJobProfile sourceProf = XMLProfileParser
					.importJobProfile(new File(profileFile));
			JobProfileOracle jobOracle = new JobProfileOracle(sourceProf);

			// Get the task scheduler
			String strScheduler = conf.get("starfish.whatif.task.scheduler",
					"advanced");
			IWhatIfScheduler scheduler = getTaskScheduler(strScheduler);

			// Get the job optimizer
			String type = conf
					.get("starfish.job.optimizer.type", OPT_SMART_RRS);
			LOG.info("Job optimizer used: " + type);
			JobOptimizer optimizer = JobOptimizer.getJobOptimizer(type,
					jobOracle, dataModel, cluster, scheduler);

			// Find the best configuration
			long start = System.currentTimeMillis();
			bestConf = optimizer.findBestConfiguration(conf, false);
			long end = System.currentTimeMillis();
			LOG.info("Job optimization time (ms): " + (end - start));

		} catch (Exception e) {
			LOG.error("Job optimization failed!", e);
		}

		return bestConf;
	}

	/**
	 * Create and return the default optimizer to use
	 * 
	 * @param profile
	 *            the job profile
	 * @param dataModel
	 *            the data model
	 * @param cluster
	 *            the cluster
	 * @return the job optimizer
	 */
	public static JobOptimizer getJobOptimizer(MRJobProfile profile,
			DataSetModel dataModel, ClusterConfiguration cluster) {

		return getJobOptimizer(OPT_SMART_RRS, new JobProfileOracle(profile),
				dataModel, cluster, new BasicFIFOScheduler());
	}

	/**
	 * Create and return the appropriate optimizer based on the type.
	 * 
	 * @param type
	 *            the optimizer type (full, smart_full, rrs, smart_rrs)
	 * @param jobOracle
	 *            the job profile oracle
	 * @param dataModel
	 *            the data model
	 * @param cluster
	 *            the cluster
	 * @param scheduler
	 *            the scheduler
	 * @return the job optimizer
	 */
	public static JobOptimizer getJobOptimizer(String type,
			JobProfileOracle jobOracle, DataSetModel dataModel,
			ClusterConfiguration cluster, IWhatIfScheduler scheduler) {

		JobOptimizer optimizer = null;
		if (type.equals(OPT_FULL)) {
			optimizer = new FullEnumJobOptimizer(jobOracle, dataModel, cluster,
					scheduler);
		} else if (type.equals(OPT_SMART_FULL)) {
			optimizer = new SmartEnumJobOptimizer(jobOracle, dataModel,
					cluster, scheduler);
		} else if (type.equals(OPT_RRS)) {
			optimizer = new RRSJobOptimizer(jobOracle, dataModel, cluster,
					scheduler);
		} else if (type.equals(OPT_SMART_RRS)) {
			optimizer = new SmartRRSJobOptimizer(jobOracle, dataModel, cluster,
					scheduler);
		} else {
			LOG.error("Unsupported optimizer type: " + type);
		}
		return optimizer;
	}

	/**
	 * Create and return the requested scheduler
	 * 
	 * @param type
	 *            the type of the scheduler (basic, advanced)
	 * @return the scheduler
	 */
	public static IWhatIfScheduler getTaskScheduler(String type) {

		IWhatIfScheduler scheduler = null;
		if (type.equals(SCH_BASIC)) {
			scheduler = new BasicFIFOSchedulerForOptimizer();
		} else if (type.equals(SCH_ADVANCED)) {
			scheduler = new BasicFIFOScheduler();
		} else {
			LOG.error("Unsupported optimizer type: " + type);
		}

		return scheduler;
	}

}
