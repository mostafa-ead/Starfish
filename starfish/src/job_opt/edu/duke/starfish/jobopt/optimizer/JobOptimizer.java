package edu.duke.starfish.jobopt.optimizer;

import static edu.duke.starfish.profile.utils.Constants.MR_COMBINE_CLASS;
import static edu.duke.starfish.profile.utils.Constants.STARFISH_USE_COMBINER;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.duke.starfish.jobopt.space.ParameterSpacePoint;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profiler.Profiler;
import edu.duke.starfish.profile.utils.ProfileUtils;
import edu.duke.starfish.whatif.WhatIfEngine;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.RealAvgDataSetModel;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.BasicFIFOScheduler;
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
	protected Configuration currConf; // The best configuration

	private WhatIfEngine whatifEngine; // The what-if engine
	private Date submissionTime; // The job submission time

	// Populated AFTER the optimization process
	private ParameterSpacePoint bestPoint; // The best point
	private MRJobInfo bestJob; // The best job

	/* ***************************************************************
	 * STATIC DATA MEMBERS
	 * ***************************************************************
	 */

	public static final String JOB_OPT_TYPE = "starfish.job.optimizer.type";
	public static final String JOB_OPT_EXCLUDE_PARAMS = "starfish.job.optimizer.exclude.parameters";
	public static final String JOB_OPT_OUTPUT = "starfish.job.optimizer.output";
	public static final String JOB_OPT_MODE = "starfish.job.optimizer.mode";
	public static final String JOB_OPT_PROFILE_ID = "starfish.job.optimizer.profile.id";
	public static final String JOB_OPT_SCHEDULER = "starfish.whatif.task.scheduler";

	public static final String JOB_OPT_RUN = "run";
	public static final String JOB_OPT_RECOMMEND = "recommend";

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
	 * @param scheduler
	 *            the scheduler
	 * @param cluster
	 *            the cluster setup
	 * @param conf
	 *            the current configuration settings
	 */
	public JobOptimizer(JobProfileOracle jobOracle, DataSetModel dataModel,
			IWhatIfScheduler scheduler, ClusterConfiguration cluster,
			Configuration conf) {

		this.jobOracle = jobOracle;
		this.dataModel = dataModel;
		this.cluster = cluster;
		this.scheduler = scheduler;
		this.currConf = new Configuration(conf);

		this.bestPoint = null;
		this.bestJob = null;

		this.whatifEngine = new WhatIfEngine(jobOracle, dataModel, scheduler);
		this.submissionTime = null;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Get the best MR job configuration.
	 * 
	 * Warning: This method should only be called after optimize() is called
	 * 
	 * @param fullConf
	 *            whether to produce the full configuration or only the
	 *            optimized parameters
	 * 
	 * @return the best MR job configuration
	 */
	public Configuration getBestConfiguration(boolean fullConf) {
		Configuration bestConf;
		if (fullConf) {
			bestConf = new Configuration(currConf);
		} else {
			bestConf = new Configuration(false);
		}
		bestPoint.populateConfiguration(bestConf);
		return bestConf;
	}

	/**
	 * Get the best MR job description.
	 * 
	 * Warning: This method should only be called after optimize() is called
	 * 
	 * @return the best MR job
	 */
	public MRJobInfo getBestMRJobInfo() {
		return bestJob;
	}

	/**
	 * Get the best MR job profile.
	 * 
	 * Warning: This method should only be called after optimize() is called
	 * 
	 * @return the best MR job profile
	 */
	public MRJobProfile getBestMRJobProfile() {
		return bestJob.getProfile();
	}

	/**
	 * Get the best MR job running time (in ms).
	 * 
	 * Warning: This method should only be called after optimize() is called
	 * 
	 * @return the best MR job running time
	 */
	public double getBestRunningTime() {
		return bestJob.getDuration();
	}

	/**
	 * The main optimization method for a MapReduce job. This method is
	 * responsible for enumerating the search space of configuration parameter
	 * settings to find the best settings. After using this method, you can use
	 * any of the getBestX() methods.
	 * 
	 */
	public void optimize() {
		optimize(new Date());
	}

	/**
	 * The main optimization method for a MapReduce job. This method is
	 * responsible for enumerating the search space of configuration parameter
	 * settings to find the best settings. After using this method, you can use
	 * any of the getBestX() methods.
	 * 
	 * The specified submission time will be used by the scheduler when
	 * simulating the job execution. This method is useful when you want to
	 * optimize multiple jobs that will be executed on the same cluster.
	 * 
	 * @param submissionTime
	 *            the job submission time
	 */
	public void optimize(Date submissionTime) {
		// Checkpoint the schedule and optimize the job
		this.submissionTime = submissionTime;
		scheduler.checkpoint();
		bestPoint = optimizeInternal();

		// Get the best MR job (based on the best configuration)
		scheduler.reset();
		bestJob = whatifEngine.whatIfJobConfGetJobInfo(submissionTime,
				getBestConfiguration(true));
	}

	/* ***************************************************************
	 * PROTECTED METHODS
	 * ***************************************************************
	 */

	/**
	 * The main optimization method to be overridden. This method is responsible
	 * for enumerating the configuration parameter space and returning the best
	 * parameter space point.
	 * 
	 * @return the best parameter space point
	 */
	protected abstract ParameterSpacePoint optimizeInternal();

	/**
	 * Find the best parameter space point from the collection of points using
	 * the provided What-if Engine and configuration
	 * 
	 * @param points
	 *            the parameter space points
	 * @param conf
	 *            the configuration
	 * @return the best parameter space point
	 */
	protected ParameterSpacePoint findBestParameterSpacePoint(
			Collection<ParameterSpacePoint> points, Configuration conf) {

		// Find the best parameter space point
		double minTime = Double.MAX_VALUE;
		double currTime = 0;
		ParameterSpacePoint bestPoint = new ParameterSpacePoint();

		for (ParameterSpacePoint point : points) {

			// Ask the what-if question for each parameter space point
			point.populateConfiguration(conf);
			currTime = whatif(conf);

			if (currTime < minTime) {
				minTime = currTime;
				bestPoint = point;
			}
		}

		return bestPoint;
	}

	/**
	 * Asks the What-if Engine to find the running time of the job with this
	 * configuration
	 * 
	 * @param conf
	 *            the suggested configuration
	 * @return the estimated running time
	 */
	protected double whatif(Configuration conf) {
		scheduler.reset();
		return whatifEngine.whatIfJobConfGetTime(submissionTime, conf);
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Copy the optimal settings from 'bestConf' into the current 'conf'
	 * 
	 * @param bestConf
	 *            the configuration to copy from
	 * @param conf
	 *            the configuration to copy to
	 */
	public static void copyOptimalConfSettings(Configuration bestConf,
			Configuration conf) {

		// Copy the optimal configurations
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
	}

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
	 * @param jobProfileId
	 *            the job id of the profiled job
	 * @return true if the optimization succeeded
	 */
	public static boolean processJobOptimizationRequest(Job job,
			String jobProfileId) {

		// Find the best configuration
		Configuration bestConf = findBestJobConfiguration(job, jobProfileId);
		if (bestConf != null) {
			// Copy the optimal configurations
			copyOptimalConfSettings(bestConf, job.getConfiguration());
			return true;
		} else {
			return false;
		}
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
	 * @param jobProfileId
	 *            the job id of the profiled job
	 * @return true if the optimization succeeded
	 */
	public static boolean processJobRecommendationRequest(Job job,
			String jobProfileId) {

		// Find the best configuration
		Configuration bestConf = findBestJobConfiguration(job, jobProfileId);
		if (bestConf != null) {

			// Get the output location
			PrintStream out = getOptimizerOutput(job.getConfiguration());
			if (out == null)
				return false;

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
	 * @param jobProfileId
	 *            the job id of the profiled job (to ensure backwards
	 *            compatibility, we allow this parameter to be a file path to
	 *            the profile XML file)
	 * @return the optimized configuration
	 */
	public static Configuration findBestJobConfiguration(Job job,
			String jobProfileId) {

		// Note: we must surround the entire method to catch all exceptions
		// because BTrace cannot catch them
		Configuration conf = job.getConfiguration();
		try {
			// Get the source profile
			MRJobProfile sourceProf = ProfileUtils.loadSourceProfile(
					jobProfileId, conf);
			if (sourceProf == null) {
				LOG.error("Unable to load the profile for " + jobProfileId);
				return null;
			}

			// Create the default parameters for the Job Optimizer
			DataSetModel dataModel = new RealAvgDataSetModel();
			ClusterConfiguration cluster = new ClusterConfiguration(conf);
			JobProfileOracle jobOracle = new JobProfileOracle(sourceProf);

			// Get the task scheduler
			String strScheduler = conf.get(JOB_OPT_SCHEDULER, SCH_ADVANCED);
			IWhatIfScheduler scheduler = getTaskScheduler(cluster, strScheduler);

			// Get the job optimizer
			String type = conf.get(JOB_OPT_TYPE, OPT_SMART_RRS);
			LOG.info("Job optimizer used: " + type);
			JobOptimizer optimizer = JobOptimizer.getJobOptimizer(type,
					jobOracle, dataModel, cluster, conf, scheduler);

			// Find the best configuration
			long start = System.currentTimeMillis();
			optimizer.optimize();
			long end = System.currentTimeMillis();
			LOG.info("Job optimization time (ms): " + (end - start));

			return optimizer.getBestConfiguration(false);

		} catch (Exception e) {
			LOG.error("Job optimization failed!", e);
			return null;
		}
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
			DataSetModel dataModel, ClusterConfiguration cluster,
			Configuration conf) {

		return getJobOptimizer(OPT_SMART_RRS, new JobProfileOracle(profile),
				dataModel, cluster, conf, new BasicFIFOScheduler(cluster));
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
	 * @param conf
	 *            the current configuration settings
	 * @param scheduler
	 *            the scheduler
	 * @return the job optimizer
	 */
	public static JobOptimizer getJobOptimizer(String type,
			JobProfileOracle jobOracle, DataSetModel dataModel,
			ClusterConfiguration cluster, Configuration conf,
			IWhatIfScheduler scheduler) {

		JobOptimizer optimizer = null;
		if (type.equals(OPT_FULL)) {
			optimizer = new FullEnumJobOptimizer(jobOracle, dataModel,
					scheduler, cluster, conf);
		} else if (type.equals(OPT_SMART_FULL)) {
			optimizer = new SmartEnumJobOptimizer(jobOracle, dataModel,
					scheduler, cluster, conf);
		} else if (type.equals(OPT_RRS)) {
			optimizer = new RRSJobOptimizer(jobOracle, dataModel, scheduler,
					cluster, conf);
		} else if (type.equals(OPT_SMART_RRS)) {
			optimizer = new SmartRRSJobOptimizer(jobOracle, dataModel,
					scheduler, cluster, conf);
		} else {
			LOG.error("Unsupported optimizer type: " + type);
		}
		return optimizer;
	}

	/**
	 * Create and return the requested scheduler
	 * 
	 * @param cluster
	 *            the cluster configuration for the scheduler to use
	 * @param type
	 *            the type of the scheduler (basic, advanced)
	 * @return the scheduler
	 */
	public static IWhatIfScheduler getTaskScheduler(
			ClusterConfiguration cluster, String type) {

		IWhatIfScheduler scheduler = null;
		if (type.equals(SCH_BASIC)) {
			LOG.error("The 'basic' optimizer is not supported anymore!");
		} else if (type.equals(SCH_ADVANCED)) {
			scheduler = new BasicFIFOScheduler(cluster);
		} else {
			LOG.error("Unsupported optimizer type: " + type);
		}

		return scheduler;
	}

	/**
	 * Get the output stream to use for the optimizer's output based on the
	 * configuration setting "starfish.job.optimizer.output"
	 * 
	 * @param conf
	 *            the configuration
	 * @return the output stream (null if something goes wrong)
	 */
	public static PrintStream getOptimizerOutput(Configuration conf) {

		// Get the output location
		PrintStream out = null;
		String output = conf.get(JOB_OPT_OUTPUT, "stdout");
		if (output.equals("stdout")) {
			out = System.out;
		} else if (output.equals("stderr")) {
			out = System.err;
		} else {
			File outFile = new File(output);
			try {
				out = new PrintStream(outFile);
			} catch (FileNotFoundException e) {
				LOG.error("Unable to build output stream", e);
			}
		}

		return out;
	}

	/**
	 * Loads system properties related to optimization into the Hadoop
	 * configuration. The system properties are set in the bin/config.sh script.
	 * 
	 * @param conf
	 *            the configuration
	 */
	public static void loadOptimizationSystemProperties(Configuration conf) {

		// Load the common system properties
		Profiler.loadProfilingSystemProperties(conf);

		// Set the optimizer type
		if (conf.get(JOB_OPT_TYPE) == null)
			conf.set(JOB_OPT_TYPE, System.getProperty(JOB_OPT_TYPE));

		// Set the task scheduler
		if (conf.get(JOB_OPT_SCHEDULER) == null)
			conf.set(JOB_OPT_SCHEDULER, System.getProperty(JOB_OPT_SCHEDULER));

		// Set the excluded parameters
		if (conf.get(JOB_OPT_EXCLUDE_PARAMS) == null)
			conf.set(JOB_OPT_EXCLUDE_PARAMS,
					System.getProperty(JOB_OPT_EXCLUDE_PARAMS));

		// Set the output location
		if (conf.get(JOB_OPT_OUTPUT) == null)
			conf.set(JOB_OPT_OUTPUT, System.getProperty(JOB_OPT_OUTPUT));
	}
}
