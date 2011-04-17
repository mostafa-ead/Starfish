package edu.duke.starfish.whatif;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;
import edu.duke.starfish.profile.profiler.XMLProfileParser;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.RealAvgDataSetModel;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.BasicFIFOScheduler;
import edu.duke.starfish.whatif.scheduler.IWhatIfScheduler;

/**
 * The What-if Engine contains the interface for other modules to ask what-if
 * questions regarding configuration, data layout, cluster setup, and task
 * scheduling.
 * 
 * @author hero
 * 
 */
public class WhatIfEngine {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private static final Log LOG = LogFactory.getLog(WhatIfEngine.class);

	// Data members for the What-if Engine
	JobProfileOracle jobOracle; // The job profile oracle
	DataSetModel dataModel; // The data model
	IWhatIfScheduler scheduler; // The task scheduler simulator
	ClusterConfiguration cluster; // The cluster setup
	Configuration conf; // The job configuration

	// Mode options
	private static final String JOB_TIME = "job_time";
	private static final String JOB_DETAILS = "details";
	private static final String JOB_PROFILE = "profile";
	private static final String TIMELINE = "timeline";
	private static final String MAPPERS = "mappers";
	private static final String REDUCERS = "reducers";

	/**
	 * Constructor
	 * 
	 * @param jobOracle
	 *            the job profile oracle
	 * @param dataModel
	 *            the data set model
	 * @param scheduler
	 *            the task scheduler
	 * @param cluster
	 *            the cluster setup
	 * @param conf
	 *            the job configuration
	 */
	public WhatIfEngine(JobProfileOracle jobOracle, DataSetModel dataModel,
			IWhatIfScheduler scheduler, ClusterConfiguration cluster,
			Configuration conf) {
		this.jobOracle = jobOracle;
		this.dataModel = dataModel;
		this.scheduler = scheduler;
		this.cluster = cluster;
		this.conf = conf;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Returns the job execution time if this particular job configuration is
	 * used
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the job execution time
	 */
	public double whatIfJobConfGetTime(Configuration conf) {

		MRJobProfile jobProf = jobOracle.whatif(conf, dataModel);
		return scheduler.scheduleJobGetTime(cluster, jobProf, conf);
	}

	/**
	 * Returns the job representation if this particular job configuration is
	 * used
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the job info
	 */
	public MRJobInfo whatIfJobConfGetJobInfo(Configuration conf) {

		MRJobProfile jobProf = jobOracle.whatif(conf, dataModel);
		return scheduler.scheduleJobGetJobInfo(cluster, jobProf, conf);
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Process a What-if request. This method is meant to be used by the
	 * BTraceWhatIf script.
	 * 
	 * @param mode
	 *            the execution mode
	 * @param profileFile
	 *            the job profile XML file
	 * @param conf
	 *            the configuration
	 */
	public static void processJobWhatIfRequest(String mode, String profileFile,
			Configuration conf) {

		// Note: we must surround the entire method to catch all exceptions
		// because BTrace cannot catch them
		try {
			// Create the default parameters for the What-if Engine
			DataSetModel dataModel = new RealAvgDataSetModel();
			ClusterConfiguration cluster = new ClusterConfiguration(conf);
			MRJobProfile sourceProf = XMLProfileParser
					.importJobProfile(new File(profileFile));
			JobProfileOracle jobOracle = new JobProfileOracle(sourceProf);
			BasicFIFOScheduler scheduler = new BasicFIFOScheduler();

			if (mode.equals(JOB_PROFILE)) {
				// Print out the job profile
				MRJobProfile jobProfile = jobOracle.whatif(conf, dataModel);
				jobProfile.printProfile(System.out, false);
				return;
			}

			// Ask the what-if question
			WhatIfEngine whatifEngine = new WhatIfEngine(jobOracle, dataModel,
					scheduler, cluster, conf);
			MRJobInfo mrJob = whatifEngine.whatIfJobConfGetJobInfo(conf);

			// Print out the output
			if (mode.equals(JOB_TIME)) {
				System.out.println("Execution Time (ms):\t"
						+ mrJob.getDuration());

			} else if (mode.equals(JOB_DETAILS)) {
				ProfileUtils.printMRJobDetails(System.out, mrJob);

			} else if (mode.equals(TIMELINE)) {
				ProfileUtils.printMRJobTimeline(System.out, mrJob);

			} else if (mode.equals(MAPPERS)) {
				ProfileUtils.printMRMapInfo(System.out, mrJob.getMapTasks());

			} else if (mode.equals(REDUCERS)) {
				ProfileUtils.printMRReduceInfo(System.out, mrJob
						.getReduceTasks());
			} else {
				LOG.error("Unsupported mode: " + mode);
			}

		} catch (Exception e) {
			LOG.error("What-if Engine failed!", e);
		}

	}

}
