package edu.duke.starfish.whatif;

import java.io.PrintStream;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.utils.ProfileUtils;
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
	 * WHAT-IF QUESTION ENUM
	 * ***************************************************************
	 */

	/**
	 * Lists all the question the What-if Engine can answer
	 */
	public static enum WhatIfQuestion {
		TIME, // The job execution time
		DETAILS, // The job statistical details
		PROFILE, // The job profile
		TIMELINE, // The task timeline
		MAPPERS, // The map tasks details
		REDUCERS; // The reduce tasks details

		/**
		 * Check if this is a valid string representation of a question
		 * 
		 * @param question
		 *            the question
		 * @return true if it is a valid question
		 */
		public static boolean isValid(String question) {
			return getQuestion(question) != null;
		}

		/**
		 * Get the question
		 * 
		 * @param question
		 *            the string representation of the question
		 * @return the enum
		 */
		public static WhatIfQuestion getQuestion(String question) {
			if (question.equals("profile")) {
				return PROFILE;
			} else if (question.equals("time")) {
				return TIME;
			} else if (question.equals("details")) {
				return DETAILS;
			} else if (question.equals("timeline")) {
				return TIMELINE;
			} else if (question.equals("mappers")) {
				return MAPPERS;
			} else if (question.equals("reducers")) {
				return REDUCERS;
			} else {
				return null;
			}

		}
	}

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private static final Log LOG = LogFactory.getLog(WhatIfEngine.class);

	// Data members for the What-if Engine
	private JobProfileOracle jobOracle; // The job profile oracle
	private DataSetModel dataModel; // The data model
	private IWhatIfScheduler scheduler; // The task scheduler simulator

	/**
	 * Constructor
	 * 
	 * @param jobOracle
	 *            the job profile oracle
	 * @param dataModel
	 *            the data set model
	 * @param scheduler
	 *            the task scheduler
	 */
	public WhatIfEngine(JobProfileOracle jobOracle, DataSetModel dataModel,
			IWhatIfScheduler scheduler) {
		this.jobOracle = jobOracle;
		this.dataModel = dataModel;
		this.scheduler = scheduler;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Returns the job execution time if this particular job configuration is
	 * used.
	 * 
	 * Also see {@link WhatIfEngine#whatIfJobConfGetTime(Date, Configuration)}
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the job execution time (in ms)
	 */
	public double whatIfJobConfGetTime(Configuration conf) {

		return whatIfJobConfGetTime(new Date(), conf);
	}

	/**
	 * Returns the job execution time if this particular job configuration is
	 * used.
	 * 
	 * The specified submission time will be used by the scheduler when
	 * simulating the job execution. This method is useful when you want to ask
	 * what-if questions for multiple jobs that will be executed on the same
	 * cluster.
	 * 
	 * @param submissionTime
	 *            the job submission time
	 * @param conf
	 *            the job configuration
	 * @return the job execution time (in ms)
	 */
	public double whatIfJobConfGetTime(Date submissionTime, Configuration conf) {

		MRJobProfile jobProf = jobOracle.whatif(conf, dataModel);
		return scheduler.scheduleJobGetTime(submissionTime, jobProf, conf);
	}

	/**
	 * Returns the job representation if this particular job configuration is
	 * used.
	 * 
	 * Also see
	 * {@link WhatIfEngine#whatIfJobConfGetJobInfo(Date, Configuration)}
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the job info
	 */
	public MRJobInfo whatIfJobConfGetJobInfo(Configuration conf) {

		return whatIfJobConfGetJobInfo(new Date(), conf);
	}

	/**
	 * Returns the job representation if this particular job configuration is
	 * used.
	 * 
	 * The specified submission time will be used by the scheduler when
	 * simulating the job execution. This method is useful when you want to ask
	 * what-if questions for multiple jobs that will be executed on the same
	 * cluster.
	 * 
	 * @param submissionTime
	 *            the job submission time
	 * @param conf
	 *            the job configuration
	 * @return the job info
	 */
	public MRJobInfo whatIfJobConfGetJobInfo(Date submissionTime,
			Configuration conf) {

		MRJobProfile jobProf = jobOracle.whatif(conf, dataModel);
		return scheduler.scheduleJobGetJobInfo(submissionTime, jobProf, conf);
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Process a What-if request. This method is meant to be used by the
	 * BTraceJobWhatIf script.
	 * 
	 * @param question
	 *            the what-if question to ask
	 * @param jobProfileId
	 *            the job id of the profiled job (to ensure backwards
	 *            compatibility, we allow this parameter to be a file path to
	 *            the profile XML file)
	 * @param conf
	 *            the configuration
	 */
	public static void processJobWhatIfRequest(String question,
			String jobProfileId, Configuration conf) {

		// Note: we must surround the entire method to catch all exceptions
		// because BTrace cannot catch them
		try {
			// Get the source profile
			MRJobProfile sourceProf = ProfileUtils.loadSourceProfile(
					jobProfileId, conf);
			if (sourceProf == null) {
				LOG.error("Unable to load the profile for " + jobProfileId);
				return;
			}

			// Create the default parameters for the What-if Engine
			DataSetModel dataModel = new RealAvgDataSetModel();
			ClusterConfiguration cluster = new ClusterConfiguration(conf);

			// Answer the question
			if (WhatIfQuestion.isValid(question)) {
				answerWhatIfQuestion(WhatIfQuestion.getQuestion(question),
						sourceProf, dataModel, cluster, conf, System.out);
			} else {
				LOG.error("Unsupported question: " + question);
			}

		} catch (Exception e) {
			LOG.error("What-if Engine failed!", e);
		}

	}

	/**
	 * Answers a what-if question
	 * 
	 * @param question
	 *            the question to ask
	 * @param sourceProf
	 *            the source profile
	 * @param dataModel
	 *            the input data model
	 * @param cluster
	 *            the cluster configuration
	 * @param conf
	 *            the job configuration
	 * @param out
	 *            the output stream to write to
	 */
	public static void answerWhatIfQuestion(WhatIfQuestion question,
			MRJobProfile sourceProf, DataSetModel dataModel,
			ClusterConfiguration cluster, Configuration conf, PrintStream out) {

		// Build the required what-if components
		JobProfileOracle jobOracle = new JobProfileOracle(sourceProf);
		BasicFIFOScheduler scheduler = new BasicFIFOScheduler(cluster);

		// Ask the what-if question
		WhatIfEngine whatifEngine = new WhatIfEngine(jobOracle, dataModel,
				scheduler);
		MRJobInfo mrJob = whatifEngine.whatIfJobConfGetJobInfo(conf);

		// Answer the question
		switch (question) {
		case PROFILE:
			mrJob.getProfile().printProfile(out, false);
			break;
		case TIME:
			out.println("Execution Time (ms):\t" + mrJob.getDuration());
			break;
		case DETAILS:
			ProfileUtils.printMRJobDetails(out, mrJob);
			break;
		case TIMELINE:
			ProfileUtils.printMRJobTimeline(out, mrJob);
			break;
		case MAPPERS:
			ProfileUtils.printMRMapInfo(out, mrJob.getMapTasks());
			break;
		case REDUCERS:
			ProfileUtils.printMRReduceInfo(out, mrJob.getReduceTasks());
			break;
		default:
			break;
		}
	}

}
