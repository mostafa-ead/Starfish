package edu.duke.starfish.jobopt;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.jobopt.optimizer.JobOptimizer;
import edu.duke.starfish.jobopt.optimizer.SmartRRSJobOptimizer;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.metrics.Metric;
import edu.duke.starfish.profile.profileinfo.metrics.MetricType;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.whatif.WhatIfEngine;
import edu.duke.starfish.whatif.WhatIfUtils;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.FixedInputSpecsDataSetModel;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.oracle.JobProfileOracle;
import edu.duke.starfish.whatif.scheduler.BasicFIFOScheduler;

/**
 * A manager for optimized MapReduce jobs. Currently, this manager can only
 * manage one MapReduce job at a time. The idea is to create this manager
 * providing the source MR job (that contains the job profile), configuration,
 * and cluster. Then, the user can update any of the cluster, or input
 * specifications using the update* methods. The load* methods will perform the
 * optimization and populate the job and profile data.
 * 
 * @author hero
 */
public class OptimizedJobManager implements IMRInfoManager {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private MRJobInfo job;
	private MRJobProfile sourceProfile;
	private Configuration conf;
	private ClusterConfiguration cluster;
	private List<MapInputSpecs> specs;
	private boolean updateData;
	private boolean updateTransfers;
	private String sourceJobId;
	private String virtualJobId;

	/**
	 * Constructor - makes deep copies of the provided data
	 * 
	 * @param job
	 *            the MR job (must contain a job profile)
	 * @param conf
	 *            the Hadoop configuration
	 * @param cluster
	 *            the cluster information
	 */
	public OptimizedJobManager(MRJobInfo job, Configuration conf,
			ClusterConfiguration cluster) {
		this.sourceProfile = new MRJobProfile(job.getAdjProfile());
		this.conf = new Configuration(conf);
		this.cluster = new ClusterConfiguration(cluster);
		this.specs = WhatIfUtils.generateMapInputSpecs(job);

		processOptimization();
		this.updateData = false;
		this.updateTransfers = true;
		this.sourceJobId = job.getExecId();
		this.virtualJobId = this.job.getExecId();
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#getAllMRJobInfos()
	 */
	@Override
	public List<MRJobInfo> getAllMRJobInfos() {
		// There is only one job
		List<MRJobInfo> one = new ArrayList<MRJobInfo>(1);
		one.add(job);
		return one;
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#getAllMRJobInfos(java.util.Date,
	 *      java.util.Date)
	 */
	@Override
	public List<MRJobInfo> getAllMRJobInfos(Date start, Date end) {
		// There is at most one job
		List<MRJobInfo> one = new ArrayList<MRJobInfo>(1);
		if (job.getStartTime().getTime() >= start.getTime()
				&& job.getEndTime().getTime() <= end.getTime()) {
			one.add(job);
		}

		return one;
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#getClusterConfiguration(java.lang.String)
	 */
	@Override
	public ClusterConfiguration getClusterConfiguration(String mrJobId) {
		if (mrJobId.equals(sourceJobId) || mrJobId.equals(virtualJobId))
			return cluster;
		else
			return null;
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#getHadoopConfiguration(java.lang.String)
	 */
	@Override
	public Configuration getHadoopConfiguration(String mrJobId) {
		if (mrJobId.equals(sourceJobId) || mrJobId.equals(virtualJobId))
			return conf;
		else
			return null;
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#getHostMetrics(edu.duke.starfish.profile.profileinfo.metrics.MetricType,
	 *      edu.duke.starfish.profile.profileinfo.setup.HostInfo,
	 *      java.util.Date, java.util.Date)
	 */
	@Override
	public List<Metric> getHostMetrics(MetricType type, HostInfo host,
			Date start, Date end) {
		// Not available
		return null;
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#getMRJobInfo(java.lang.String)
	 */
	@Override
	public MRJobInfo getMRJobInfo(String mrJobId) {
		if (mrJobId.equals(sourceJobId) || mrJobId.equals(virtualJobId))
			return job;
		else
			return null;
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#getMRJobProfile(java.lang.String)
	 */
	@Override
	public MRJobProfile getMRJobProfile(String mrJobId) {
		if (mrJobId.equals(sourceJobId) || mrJobId.equals(virtualJobId))
			return job.getProfile();
		else
			return null;
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#loadDataTransfersForMRJob(edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo)
	 */
	@Override
	public boolean loadDataTransfersForMRJob(MRJobInfo mrJob) {
		return loadOptimizedDataTransfersForMRJob(mrJob);
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#loadProfilesForMRJob(edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo)
	 */
	@Override
	public boolean loadProfilesForMRJob(MRJobInfo mrJob) {
		return loadOptimizedDataForMRJob(mrJob);
	}

	/**
	 * @see edu.duke.starfish.profile.profileinfo.IMRInfoManager#loadTaskDetailsForMRJob(edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo)
	 */
	@Override
	public boolean loadTaskDetailsForMRJob(MRJobInfo mrJob) {
		return loadOptimizedDataForMRJob(mrJob);
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Update the cluster configuration. Next time a load*() method is called,
	 * the new cluster configuration will be used to process the what-if
	 * request.
	 * 
	 * @param newCluster
	 *            the new cluster configuration
	 */
	public void updateClusterConfiguration(ClusterConfiguration newCluster) {
		this.cluster = newCluster;
		this.updateData = true;
		this.updateTransfers = true;
	}

	/**
	 * Update the input specifications. Next time a load*() method is called,
	 * the new input specifications will be used to process the what-if request.
	 * 
	 * @param newInputSpecs
	 *            the new input specifications
	 */
	public void updateInputSpecifications(List<MapInputSpecs> newInputSpecs) {
		this.specs = newInputSpecs;
		this.updateData = true;
		this.updateTransfers = true;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Load the virtual data (task details and profiles) into the input MR job.
	 * If any of configuration, cluster, or input specifications has change
	 * since the last time this method was called, a what-if request is issued
	 * to the What-if Engine to update the data
	 * 
	 * @param mrJob
	 *            the MR job
	 * @return true if loading was successful
	 */
	private boolean loadOptimizedDataForMRJob(MRJobInfo mrJob) {
		// Ensure we are asked to load profiles for the correct job
		if (!mrJob.getExecId().equals(sourceJobId)
				&& !mrJob.getExecId().equals(virtualJobId))
			return false;

		if (updateData) {
			// Got new data -> process the what-if request
			processOptimization();
			updateData = false;
		}

		mrJob.copyOtherJob(job);
		return true;
	}

	/**
	 * Load the virtual data transfer in the provided job.
	 * 
	 * @param mrJob
	 *            the MR job
	 * @return true if loading was successful
	 */
	private boolean loadOptimizedDataTransfersForMRJob(MRJobInfo mrJob) {
		// Load the task details and profiles first
		if (!loadOptimizedDataForMRJob(mrJob))
			return false;

		boolean success = true;
		if (updateTransfers) {
			// Need to generate the new data transfers
			success = WhatIfUtils.generateDataTransfers(job, conf);
			updateTransfers = false;
		}

		mrJob.copyOtherJob(job);
		return success;
	}

	/**
	 * Performs optimization based on the configuration, cluster, and input
	 * specifications, and sets the new optimized job.
	 */
	private void processOptimization() {

		// Create the necessary parameters for the Optimizer
		JobProfileOracle jobOracle = new JobProfileOracle(sourceProfile);
		DataSetModel dataModel = new FixedInputSpecsDataSetModel(specs);
		BasicFIFOScheduler scheduler = new BasicFIFOScheduler();

		// Optimize job
		JobOptimizer optimizer = new SmartRRSJobOptimizer(jobOracle, dataModel,
				cluster, scheduler);
		conf = optimizer.findBestConfiguration(conf, true);
		WhatIfEngine whatifEngine = new WhatIfEngine(jobOracle, dataModel,
				scheduler, cluster, conf);
		job = whatifEngine.whatIfJobConfGetJobInfo(conf);
	}
}
