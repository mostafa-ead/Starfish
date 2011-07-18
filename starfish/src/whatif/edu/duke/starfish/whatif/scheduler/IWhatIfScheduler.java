package edu.duke.starfish.whatif.scheduler;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;

/**
 * Interface for the WhatIf Scheduler.
 * 
 * @author hero
 */
public interface IWhatIfScheduler {

	/**
	 * Use this method to checkpoint the status of the scheduler. Next time the
	 * method {@link IWhatIfScheduler#reset()} is invoked, the Scheduler will
	 * roll back to this point.
	 * 
	 * This method is particularly useful when we want to schedule the same job
	 * multiple times to test different configuration settings.
	 * 
	 * Also see {@link IWhatIfScheduler#reset()}
	 */
	public void checkpoint();

	/**
	 * Reset the status of the scheduler to the previous checkpoint. If not
	 * checkpoint is defined, then the scheduler is reset to the beginning.
	 * 
	 * This method is particularly useful when we want to schedule the same job
	 * multiple times to test different configuration settings.
	 * 
	 * Also see {@link IWhatIfScheduler#checkpoint()}
	 */
	public void reset();

	/**
	 * Get the cluster information
	 * 
	 * @return the cluster
	 */
	public ClusterConfiguration getCluster();

	/**
	 * Schedule the job on a cluster using the input configuration parameters
	 * and the job profile, and return a representation of the job execution.
	 * 
	 * @param submissionTime
	 *            the job submission time
	 * @param jobProfile
	 *            the virtual job profile
	 * @param conf
	 *            the job configuration parameters
	 * @return the job execution
	 */
	public MRJobInfo scheduleJobGetJobInfo(Date submissionTime,
			MRJobProfile jobProfile, Configuration conf);

	/**
	 * Schedule the job on a cluster using the input configuration parameters
	 * and the job profile, and return the overall execution time.
	 * 
	 * @param submissionTime
	 *            the job submission time
	 * @param jobProfile
	 *            the virtual job profile
	 * @param conf
	 *            the job configuration parameters
	 * @return the job execution time
	 */
	public double scheduleJobGetTime(Date submissionTime,
			MRJobProfile jobProfile, Configuration conf);

	/**
	 * When this flag is set, the reducers will not get schedule on the cluster.
	 * Instead, the job will only contain map tasks.
	 * 
	 * This is designed for efficiency purposes only, use with care!
	 * 
	 * @param ignoreReducers
	 *            set ignore reducers flag
	 */
	public void setIgnoreReducers(boolean ignoreReducers);
}
