package edu.duke.starfish.whatif.scheduler;

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
	 * Schedule the job on the provided cluster using the input configuration
	 * parameters and return a representation of the job execution.
	 * 
	 * @param cluster
	 *            the cluster representation
	 * @param jobProfile
	 *            the virtual job profile
	 * @param conf
	 *            the job configuration parameters
	 * @return the job execution
	 */
	public MRJobInfo scheduleJobGetJobInfo(ClusterConfiguration cluster,
			MRJobProfile jobProfile, Configuration conf);

	/**
	 * Schedule the job on the provided cluster using the input configuration
	 * parameters and return the overall execution time.
	 * 
	 * @param cluster
	 *            the cluster representation
	 * @param jobProfile
	 *            the virtual job profile
	 * @param conf
	 *            the job configuration parameters
	 * @return the job execution time
	 */
	public double scheduleJobGetTime(ClusterConfiguration cluster,
			MRJobProfile jobProfile, Configuration conf);

	/**
	 * @param ignoreReducers
	 *            set ignore reducers flag
	 */
	public void setIgnoreReducers(boolean ignoreReducers);
}
