package edu.duke.starfish.profile.profileinfo;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.metrics.Metric;
import edu.duke.starfish.profile.profileinfo.metrics.MetricType;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;

/**
 * Interface for a manager that manages the information regarding the setup and
 * execution of MapReduce jobs in a cluster
 * 
 * @author hero
 */
public interface IMRInfoManager {

	/**
	 * Find and return all available MRJobInfos.
	 * 
	 * NOTE: The only populate fields in the MRJobInfo are:
	 * <ol>
	 * <li>execId</li>
	 * <li>startTime</li>
	 * <li>endTime</li>
	 * <li>status</li>
	 * <li>errorMsg</li>
	 * <li>name</li>
	 * <li>user</li>
	 * </ol>
	 * 
	 * @return a list of MRJobInfos
	 */
	public List<MRJobInfo> getAllMRJobInfos();

	/**
	 * Find and return all the JobInfos that where completely executed within
	 * the provided interval.
	 * 
	 * NOTE: The only populate fields in the MRJobInfo are:
	 * <ol>
	 * <li>execId</li>
	 * <li>startTime</li>
	 * <li>endTime</li>
	 * <li>status</li>
	 * <li>errorMsg</li>
	 * <li>name</li>
	 * <li>user</li>
	 * </ol>
	 * 
	 * @param start
	 *            the start time of the interval of interest
	 * @param end
	 *            the end time of the interval of interest
	 * @return a list of MRJobInfos
	 */
	public List<MRJobInfo> getAllMRJobInfos(Date start, Date end);

	/**
	 * Find and return the MRJobInfo with the provided execution id
	 * 
	 * NOTE: The only populate fields in the MRJobInfo are:
	 * <ol>
	 * <li>execId</li>
	 * <li>startTime</li>
	 * <li>endTime</li>
	 * <li>status</li>
	 * <li>errorMsg</li>
	 * <li>name</li>
	 * <li>user</li>
	 * </ol>
	 * 
	 * @param mrJobId
	 *            the job id
	 * @return the MRJobInfo
	 */
	public MRJobInfo getMRJobInfo(String mrJobId);

	/**
	 * Returns the cluster configuration present during the execution of this
	 * job.
	 * 
	 * NOTE: if the cluster configuration changed during the job execution, the
	 * returned object will reflect the union of all configurations.
	 * 
	 * @param mrJobId
	 *            the job id
	 * @return the cluster configuration
	 */
	public ClusterConfiguration getClusterConfiguration(String mrJobId);

	/**
	 * Finds and returns the MR job configuration given a job id. Returns null
	 * if the job is not found.
	 * 
	 * @param mrJobId
	 *            the job id
	 * @return the Hadoop configuration
	 */
	public Configuration getHadoopConfiguration(String mrJobId);

	/**
	 * Finds and returns the MR job profile given a job id. Returns null if the
	 * job is not found.
	 * 
	 * @param mrJobId
	 *            the job id
	 * @return the job profile
	 */
	public MRJobProfile getMRJobProfile(String mrJobId);

	/**
	 * Return a list of metrics of the given type on the given host within the
	 * specified interval
	 * 
	 * @param type
	 *            type of metric
	 * @param host
	 *            the host machine
	 * @param start
	 *            the start time for the metrics
	 * @param end
	 *            the end time for the metrics
	 * @return a list of metrics
	 */
	public List<Metric> getHostMetrics(MetricType type, HostInfo host,
			Date start, Date end);

	/**
	 * Finds and populates the map-reduce job with all task information.
	 * 
	 * @param mrJob
	 *            the map reduce job
	 * @return true if the data were loaded successfully
	 */
	public boolean loadTaskDetailsForMRJob(MRJobInfo mrJob);

	/**
	 * Finds and populates the MRJobInfo with all data transfers that occurred
	 * during this MR job execution
	 * 
	 * @param mrJob
	 *            the map reduce job
	 * @return true if the data were loaded successfully
	 */
	public boolean loadDataTransfersForMRJob(MRJobInfo mrJob);

	/**
	 * Finds and populates the map-reduce job with all available profiling
	 * information.
	 * 
	 * @param mrJob
	 *            the map reduce job
	 * @return true if the data were loaded successfully
	 */
	public boolean loadProfilesForMRJob(MRJobInfo mrJob);

}
