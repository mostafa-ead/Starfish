package edu.duke.starfish.profile.test;

import java.util.List;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import edu.duke.starfish.profile.profileinfo.metrics.Metric;
import edu.duke.starfish.profile.profileinfo.metrics.MetricType;

/**
 * A simple class to test the profile info classes and the TestInfoManager
 * 
 * @author hero
 */
public class TestClusterInfo {

	/**
	 * A simple main to test the profile info classes
	 * 
	 * @param args
	 *            command line arguments - none used
	 */
	public static void main(String[] args) {

		// Create a test managers
		IMRInfoManager manager = new TestInfoManager(2, 4, 3, 2, 30, 20);

		// Get a job
		List<MRJobInfo> jobs = manager.getAllMRJobInfos();
		MRJobInfo job = (MRJobInfo) jobs.get(0);
		if (manager.loadTaskDetailsForMRJob(job)) {
			TestUtils.printMRJobInfo(job);
		} else {
			System.out.println("Unable to find details for job "
					+ job.getExecId());
		}

		// Get a cluster configuration
		ClusterConfiguration cluster = manager.getClusterConfiguration(job
				.getExecId());
		TestUtils.printClusterConfiguration(cluster);

		// Print out the data transfer from a job
		System.out.println("DATA TRANSFERS");
		if (manager.loadDataTransfersForMRJob(job))
			for (DataTransfer dataTransfer : job.getDataTransfers()) {
				System.out.println(dataTransfer);
			}
		System.out.println();

		// Print out some metrics data
		System.out.println("CPU METRICS");
		for (Metric metric : manager.getHostMetrics(MetricType.CPU, cluster
				.getAllSlaveHostInfos().iterator().next(), job.getStartTime(),
				job.getEndTime())) {
			System.out.println(metric);
		}
		System.out.println();
	}

}
