package edu.duke.starfish.profile.test;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRCleanupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRCleanupInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRSetupInfo;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profileinfo.setup.RackInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * Utility methods for testing purposes
 * 
 * @author hero
 */
public class TestUtils {

	/**
	 * Print out the cluster configuration
	 * 
	 * @param cluster
	 *            the cluster configuration
	 */
	public static void printClusterConfiguration(ClusterConfiguration cluster) {
		System.out.println("Cluster:");

		for (RackInfo rack : cluster.getAllRackInfos()) {
			System.out.println("\tRack=" + rack.getName());
			if (rack.getMasterHost() != null) {
				System.out.println("\t\t" + rack.getMasterHost());
			}

			for (HostInfo host : rack.getSlaveHosts()) {
				System.out.println("\t\t" + host);
			}
		}

		System.out.println("\t" + cluster.getJobTrackerInfo());

		for (TaskTrackerInfo tracker : cluster.getAllTaskTrackersInfos()) {
			System.out.println("\t" + tracker);
		}

		System.out.println();
	}

	/**
	 * Print out a MR Job
	 * 
	 * @param job
	 *            the map reduce job
	 */
	public static void printMRJobInfo(MRJobInfo job) {
		System.out.println(job);

		for (MRSetupInfo task : job.getSetupTasks()) {
			System.out.println("\t" + task);
			for (MRSetupAttemptInfo attempt : task.getAttempts())
				System.out.println("\t\t" + attempt);
		}
		for (MRMapInfo task : job.getMapTasks()) {
			System.out.println("\t" + task);
			for (MRMapAttemptInfo attempt : task.getAttempts())
				System.out.println("\t\t" + attempt);
		}
		for (MRReduceInfo task : job.getReduceTasks()) {
			System.out.println("\t" + task);
			for (MRReduceAttemptInfo attempt : task.getAttempts())
				System.out.println("\t\t" + attempt);
		}
		for (MRCleanupInfo task : job.getCleanupTasks()) {
			System.out.println("\t" + task);
			for (MRCleanupAttemptInfo attempt : task.getAttempts())
				System.out.println("\t\t" + attempt);
		}
		System.out.println();
	}

	/**
	 * Prints the data transfers from this job
	 * 
	 * @param job
	 *            the job
	 */
	public static void printMRJobDataTransfers(MRJobInfo job) {
		System.out.println("Data Transfers for " + job.getExecId());

		for (DataTransfer dataTransfer : job.getDataTransfers()) {
			System.out.println(dataTransfer.toString());
		}
	}
}
