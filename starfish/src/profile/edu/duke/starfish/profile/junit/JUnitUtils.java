package edu.duke.starfish.profile.junit;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * Utility methods for the JUnit tests
 * 
 * @author hero
 */
public class JUnitUtils {

	/**
	 * @return a cluster with 15 nodes with 2 map slots and 2 reduce slots each
	 */
	public static ClusterConfiguration getClusterConfiguration() {

		ClusterConfiguration cluster = new ClusterConfiguration();

		// Add task trackers
		TaskTrackerInfo taskTracker = null;
		for (int i = 0; i < 15; ++i) {
			taskTracker = cluster.addFindTaskTrackerInfo("tracker-" + i,
					"/rack/host-" + i);
			taskTracker.setNumMapSlots(2);
			taskTracker.setNumReduceSlots(2);
		}

		// Add job tracker
		cluster
				.addFindJobTrackerInfo("job_tracker",
						"/master-rack/master-host");

		return cluster;
	}

	/**
	 * @return a TeraSort job profile
	 */
	public static MRJobProfile getTeraSortJobProfile() {
		MRJobProfile prof = new MRJobProfile("job_201011062135_0003");

		String[] inputs = { "hdfs://hadoop21.cs.duke.edu:9000/usr/research/home/hero/tera/in" };
		prof.setJobInputs(inputs);

		prof.addMapProfile(getTeraSortMapProfile());
		prof.addReduceProfile(getTeraSortReduceProfile());
		prof.updateProfile();

		prof.addCounter(MRCounter.MAP_TASKS, 5l);
		prof.addCounter(MRCounter.REDUCE_TASKS, 1l);

		return prof;
	}

	/**
	 * @return a TeraSort map profile
	 */
	public static MRMapProfile getTeraSortMapProfile() {
		MRMapProfile prof = new MRMapProfile(
				"aggegated_map_0_job_201011062135_0003");
		prof.addCounter(MRCounter.MAP_INPUT_RECORDS, 200000l);
		prof.addCounter(MRCounter.MAP_INPUT_BYTES, 20000000l);
		prof.addCounter(MRCounter.MAP_OUTPUT_RECORDS, 200000l);
		prof.addCounter(MRCounter.MAP_OUTPUT_BYTES, 20000000l);
		prof.addCounter(MRCounter.MAP_NUM_SPILLS, 1l);
		prof.addCounter(MRCounter.MAP_NUM_SPILL_MERGES, 0l);
		prof.addCounter(MRCounter.MAP_RECS_PER_BUFF_SPILL, 200000l);
		prof.addCounter(MRCounter.MAP_SPILL_SIZE, 2945155l);
		prof.addCounter(MRCounter.COMBINE_INPUT_RECORDS, 0l);
		prof.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, 0l);
		prof.addCounter(MRCounter.SPILLED_RECORDS, 200000l);
		prof.addCounter(MRCounter.FILE_BYTES_READ, 129l);
		prof.addCounter(MRCounter.FILE_BYTES_WRITTEN, 2945187l);
		prof.addCounter(MRCounter.HDFS_BYTES_READ, 20000000l);

		prof.addStatistic(MRStatistics.INPUT_PAIR_WIDTH, 100.000000d);
		prof.addStatistic(MRStatistics.MAP_SIZE_SEL, 1.000000d);
		prof.addStatistic(MRStatistics.MAP_PAIRS_SEL, 1.000000d);
		prof.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO, 0.144370d);
		prof.addStatistic(MRStatistics.STARTUP_MEM, 5169355.200000d);
		prof.addStatistic(MRStatistics.SETUP_MEM, 0.000000d);
		prof.addStatistic(MRStatistics.MAP_MEM_PER_RECORD, 33.297768d);
		prof.addStatistic(MRStatistics.CLEANUP_MEM, 0.000000d);

		prof.addCostFactor(MRCostFactors.READ_HDFS_IO_COST, 91.704462d);
		prof.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST, 297.838161d);
		prof.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, 1138.331329d);
		prof.addCostFactor(MRCostFactors.MAP_CPU_COST, 12568.075065d);
		prof.addCostFactor(MRCostFactors.PARTITION_CPU_COST, 2580.230989d);
		prof.addCostFactor(MRCostFactors.SERDE_CPU_COST, 5420.882610d);
		prof.addCostFactor(MRCostFactors.SORT_CPU_COST, 329.777463d);
		prof.addCostFactor(MRCostFactors.MERGE_CPU_COST, 114.607982d);
		prof.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
				396.040195d);
		prof.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST, 317.733015d);
		prof.addCostFactor(MRCostFactors.SETUP_CPU_COST, 1696252.400000d);
		prof.addCostFactor(MRCostFactors.CLEANUP_CPU_COST, 232722.600000d);

		prof.addTiming(MRTaskPhase.SETUP, 1.696252d);
		prof.addTiming(MRTaskPhase.READ, 1834.089239d);
		prof.addTiming(MRTaskPhase.MAP, 2513.615013d);
		prof.addTiming(MRTaskPhase.COLLECT, 1600.222720d);
		prof.addTiming(MRTaskPhase.CLEANUP, 0.232723d);
		prof.addTiming(MRTaskPhase.SPILL, 10995.771881d);
		prof.addTiming(MRTaskPhase.MERGE, 3.791427d);

		return prof;
	}

	/**
	 * @return a TeraSort reduce profile
	 */
	public static MRReduceProfile getTeraSortReduceProfile() {
		MRReduceProfile prof = new MRReduceProfile(
				"aggegated_reduce_job_201011062135_0003");

		prof.addCounter(MRCounter.REDUCE_SHUFFLE_BYTES, 14725775l);
		prof.addCounter(MRCounter.REDUCE_INPUT_GROUPS, 1000000l);
		prof.addCounter(MRCounter.REDUCE_INPUT_RECORDS, 1000000l);
		prof.addCounter(MRCounter.REDUCE_INPUT_BYTES, 102000010l);
		prof.addCounter(MRCounter.REDUCE_OUTPUT_RECORDS, 1000000l);
		prof.addCounter(MRCounter.REDUCE_OUTPUT_BYTES, 100000000l);
		prof.addCounter(MRCounter.COMBINE_INPUT_RECORDS, 0l);
		prof.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, 0l);
		prof.addCounter(MRCounter.SPILLED_RECORDS, 1000000l);
		prof.addCounter(MRCounter.FILE_BYTES_READ, 14885588l);
		prof.addCounter(MRCounter.FILE_BYTES_WRITTEN, 14885588l);
		prof.addCounter(MRCounter.HDFS_BYTES_WRITTEN, 15075873l);

		prof.addStatistic(MRStatistics.REDUCE_PAIRS_PER_GROUP, 1.000000d);
		prof.addStatistic(MRStatistics.REDUCE_SIZE_SEL, 0.980392d);
		prof.addStatistic(MRStatistics.REDUCE_PAIRS_SEL, 1.000000d);
		prof.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO, 0.144370d);
		prof.addStatistic(MRStatistics.OUT_COMPRESS_RATIO, 0.150759d);
		prof.addStatistic(MRStatistics.STARTUP_MEM, 103362720.000000d);
		prof.addStatistic(MRStatistics.SETUP_MEM, 164848.000000d);
		prof.addStatistic(MRStatistics.REDUCE_MEM_PER_RECORD, 1.692152d);
		prof.addStatistic(MRStatistics.CLEANUP_MEM, 0.000000d);

		prof.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, 863.664383d);
		prof.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST, 297.838161d);
		prof.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, 1138.331329d);
		prof.addCostFactor(MRCostFactors.NETWORK_COST, 443.461224d);
		prof.addCostFactor(MRCostFactors.REDUCE_CPU_COST, 13630.213317d);
		prof.addCostFactor(MRCostFactors.MERGE_CPU_COST, 114.607982d);
		prof.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
				396.040195d);
		prof.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST, 323.935673d);
		prof.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST, 276.218865d);
		prof.addCostFactor(MRCostFactors.SETUP_CPU_COST, 1521152.000000d);
		prof.addCostFactor(MRCostFactors.CLEANUP_CPU_COST, 79730.000000d);

		prof.addTiming(MRTaskPhase.SHUFFLE, 12362.165792d);
		prof.addTiming(MRTaskPhase.SORT, 50120.369877d);
		prof.addTiming(MRTaskPhase.SETUP, 1.521152d);
		prof.addTiming(MRTaskPhase.REDUCE, 18016.111064d);
		prof.addTiming(MRTaskPhase.WRITE, 40642.381101d);
		prof.addTiming(MRTaskPhase.CLEANUP, 0.079730d);

		return prof;
	}

}
