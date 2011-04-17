package edu.duke.starfish.whatif.junit;

import org.apache.hadoop.conf.Configuration;

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
 * Contains functions for obtaining some sample job profiles obtained from
 * actual job executions.
 * 
 * @author hero
 */
public class SampleProfiles {

	/**
	 * @return a cluster with 15 nodes with 2 map slots and 2 reduce slots each
	 */
	public static ClusterConfiguration getClusterConfiguration() {

		ClusterConfiguration cluster = new ClusterConfiguration();

		TaskTrackerInfo taskTracker = null;
		for (int i = 0; i < 15; ++i) {
			taskTracker = cluster.addFindTaskTrackerInfo("tracker-" + i,
					"/rack/host-" + i);
			taskTracker.setNumMapSlots(2);
			taskTracker.setNumReduceSlots(2);
		}

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
		prof.setInputIndex(0);
		prof.setNumTasks(5);
		
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

	/**
	 * @return a TeraSort configuration
	 */
	public static Configuration getTeraSortConfiguration() {
		Configuration conf = new Configuration(false);

		conf.set("hadoop.tmp.dir", "/local/cps216/TMP_DIR");
		conf.set("map.sort.class", "org.apache.hadoop.util.QuickSort");
		conf.set("mapred.system.dir", "${hadoop.tmp.dir}/mapred/system");
		conf.set("dfs.datanode.address", "0.0.0.0:50010");
		conf.set("io.skip.checksum.errors", "false");
		conf.set("fs.default.name", "hdfs://hadoop21.cs.duke.edu:9000");
		conf.set("mapred.reducer.new-api", "true");
		conf.set("mapred.child.tmp", "./tmp");
		conf.set("mapred.skip.reduce.max.skip.groups", "0");
		conf.set("io.sort.factor", "10");
		conf.set("mapred.task.timeout", "600000");
		conf.set("mapred.max.tracker.failures", "4");
		conf.set("mapred.output.key.class", "org.apache.hadoop.io.Text");
		conf.set("mapred.skip.map.auto.incr.proc.count", "true");
		conf.set("io.mapfile.bloom.size", "1048576");
		conf.set("tasktracker.http.threads", "40");
		conf.set("mapred.job.shuffle.merge.percent", "0.66");
		conf.set("mapreduce.inputformat.class",
				"org.apache.hadoop.examples.terasort.TeraInputFormat");
		conf.set("user.name", "hero");
		conf.set("mapred.output.compress", "true");
		conf.set("io.bytes.per.checksum", "512");
		conf.set("btrace.profile.dir", "/local/hero/hadoop-btrace");
		conf.set("mapred.reduce.slowstart.completed.maps", "0.05");
		conf.set("mapred.reduce.max.attempts", "4");
		conf.set("mapred.skip.map.max.skip.records", "0");
		conf.set("dfs.block.size", "67108864");
		conf.set("mapred.output.compression.type", "RECORD");
		conf.set("dfs.permissions", "true");
		conf.set("mapred.task.profile.maps", "0-9999");
		conf.set("local.cache.size", "10737418240");
		conf.set("mapred.min.split.size", "0");
		conf.set("mapred.map.tasks", "5");
		conf.set("mapred.child.java.opts", "-Xmx300m");
		conf.set("mapred.output.value.class", "org.apache.hadoop.io.Text");
		conf.set("mapred.inmem.merge.threshold", "1000");
		conf.set("mapred.reduce.tasks", "1");
		conf.set("io.sort.spill.percent", "0.80");
		conf.set("mapred.job.shuffle.input.buffer.percent", "0.70");
		conf.set("mapred.job.name", "TeraSort");
		conf.set("mapred.map.tasks.speculative.execution", "true");
		conf.set("mapred.map.max.attempts", "4");
		conf.set("mapred.job.tracker.handler.count", "10");
		conf.set("mapred.jobtracker.maxtasks.per.job", "-1");
		conf.set("mapred.jobtracker.job.history.block.size", "3145728");
		conf.set("mapreduce.outputformat.class",
				"org.apache.hadoop.examples.terasort.TeraOutputFormat");
		conf.set("mapred.task.profile.reduces", "0-9999");
		conf.set("mapred.output.compression.codec",
				"org.apache.hadoop.io.compress.DefaultCodec");
		conf.set("io.map.index.skip", "0");
		conf.set("mapred.working.dir",
				"hdfs://hadoop21.cs.duke.edu:9000/user/hero");
		conf.set("mapred.used.genericoptionsparser", "true");
		conf.set("mapred.mapper.new-api", "true");
		conf.set("min.num.spills.for.combine", "9999");
		conf.set("hadoop.logfile.size", "10000000");
		conf.set("mapred.reduce.tasks.speculative.execution", "true");
		conf.set("fs.checkpoint.period", "3600");
		conf.set("mapred.job.reuse.jvm.num.tasks", "1");
		conf.set("dfs.df.interval", "60000");
		conf.set("dfs.data.dir", "/local/cps216/DATA_DIR");
		conf.set("dfs.replication.min", "1");
		conf.set("mapred.submit.replication", "10");
		conf.set("mapred.map.output.compression.codec",
				"org.apache.hadoop.io.compress.DefaultCodec");
		conf.set("dfs.heartbeat.interval", "3");
		conf.set("mapred.job.tracker", "hadoop21.cs.duke.edu:9100");
		conf.set("io.seqfile.sorter.recordlimit", "1000000");
		conf.set("dfs.name.dir", "/local/cps216/NAME_DIR");
		conf.set("mapred.line.input.format.linespermap", "1");
		conf.set("mapred.create.symlink", "yes");
		conf
				.set("mapreduce.partitioner.class",
						"org.apache.hadoop.examples.terasort.TeraSort$TotalOrderPartitioner");
		conf.set("io.sort.record.percent", "0.05");
		conf.set("mapred.tasktracker.reduce.tasks.maximum", "2");
		conf.set("dfs.replication", "1");
		conf.set("mapred.job.reduce.input.buffer.percent", "0.0");
		conf.set("mapred.tasktracker.indexcache.mb", "10");
		conf.set("hadoop.logfile.count", "10");
		conf.set("terasort.final.sync", "true");
		conf.set("io.seqfile.compress.blocksize", "1000000");
		conf.set("fs.s3.block.size", "67108864");
		conf.set("dfs.access.time.precision", "3600000");
		conf.set("mapred.reduce.parallel.copies", "5");
		conf.set("io.seqfile.lazydecompress", "true");
		conf.set("mapred.output.dir", "/usr/research/home/hero/tera/out");
		conf.set("io.sort.mb", "100");
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.input.dir", "hdfs://hadoop21.cs.duke.edu:9000"
				+ "/usr/research/home/hero/tera/in");
		conf.set("io.file.buffer.size", "4096");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.WritableSerialization");
		conf.set("dfs.datanode.handler.count", "3");
		conf.set("mapred.reduce.copy.backoff", "300");
		conf.set("mapred.task.profile", "true");
		conf.set("mapred.tasktracker.map.tasks.maximum", "2");
		conf.set("fs.checkpoint.size", "67108864");

		return conf;
	}

	/**
	 * @return a WordCount job profile
	 */
	public static MRJobProfile getWordCountJobProfile() {
		MRJobProfile prof = new MRJobProfile("job_201011062135_0002");

		String[] inputs = { "hdfs://hadoop21.cs.duke.edu:9000/usr/research/home/hero/wordcount/in" };
		prof.setJobInputs(inputs);

		prof.addMapProfile(getWordCountMapProfile());
		prof.addReduceProfile(getWordCountReduceProfile());
		prof.updateProfile();

		prof.addCounter(MRCounter.MAP_TASKS, 15l);
		prof.addCounter(MRCounter.REDUCE_TASKS, 1l);

		return prof;
	}

	/**
	 * @return a WordCount map profile
	 */
	public static MRMapProfile getWordCountMapProfile() {
		MRMapProfile prof = new MRMapProfile(
				"aggegated_map_0_job_201011062135_0002");
		prof.setInputIndex(0);
		prof.setNumTasks(15);
		
		prof.addCounter(MRCounter.MAP_INPUT_RECORDS, 140580l);
		prof.addCounter(MRCounter.MAP_INPUT_BYTES, 21252750l);
		prof.addCounter(MRCounter.MAP_OUTPUT_RECORDS, 1968316l);
		prof.addCounter(MRCounter.MAP_OUTPUT_BYTES, 28844854l);
		prof.addCounter(MRCounter.MAP_NUM_SPILLS, 8l);
		prof.addCounter(MRCounter.MAP_NUM_SPILL_MERGES, 1l);
		prof.addCounter(MRCounter.MAP_RECS_PER_BUFF_SPILL, 246039l);
		prof.addCounter(MRCounter.MAP_SPILL_SIZE, 8319l);
		prof.addCounter(MRCounter.COMBINE_INPUT_RECORDS, 1968316l);
		prof.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, 8000l);
		prof.addCounter(MRCounter.SPILLED_RECORDS, 16000l);
		prof.addCounter(MRCounter.FILE_BYTES_READ, 66561l);
		prof.addCounter(MRCounter.FILE_BYTES_WRITTEN, 94871l);
		prof.addCounter(MRCounter.HDFS_BYTES_READ, 21252750l);

		prof.addStatistic(MRStatistics.INPUT_PAIR_WIDTH, 151.178134d);
		prof.addStatistic(MRStatistics.MAP_SIZE_SEL, 1.357229d);
		prof.addStatistic(MRStatistics.MAP_PAIRS_SEL, 14.001312d);
		prof.addStatistic(MRStatistics.COMBINE_SIZE_SEL, 0.004620d);
		prof.addStatistic(MRStatistics.COMBINE_PAIRS_SEL, 0.004064d);
		prof.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO, 0.499500d);
		prof.addStatistic(MRStatistics.STARTUP_MEM, 3358142.933333d);
		prof.addStatistic(MRStatistics.SETUP_MEM, 3.200000d);
		prof.addStatistic(MRStatistics.MAP_MEM_PER_RECORD, 62.370606d);
		prof.addStatistic(MRStatistics.CLEANUP_MEM, 0.000000d);

		prof.addCostFactor(MRCostFactors.READ_HDFS_IO_COST, 82.316862d);
		prof.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST, 30.440431d);
		prof.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, 4395.655327d);
		prof.addCostFactor(MRCostFactors.MAP_CPU_COST, 75491.663129d);
		prof.addCostFactor(MRCostFactors.COMBINE_CPU_COST, 2418.005863d);
		prof.addCostFactor(MRCostFactors.PARTITION_CPU_COST, 2971.361980d);
		prof.addCostFactor(MRCostFactors.SERDE_CPU_COST, 6490.886423d);
		prof.addCostFactor(MRCostFactors.SORT_CPU_COST, 257.609818d);
		prof.addCostFactor(MRCostFactors.MERGE_CPU_COST, 7413.329117d);
		prof
				.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
						71.797985d);
		prof
				.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST,
						2968.580332d);
		prof.addCostFactor(MRCostFactors.SETUP_CPU_COST, 1739035.666667d);
		prof.addCostFactor(MRCostFactors.CLEANUP_CPU_COST, 213897.400000d);

		prof.addTiming(MRTaskPhase.SETUP, 1.739036d);
		prof.addTiming(MRTaskPhase.READ, 1749.459504d);
		prof.addTiming(MRTaskPhase.MAP, 10612.393036d);
		prof.addTiming(MRTaskPhase.COLLECT, 18624.622090d);
		prof.addTiming(MRTaskPhase.CLEANUP, 0.213897d);
		prof.addTiming(MRTaskPhase.SPILL, 15470.004914d);
		prof.addTiming(MRTaskPhase.MERGE, 469.370330d);

		return prof;
	}

	/**
	 * @return a WordCount reduce profile
	 */
	public static MRReduceProfile getWordCountReduceProfile() {
		MRReduceProfile prof = new MRReduceProfile(
				"aggegated_reduce_job_201011062135_0002");

		prof.addCounter(MRCounter.REDUCE_SHUFFLE_BYTES, 424171l);
		prof.addCounter(MRCounter.REDUCE_INPUT_GROUPS, 1000l);
		prof.addCounter(MRCounter.REDUCE_INPUT_RECORDS, 120000l);
		prof.addCounter(MRCounter.REDUCE_INPUT_BYTES, 1998630l);
		prof.addCounter(MRCounter.REDUCE_OUTPUT_RECORDS, 1000l);
		prof.addCounter(MRCounter.REDUCE_OUTPUT_BYTES, 16655l);
		prof.addCounter(MRCounter.COMBINE_INPUT_RECORDS, 0l);
		prof.addCounter(MRCounter.COMBINE_OUTPUT_RECORDS, 0l);
		prof.addCounter(MRCounter.SPILLED_RECORDS, 120000l);
		prof.addCounter(MRCounter.FILE_BYTES_READ, 268317l);
		prof.addCounter(MRCounter.FILE_BYTES_WRITTEN, 268317l);
		prof.addCounter(MRCounter.HDFS_BYTES_WRITTEN, 8040l);

		prof.addStatistic(MRStatistics.REDUCE_PAIRS_PER_GROUP, 120.000000d);
		prof.addStatistic(MRStatistics.REDUCE_SIZE_SEL, 0.008333d);
		prof.addStatistic(MRStatistics.REDUCE_PAIRS_SEL, 0.008333d);
		prof.addStatistic(MRStatistics.COMBINE_SIZE_SEL, 0.004620d);
		prof.addStatistic(MRStatistics.COMBINE_PAIRS_SEL, 0.004064d);
		prof.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO, 0.212231d);
		prof.addStatistic(MRStatistics.OUT_COMPRESS_RATIO, 0.482738d);
		prof.addStatistic(MRStatistics.STARTUP_MEM, 23495472.000000d);
		prof.addStatistic(MRStatistics.SETUP_MEM, 48.000000d);
		prof.addStatistic(MRStatistics.REDUCE_MEM_PER_RECORD, 9.043267d);
		prof.addStatistic(MRStatistics.CLEANUP_MEM, 0.000000d);

		prof.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST, 34382.508955d);
		prof.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST, 30.440431d);
		prof.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST, 4395.655327d);
		prof.addCostFactor(MRCostFactors.NETWORK_COST, 2090.023125d);
		prof.addCostFactor(MRCostFactors.REDUCE_CPU_COST, 4418.356650d);
		prof.addCostFactor(MRCostFactors.COMBINE_CPU_COST, 2418.005863d);
		prof.addCostFactor(MRCostFactors.MERGE_CPU_COST, 1333.549017d);
		prof.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
				172.214225d);
		prof.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST, 998.344163d);
		prof
				.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
						1836.707235d);
		prof.addCostFactor(MRCostFactors.SETUP_CPU_COST, 1382537.000000d);
		prof.addCostFactor(MRCostFactors.CLEANUP_CPU_COST, 80402.000000d);

		prof.addTiming(MRTaskPhase.SHUFFLE, 959.433431d);
		prof.addTiming(MRTaskPhase.SORT, 3967.093395d);
		prof.addTiming(MRTaskPhase.SETUP, 1.382537d);
		prof.addTiming(MRTaskPhase.REDUCE, 543.114746d);
		prof.addTiming(MRTaskPhase.WRITE, 307.025731d);
		prof.addTiming(MRTaskPhase.CLEANUP, 0.080402d);

		return prof;
	}

	/**
	 * @return a WordCount configuration
	 */
	public static Configuration getWordCountConfiguration() {
		Configuration conf = new Configuration(false);
		conf.set("hadoop.tmp.dir", "/local/cps216/TMP_DIR");
		conf.set("map.sort.class", "org.apache.hadoop.util.QuickSort");
		conf.set("dfs.namenode.decommission.nodes.per.interval", "5");
		conf.set("dfs.https.need.client.auth", "false");
		conf.set("ipc.client.idlethreshold", "4000");
		conf.set("mapred.system.dir", "${hadoop.tmp.dir}/mapred/system");
		conf.set("mapred.job.tracker.persist.jobstatus.hours", "0");
		conf.set("dfs.namenode.logging.level", "info");
		conf.set("io.skip.checksum.errors", "false");
		conf.set("mapred.reducer.new-api", "true");
		conf.set("fs.har.impl.disable.cache", "true");
		conf.set("dfs.safemode.threshold.pct", "0.999f");
		conf.set("mapred.skip.reduce.max.skip.groups", "0");
		conf.set("dfs.namenode.handler.count", "10");
		conf.set("dfs.blockreport.initialDelay", "0");
		conf.set("io.sort.factor", "10");
		conf.set("mapred.task.timeout", "600000");
		conf.set("mapred.max.tracker.failures", "4");
		conf.set("mapred.output.key.class", "org.apache.hadoop.io.Text");
		conf.set("mapred.skip.map.auto.incr.proc.count", "true");
		conf.set("io.mapfile.bloom.size", "1048576");
		conf.set("dfs.safemode.extension", "30000");
		conf.set("tasktracker.http.threads", "40");
		conf.set("mapred.job.shuffle.merge.percent", "0.66");
		conf.set("user.name", "hero");
		conf.set("mapred.output.compress", "true");
		conf.set("io.bytes.per.checksum", "512");
		conf.set("btrace.profile.dir", "/local/hero/hadoop-btrace");
		conf.set("mapred.reduce.slowstart.completed.maps", "0.05");
		conf.set("mapred.reduce.max.attempts", "4");
		conf.set("dfs.name.edits.dir", "${dfs.name.dir}");
		conf.set("mapred.skip.map.max.skip.records", "0");
		conf.set("hadoop.job.ugi", "hero,grad,oldissg,issg");
		conf.set("dfs.block.size", "67108864");
		conf.set("job.end.retry.attempts", "0");
		conf.set("mapred.local.dir.minspacestart", "0");
		conf.set("mapred.output.compression.type", "RECORD");
		conf.set("dfs.permissions", "true");
		conf.set("topology.script.number.args", "100");
		conf.set("io.mapfile.bloom.error.rate", "0.005");
		conf.set("mapred.max.tracker.blacklists", "4");
		conf.set("mapred.task.profile.maps", "0-9999");
		conf.set("mapred.userlog.retain.hours", "24");
		conf.set("dfs.replication.max", "512");
		conf.set("mapred.job.tracker.persist.jobstatus.active", "false");
		conf.set("hadoop.security.authorization", "false");
		conf.set("local.cache.size", "10737418240");
		conf.set("mapred.min.split.size", "0");
		conf.set("mapred.map.tasks", "15");
		conf.set("mapred.child.java.opts", "-Xmx300m");
		conf.set("mapred.output.value.class",
				"org.apache.hadoop.io.IntWritable");
		conf.set("dfs.balance.bandwidthPerSec", "1048576");
		conf.set("ipc.server.listen.queue.size", "128");
		conf.set("mapred.inmem.merge.threshold", "1000");
		conf.set("job.end.retry.interval", "30000");
		conf.set("mapred.skip.attempts.to.start.skipping", "2");
		conf.set("mapred.reduce.tasks", "1");
		conf.set("mapred.merge.recordsBeforeProgress", "10000");
		conf.set("mapred.userlog.limit.kb", "0");
		conf.set("dfs.max.objects", "0");
		conf.set("webinterface.private.actions", "false");
		conf.set("io.sort.spill.percent", "0.80");
		conf.set("mapred.job.shuffle.input.buffer.percent", "0.70");
		conf.set("mapred.job.name", "word count");
		conf.set("mapred.map.tasks.speculative.execution", "true");
		conf.set("dfs.blockreport.intervalMsec", "3600000");
		conf.set("mapred.map.max.attempts", "4");
		conf.set("dfs.client.block.write.retries", "3");
		conf.set("mapred.job.tracker.handler.count", "10");
		conf.set("mapred.tasktracker.expiry.interval", "600000");
		conf.set("mapred.jobtracker.maxtasks.per.job", "-1");
		conf.set("mapred.jobtracker.job.history.block.size", "3145728");
		conf.set("keep.failed.task.files", "false");
		conf.set("ipc.client.tcpnodelay", "false");
		conf.set("mapred.task.profile.reduces", "0-9999");
		conf.set("mapred.output.compression.codec",
				"org.apache.hadoop.io.compress.DefaultCodec");
		conf.set("io.map.index.skip", "0");
		conf.set("mapred.working.dir",
				"hdfs://hadoop21.cs.duke.edu:9000/user/hero");
		conf.set("mapred.used.genericoptionsparser", "true");
		conf.set("mapred.mapper.new-api", "true");
		conf.set("min.num.spills.for.combine", "9999");
		conf.set("dfs.default.chunk.view.size", "32768");
		conf.set("hadoop.logfile.size", "10000000");
		conf.set("mapred.reduce.tasks.speculative.execution", "true");
		conf.set("dfs.datanode.du.reserved", "0");
		conf.set("fs.checkpoint.period", "3600");
		conf.set("dfs.web.ugi", "webuser,webgroup");
		conf.set("mapred.job.reuse.jvm.num.tasks", "1");
		conf.set("mapred.jobtracker.completeuserjobs.maximum", "100");
		conf.set("dfs.df.interval", "60000");
		conf.set("dfs.data.dir", "/local/cps216/DATA_DIR");
		conf.set("fs.s3.maxRetries", "4");
		conf.set("dfs.datanode.dns.interface", "default");
		conf.set("dfs.support.append", "false");
		conf.set("dfs.permissions.supergroup", "supergroup");
		conf.set("mapred.local.dir", "${hadoop.tmp.dir}/mapred/local");
		conf.set("dfs.replication.min", "1");
		conf.set("mapred.submit.replication", "10");
		conf.set("mapred.map.output.compression.codec",
				"org.apache.hadoop.io.compress.DefaultCodec");
		conf.set("mapred.tasktracker.dns.interface", "default");
		conf.set("dfs.namenode.decommission.interval", "30");
		conf.set("dfs.heartbeat.interval", "3");
		conf.set("mapred.job.tracker", "hadoop21.cs.duke.edu:9100");
		conf.set("io.seqfile.sorter.recordlimit", "1000000");
		conf.set("mapred.line.input.format.linespermap", "1");
		conf.set("mapred.local.dir.minspacekill", "0");
		conf.set("dfs.replication.interval", "3");
		conf.set("io.sort.record.percent", "0.05");
		conf.set("mapreduce.reduce.class",
				"org.apache.hadoop.examples.WordCount$IntSumReducer");
		conf.set("mapred.temp.dir", "${hadoop.tmp.dir}/mapred/temp");
		conf.set("mapred.tasktracker.reduce.tasks.maximum", "2");
		conf.set("dfs.replication", "2");
		conf.set("fs.checkpoint.edits.dir", "${fs.checkpoint.dir}");
		conf.set("mapred.job.reduce.input.buffer.percent", "0.0");
		conf.set("mapred.tasktracker.indexcache.mb", "10");
		conf.set("hadoop.logfile.count", "10");
		conf.set("mapred.skip.reduce.auto.incr.proc.count", "true");
		conf.set("io.seqfile.compress.blocksize", "1000000");
		conf.set("fs.s3.block.size", "67108864");
		conf.set("mapred.acls.enabled", "false");
		conf.set("mapred.queue.names", "default");
		conf.set("dfs.access.time.precision", "3600000");
		conf.set("mapreduce.combine.class",
				"org.apache.hadoop.examples.WordCount$IntSumReducer");
		conf.set("mapred.reduce.parallel.copies", "5");
		conf.set("io.seqfile.lazydecompress", "true");
		conf.set("mapred.output.dir", "/usr/research/"
				+ "home/hero/wordcount/out");
		conf.set("io.sort.mb", "100");
		conf.set("ipc.client.connection.maxidletime", "10000");
		conf.set("mapred.compress.map.output", "true");
		conf.set("ipc.client.kill.max", "10");
		conf.set("ipc.client.connect.max.retries", "10");
		conf.set("mapreduce.map.class",
				"org.apache.hadoop.examples.WordCount$TokenizerMapper");
		conf.set("mapred.input.dir", "hdfs://hadoop21.cs."
				+ "duke.edu:9000/usr/research/home/hero/wordcount/in");
		conf.set("io.file.buffer.size", "4096");
		conf.set("mapred.jobtracker.restart.recover", "false");
		conf.set("dfs.datanode.handler.count", "3");
		conf.set("mapred.reduce.copy.backoff", "300");
		conf.set("mapred.task.profile", "true");
		conf.set("dfs.replication.considerLoad", "true");
		conf.set("mapred.tasktracker.map.tasks.maximum", "2");
		conf.set("fs.checkpoint.size", "67108864");

		return conf;
	}

}
