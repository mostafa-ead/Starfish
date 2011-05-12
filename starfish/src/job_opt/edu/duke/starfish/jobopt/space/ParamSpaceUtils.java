package edu.duke.starfish.jobopt.space;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_RED_TASKS;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_COMBINE_CLASS;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.jobopt.params.BooleanParamDescriptor;
import edu.duke.starfish.jobopt.params.DoubleParamDescriptor;
import edu.duke.starfish.jobopt.params.HadoopParameter;
import edu.duke.starfish.jobopt.params.IntegerParamDescriptor;
import edu.duke.starfish.jobopt.params.ListParamDescriptor;
import edu.duke.starfish.jobopt.params.ParamTaskEffect;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;
import edu.duke.starfish.whatif.WhatIfUtils;

/**
 * This class contains several utility methods for the Parameter Space.
 * 
 * @author hero
 */
public class ParamSpaceUtils {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Constants
	private static final long MIN_SORT_MB = 20971520l;
	private static final float MAX_MEM_RATIO = 0.75f;

	public static final String EXCLUDE_PARAMS = "starfish.job.optimizer.exclude.parameters";

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Adjusts the domain of some parameter descriptors based on information
	 * from the cluster, the configuration, and the virtual job profile.
	 * Currently, the parameters adjusted are:
	 * <ul>
	 * <li>io.sort.mb</li>
	 * <li>mapred.job.reduce.input.buffer.percent</li>
	 * <li>mapred.reduce.tasks</li>
	 * </ul>
	 * 
	 * @param space
	 *            the parameter space
	 * @param cluster
	 *            the cluster
	 * @param conf
	 *            the configuration
	 * @param jobProfile
	 *            the virtual job profile
	 */
	public static void adjustParameterDescriptors(ParameterSpace space,
			ClusterConfiguration cluster, Configuration conf,
			MRJobProfile jobProfile) {

		long taskMemory = ProfileUtils.getTaskMemory(conf);

		// Adjust the max value of io.sort.mb
		if (space.containsParamDescriptor(HadoopParameter.SORT_MB)) {

			// Find the memory required by the map tasks
			long mapMemory = 0l;
			for (MRMapProfile mapProfile : jobProfile.getAvgMapProfiles()) {
				mapMemory += WhatIfUtils.getMapMemoryRequired(mapProfile);
			}
			mapMemory /= jobProfile.getAvgMapProfiles().size();

			// Set the memory left for io.sort.mb
			long ioSortMem = taskMemory - mapMemory;
			if (ioSortMem > (long) (MAX_MEM_RATIO * taskMemory))
				ioSortMem = (long) (MAX_MEM_RATIO * taskMemory);
			if (ioSortMem < MIN_SORT_MB)
				ioSortMem = MIN_SORT_MB;

			((IntegerParamDescriptor) space
					.getParameterDescriptor(HadoopParameter.SORT_MB))
					.setMaxValue((int) (ioSortMem >> 20));
		}

		// Adjust the max value of mapred.job.reduce.input.buffer.percent
		if (space.containsParamDescriptor(HadoopParameter.RED_IN_BUFF_PERC)) {

			// Find the memory required by the reduce tasks
			long redMemory = WhatIfUtils.getReduceMemoryRequired(jobProfile
					.getAvgReduceProfile());

			// Calculate the percent of memory to be used to buffer input
			double percent = (taskMemory - redMemory) / (double) taskMemory;
			if (percent < 0.0)
				percent = 0;
			else if (percent > 0.8)
				percent = 0.8;

			((DoubleParamDescriptor) space
					.getParameterDescriptor(HadoopParameter.RED_IN_BUFF_PERC))
					.setMaxValue(percent);
		}

		// Adjust the min and max number of mapred.reduce.tasks
		MRReduceProfile redProfile = jobProfile.getAvgReduceProfile();
		if (space.containsParamDescriptor(HadoopParameter.RED_TASKS)
				&& redProfile != null) {

			// Calculate the (uncompressed) reduce input size
			double shuffleSize = redProfile.getNumTasks()
					* redProfile.getCounter(MRCounter.REDUCE_SHUFFLE_BYTES)
					/ redProfile.getStatistic(
							MRStatistics.INTERM_COMPRESS_RATIO, 1d);

			// Calculate the number of reduce groups
			long numGroups = redProfile.getNumTasks()
					* redProfile.getCounter(MRCounter.REDUCE_INPUT_GROUPS, 1l);

			// Calculate the number of reduce slots
			int numRedSlots = 0;
			for (TaskTrackerInfo taskTracker : cluster
					.getAllTaskTrackersInfos()) {
				numRedSlots += taskTracker.getNumReduceSlots();
			}

			// Calculate the min and max number of reducers
			double min = Math.ceil(shuffleSize / (2 * taskMemory));
			double max = Math.ceil(4 * shuffleSize / taskMemory);
			max = Math.min(max, numGroups);
			max = Math.max(max, numRedSlots);
			if (max < min)
				max = min;

			// Set the min and max number of reducers
			space.addParameterDescriptor(new IntegerParamDescriptor(
					HadoopParameter.RED_TASKS, ParamTaskEffect.EFFECT_REDUCE,
					(int) min, (int) max));
		}

	}

	/**
	 * Add a parameter to the excluded parameters list in the conf
	 * 
	 * @param conf
	 *            the configuration
	 * @param param
	 *            the parameter to exclude
	 */
	public static void addExludedParameter(Configuration conf, String param) {
		String exclude = conf.get(EXCLUDE_PARAMS);
		if (exclude == null || exclude.equals(""))
			exclude = param;
		else
			exclude += "," + param;
		conf.set(EXCLUDE_PARAMS, exclude);
	}

	/**
	 * Exclude all map-side parameters from the optimization space
	 * 
	 * @param conf
	 *            the configuration
	 */
	public static void excludeMapSideParams(Configuration conf) {

		String exclude = "io.sort.mb,io.sort.spill.percent,"
				+ "io.sort.record.percent,min.num.spills.for.combine";

		conf.set(EXCLUDE_PARAMS, exclude);
	}

	/**
	 * Exclude all reduce-side parameters from the optimization space
	 * 
	 * @param conf
	 *            the configuration
	 */
	public static void excludeReduceSideParams(Configuration conf) {

		String exclude = "mapred.reduce.tasks,"
				+ "mapred.inmem.merge.threshold,"
				+ "mapred.job.shuffle.input.buffer.percent,"
				+ "mapred.job.shuffle.merge.percent,"
				+ "mapred.job.reduce.input.buffer.percent,"
				+ "mapred.reduce.slowstart.completed.maps,"
				+ "mapred.output.compress";

		conf.set(EXCLUDE_PARAMS, exclude);
	}

	/**
	 * Returns a parameter space with all the parameter descriptors
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the parameter space
	 */
	public static ParameterSpace getFullParamSpace(Configuration conf) {

		Set<String> exclude = buildParamExclusionSet(conf);

		// Check for map-only job
		if (conf.getInt(MR_RED_TASKS, 1) == 0)
			return getMapOnlyParamSpace(conf, exclude);

		// Populate the full space
		ParameterSpace space = new ParameterSpace();

		addEffectMapParameters(space, conf, exclude);
		addEffectReduceParameters(space, conf, exclude);
		addEffectBothParameters(space, conf, exclude);
		addEffectNoneParameters(space, conf, exclude);

		return space;
	}

	/**
	 * Returns a parameter space with all the parameter descriptors that can
	 * effect the map task execution
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the parameter space
	 */
	public static ParameterSpace getParamSpaceForMappers(Configuration conf) {

		Set<String> exclude = buildParamExclusionSet(conf);

		// Check for map-only job
		if (conf.getInt(MR_RED_TASKS, 1) == 0)
			return getMapOnlyParamSpace(conf, exclude);

		// Populate the space
		ParameterSpace space = new ParameterSpace();

		addEffectMapParameters(space, conf, exclude);
		addEffectBothParameters(space, conf, exclude);

		return space;
	}

	/**
	 * Returns a parameter space with all the parameter descriptors that can
	 * effect the reduce task execution
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the parameter space
	 */
	public static ParameterSpace getParamSpaceForReducers(Configuration conf) {

		Set<String> exclude = buildParamExclusionSet(conf);

		// Check for map-only job
		ParameterSpace space = new ParameterSpace();
		if (conf.getInt(MR_RED_TASKS, 1) == 0)
			return space;

		// Populate the space
		addEffectReduceParameters(space, conf, exclude);
		addEffectBothParameters(space, conf, exclude);

		return space;
	}

	/**
	 * Returns a parameter space with all the parameter descriptors that can
	 * effect the execution of the next MapReduce job (currently, the number of
	 * reducers and output compression)
	 * 
	 * @param conf
	 *            the job configuration
	 * @return the parameter space
	 */
	public static ParameterSpace getParamSpaceForNextJob(Configuration conf) {

		Set<String> exclude = buildParamExclusionSet(conf);
		ParameterSpace space = new ParameterSpace();

		if (conf.getInt(MR_RED_TASKS, 1) != 0
				&& !exclude.contains(HadoopParameter.RED_TASKS.toString()))
			space.addParameterDescriptor(new IntegerParamDescriptor(
					HadoopParameter.RED_TASKS, ParamTaskEffect.EFFECT_REDUCE,
					1, 100));

		if (!exclude.contains(HadoopParameter.COMPRESS_MAP_OUT.toString()))
			space.addParameterDescriptor(new BooleanParamDescriptor(
					HadoopParameter.COMPRESS_MAP_OUT,
					ParamTaskEffect.EFFECT_BOTH));

		return space;
	}

	/* ***************************************************************
	 * PRIVATE STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add the map-related parameters into the space, except the ones in the
	 * excluded set
	 * 
	 * @param space
	 *            the parameter space
	 * @param conf
	 *            the configuration
	 * @param exclude
	 *            the exclusion set
	 */
	private static void addEffectMapParameters(ParameterSpace space,
			Configuration conf, Set<String> exclude) {

		// Get the maximum memory
		long maxMem = (long) (MAX_MEM_RATIO * ProfileUtils.getTaskMemory(conf));
		if (maxMem < MIN_SORT_MB)
			maxMem = MIN_SORT_MB;

		// Add parameters that effect the map tasks
		if (!exclude.contains(HadoopParameter.SORT_MB.toString()))
			space.addParameterDescriptor(new IntegerParamDescriptor(
					HadoopParameter.SORT_MB, ParamTaskEffect.EFFECT_MAP,
					(int) (MIN_SORT_MB >> 20), (int) (maxMem >> 20)));
		if (!exclude.contains(HadoopParameter.SPILL_PERC.toString()))
			space.addParameterDescriptor(new DoubleParamDescriptor(
					HadoopParameter.SPILL_PERC, ParamTaskEffect.EFFECT_MAP,
					0.2, 0.9));
		if (!exclude.contains(HadoopParameter.SORT_REC_PERC.toString()))
			space.addParameterDescriptor(new DoubleParamDescriptor(
					HadoopParameter.SORT_REC_PERC, ParamTaskEffect.EFFECT_MAP,
					0.01, 0.5));

		if (conf.get(MR_COMBINE_CLASS) != null
				&& !exclude.contains(HadoopParameter.NUM_SPILLS_COMBINE
						.toString())) {
			space.addParameterDescriptor(new ListParamDescriptor(
					HadoopParameter.NUM_SPILLS_COMBINE,
					ParamTaskEffect.EFFECT_MAP, "3", "9999"));
		}
	}

	/**
	 * Add the reduce-related parameters into the space, except the ones in the
	 * excluded set
	 * 
	 * @param space
	 *            the parameter space
	 * @param conf
	 *            the configuration
	 * @param exclude
	 *            the exclusion set
	 */
	private static void addEffectReduceParameters(ParameterSpace space,
			Configuration conf, Set<String> exclude) {

		// Add parameters the effect the reduce tasks
		if (!exclude.contains(HadoopParameter.RED_TASKS.toString()))
			space.addParameterDescriptor(new IntegerParamDescriptor(
					HadoopParameter.RED_TASKS, ParamTaskEffect.EFFECT_REDUCE,
					1, 100));
		if (!exclude.contains(HadoopParameter.INMEM_MERGE.toString()))
			space.addParameterDescriptor(new IntegerParamDescriptor(
					HadoopParameter.INMEM_MERGE, ParamTaskEffect.EFFECT_REDUCE,
					10, 1000));
		if (!exclude.contains(HadoopParameter.SHUFFLE_IN_BUFF_PERC.toString()))
			space.addParameterDescriptor(new DoubleParamDescriptor(
					HadoopParameter.SHUFFLE_IN_BUFF_PERC,
					ParamTaskEffect.EFFECT_REDUCE, 0.2, 0.9));
		if (!exclude.contains(HadoopParameter.SHUFFLE_MERGE_PERC.toString()))
			space.addParameterDescriptor(new DoubleParamDescriptor(
					HadoopParameter.SHUFFLE_MERGE_PERC,
					ParamTaskEffect.EFFECT_REDUCE, 0.2, 0.9));
		if (!exclude.contains(HadoopParameter.RED_IN_BUFF_PERC.toString()))
			space.addParameterDescriptor(new DoubleParamDescriptor(
					HadoopParameter.RED_IN_BUFF_PERC,
					ParamTaskEffect.EFFECT_REDUCE, 0, 0.8));
		if (!exclude.contains(HadoopParameter.COMPRESS_OUT.toString()))
			space
					.addParameterDescriptor(new BooleanParamDescriptor(
							HadoopParameter.COMPRESS_OUT,
							ParamTaskEffect.EFFECT_REDUCE));
	}

	/**
	 * Add the parameters that effect both map and reducers into the space,
	 * except the ones in the excluded set
	 * 
	 * @param space
	 *            the parameter space
	 * @param conf
	 *            the configuration
	 * @param exclude
	 *            the exclusion set
	 */
	private static void addEffectBothParameters(ParameterSpace space,
			Configuration conf, Set<String> exclude) {

		// Add parameters the effect both map and reduce tasks
		if (!exclude.contains(HadoopParameter.SORT_FACTOR.toString()))
			space.addParameterDescriptor(new IntegerParamDescriptor(
					HadoopParameter.SORT_FACTOR, ParamTaskEffect.EFFECT_BOTH,
					2, 100));
		if (!exclude.contains(HadoopParameter.COMPRESS_MAP_OUT.toString()))
			space.addParameterDescriptor(new BooleanParamDescriptor(
					HadoopParameter.COMPRESS_MAP_OUT,
					ParamTaskEffect.EFFECT_BOTH));

		if (conf.get(MR_COMBINE_CLASS) != null
				&& !exclude.contains(HadoopParameter.COMBINE.toString())) {
			space.addParameterDescriptor(new BooleanParamDescriptor(
					HadoopParameter.COMBINE, ParamTaskEffect.EFFECT_BOTH));
		}
	}

	/**
	 * Add the parameters that effect the job but not the individual tasks into
	 * the space, except the ones in the excluded set
	 * 
	 * @param space
	 *            the parameter space
	 * @param conf
	 *            the configuration
	 * @param exclude
	 *            the exclusion set
	 */
	private static void addEffectNoneParameters(ParameterSpace space,
			Configuration conf, Set<String> exclude) {

		// Add parameters the effect neither map nor reduce tasks
		// Note: The current model will always find 0.05 to be the best
		// space.addParameterDescriptor(
		// new DoubleParamDescriptor(HadoopParameter.RED_SLOWSTART_MAPS,
		// ParamTaskEffect.EFFECT_NONE, 0.05, 1.0));
	}

	/**
	 * Builds and returns a set containing parameters that should be excluded
	 * from the parameter space. The parameters are found as a comma-separated
	 * string in the Hadoop configuration
	 * "starfish.job.optimizer.exclude.parameters"
	 * 
	 * @param conf
	 *            the configuration
	 * @return the exclusion set
	 */
	private static Set<String> buildParamExclusionSet(Configuration conf) {
		Set<String> excludeSet = new HashSet<String>();

		String[] excludeArray = conf.getStrings(EXCLUDE_PARAMS);
		if (excludeArray != null) {
			for (String param : excludeArray) {
				excludeSet.add(param);
			}
		}

		return excludeSet;
	}

	/**
	 * Returns a parameter space with all the parameter descriptors for a
	 * map-only job
	 * 
	 * @param conf
	 *            the job configuration
	 * @param exclude
	 *            the exclusion set
	 * @return the parameter space
	 */
	private static ParameterSpace getMapOnlyParamSpace(Configuration conf,
			Set<String> exclude) {
		ParameterSpace space = new ParameterSpace();

		if (!exclude.contains(HadoopParameter.COMPRESS_OUT.toString()))
			space.addParameterDescriptor(new BooleanParamDescriptor(
					HadoopParameter.COMPRESS_OUT, ParamTaskEffect.EFFECT_MAP));

		return space;
	}
}
