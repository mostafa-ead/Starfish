package edu.duke.starfish.whatif.oracle;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;

/**
 * An abstract class providing functionality that is common between the map and
 * reduce profile oracles.
 * 
 * @author hero
 * 
 */
public abstract class TaskProfileOracle {

	// Constants
	private static final char UNDERSCORE = '_';
	private static final String VIRTUAL = "virtual";

	/**
	 * Calculate the task statistics for a virtual task profile based on the
	 * source task profile and the suggested configuration. This method only
	 * calculates the statistics that are common between the map and the reduce
	 * profiles.
	 * 
	 * @param sourceProf
	 *            the source profile
	 * @param virtualProf
	 *            the virtual profile to populate
	 * @param conf
	 *            the suggested configuration
	 */
	protected void calcVirtualTaskStatistics(MRTaskProfile sourceProf,
			MRTaskProfile virtualProf, Configuration conf) {

		// Set the combiner statistics
		if (conf.get(MR_COMBINE_CLASS) != null
				&& conf.getBoolean(STARFISH_USE_COMBINER, true)) {
			virtualProf.addStatistic(MRStatistics.COMBINE_SIZE_SEL, sourceProf
					.getStatistic(MRStatistics.COMBINE_SIZE_SEL, DEF_SEL_ONE));
			virtualProf.addStatistic(MRStatistics.COMBINE_PAIRS_SEL, sourceProf
					.getStatistic(MRStatistics.COMBINE_PAIRS_SEL, DEF_SEL_ONE));
		}

		// Set intermediate compression statistics
		if (conf.getBoolean(MR_COMPRESS_MAP_OUT, false)) {
			virtualProf.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO,
					sourceProf.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO,
							DEF_COMPRESS_RATIO));
		}

		// Set the memory statistics
		virtualProf.addStatistic(MRStatistics.STARTUP_MEM, sourceProf
				.getStatistic(MRStatistics.STARTUP_MEM, DEF_MEM));
		virtualProf.addStatistic(MRStatistics.SETUP_MEM, sourceProf
				.getStatistic(MRStatistics.SETUP_MEM, DEF_MEM));
		virtualProf.addStatistic(MRStatistics.CLEANUP_MEM, sourceProf
				.getStatistic(MRStatistics.CLEANUP_MEM, DEF_MEM));
	}

	/**
	 * Calculate the task costs for a virtual task profile based on the source
	 * task profile and the suggested configuration. This method only calculates
	 * the costs that are common between the map and the reduce profiles.
	 * 
	 * @param sourceProf
	 *            the source profile
	 * @param virtualProf
	 *            the virtual profile to populate
	 * @param conf
	 *            the suggested configuration
	 */
	protected void calcVirtualTaskCosts(MRTaskProfile sourceProf,
			MRTaskProfile virtualProf, Configuration conf) {

		virtualProf.addCostFactors(sourceProf.getCostFactors());

		// Ensure we have combine costs
		if (conf.get(MR_COMBINE_CLASS) != null
				&& conf.getBoolean(STARFISH_USE_COMBINER, true)) {
			virtualProf.addCostFactor(MRCostFactors.COMBINE_CPU_COST,
					sourceProf.getCostFactor(MRCostFactors.COMBINE_CPU_COST,
							DEF_COST_CPU_COMBINE));
		}

		// Ensure we have compression costs and are not set to zero
		if (conf.getBoolean(MR_COMPRESS_MAP_OUT, false)) {
			virtualProf.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST,
					sourceProf.getCostFactor(
							MRCostFactors.INTERM_COMPRESS_CPU_COST,
							DEF_COST_CPU_COMPRESS));
			virtualProf.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
					sourceProf.getCostFactor(
							MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
							DEF_COST_CPU_UNCOMPRESS));

			if (virtualProf
					.getCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST) == 0d)
				virtualProf.addCostFactor(
						MRCostFactors.INTERM_COMPRESS_CPU_COST,
						DEF_COST_CPU_COMPRESS);
			if (virtualProf
					.getCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST) == 0d)
				virtualProf.addCostFactor(
						MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
						DEF_COST_CPU_UNCOMPRESS);
		}
	}

	/**
	 * Determines the total number of spill files read during the intermediate
	 * merging of spill files (i.e. up to just before the final merge)
	 * 
	 * Warning: For correct results ensure: numSpills <= factor^2
	 * 
	 * @param numSpills
	 *            the total number of spills
	 * @param sortFactor
	 *            the sort factor
	 * @return the number of spill reads for intermediate merges
	 */
	protected long getNumIntermSpillReads(long numSpills, long sortFactor) {

		if (numSpills <= sortFactor)
			return 0;

		long firstMerge = getNumSpillsInFirstMerge(numSpills, sortFactor);
		return firstMerge + ((numSpills - firstMerge) / sortFactor)
				* sortFactor;
	}

	/**
	 * Determines the total number of merge passes that will happen over the
	 * spills to produce a single spill file.
	 * 
	 * Warning: For correct results ensure: numSpills <= factor^2
	 * 
	 * @param numSpills
	 *            the total number of spills
	 * @param sortFactor
	 *            the sort factor
	 * @return the number of spill merges
	 */
	protected long getNumSpillMerges(long numSpills, long sortFactor) {
		if (numSpills == 1)
			return 0;
		else if (numSpills <= sortFactor)
			return 1;
		else
			return 2
					+ (numSpills - getNumSpillsInFirstMerge(numSpills,
							sortFactor)) / sortFactor;
	}

	/**
	 * Determine the number of spills to merge in the first pass. Assuming more
	 * than factor spills, the first pass will attempt to bring the total number
	 * of (numSpills - 1) to be divisible by the (sortFactor - 1) to minimize
	 * the number of merges.
	 * 
	 * @param numSpills
	 *            the total number of spills
	 * @param sortFactor
	 *            the sort factor
	 * @return the number of spills to merge in the first pass
	 */
	protected long getNumSpillsInFirstMerge(long numSpills, long sortFactor) {
		if (numSpills <= sortFactor)
			return numSpills;

		long mod = (numSpills - 1) % (sortFactor - 1);
		if (mod == 0)
			return sortFactor;
		return mod + 1;
	}

	/**
	 * Returns a virtual task id given the current id. The input is expected to
	 * be of the form "aggregated_reduce_job__201011062135_0003". The virtual id
	 * will then be "virtual_reduce_job__201011062135_0003"
	 * 
	 * @param currentId
	 *            the current task id
	 * @return the virtual task id
	 */
	protected String getVirtualTaskId(String currentId) {
		int index = currentId.indexOf(UNDERSCORE);
		if (index != -1) {
			return VIRTUAL + currentId.substring(index);
		} else {
			return VIRTUAL + currentId;
		}
	}
}
