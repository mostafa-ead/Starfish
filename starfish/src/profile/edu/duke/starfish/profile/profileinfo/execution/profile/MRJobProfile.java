package edu.duke.starfish.profile.profileinfo.execution.profile;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;

/**
 * Encapsulates all the profile information regarding the execution of a
 * map-reduce job. In particular it contains:
 * <ol>
 * <li>counters, statistics, and cost factors for the job
 * <li>detailed profiles for each task attempt
 * <li>averaged profiles for the map and reduce tasks
 * </ol>
 * 
 * @author hero
 */
public class MRJobProfile extends MRExecProfile {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private String jobId; // The job ID
	private String clusterName; // The cluster name where profile was obtained
	private String[] jobInputs; // The input paths of the job
	private List<MRMapProfile> mapProfiles; // Map profiles
	private List<MRReduceProfile> reduceProfiles;// Reduce profiles

	private List<MRMapProfile> avgMapProfiles; // Average map profiles
	private MRReduceProfile avgReduceProfile; // Average reduce profile

	// CONSTANTS
	private static final String AVG_MAP = "average_map_";
	private static final String AVG_REDUCE = "average_reduce_";

	private static final MRCostFactors[] missingMapCosts = {
			MRCostFactors.READ_LOCAL_IO_COST,
			MRCostFactors.WRITE_LOCAL_IO_COST, MRCostFactors.MERGE_CPU_COST,
			MRCostFactors.INTERM_UNCOMPRESS_CPU_COST };

	private static final MRCostFactors[] missingReduceCosts = {
			MRCostFactors.READ_LOCAL_IO_COST,
			MRCostFactors.WRITE_LOCAL_IO_COST, MRCostFactors.COMBINE_CPU_COST,
			MRCostFactors.MERGE_CPU_COST,
			MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
			MRCostFactors.INTERM_COMPRESS_CPU_COST };

	private static final MRStatistics[] missingReduceStats = {
			MRStatistics.COMBINE_SIZE_SEL, MRStatistics.COMBINE_PAIRS_SEL };

	/**
	 * Constructor
	 * 
	 * @param jobId
	 *            the job id of the job to profile
	 */
	public MRJobProfile(String jobId) {
		super();

		this.jobId = jobId;
		this.clusterName = null;
		this.jobInputs = null;
		this.mapProfiles = null;
		this.reduceProfiles = null;
		this.avgMapProfiles = null;
		this.avgReduceProfile = null;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 *            the job profile to copy from
	 */
	public MRJobProfile(MRJobProfile other) {
		super(other);

		jobId = other.jobId;
		clusterName = other.clusterName;
		if (other.jobInputs != null) {
			jobInputs = new String[other.jobInputs.length];
			for (int i = 0; i < other.jobInputs.length; ++i)
				jobInputs[i] = other.jobInputs[i];
		}

		if (other.mapProfiles != null) {
			mapProfiles = new ArrayList<MRMapProfile>(other.mapProfiles.size());
			for (MRMapProfile prof : other.mapProfiles)
				mapProfiles.add(new MRMapProfile(prof));
		} else {
			mapProfiles = null;
		}

		if (other.reduceProfiles != null) {
			reduceProfiles = new ArrayList<MRReduceProfile>(
					other.reduceProfiles.size());
			for (MRReduceProfile prof : other.reduceProfiles)
				reduceProfiles.add(new MRReduceProfile(prof));
		} else {
			reduceProfiles = null;
		}

		if (other.avgMapProfiles != null) {
			avgMapProfiles = new ArrayList<MRMapProfile>(other.avgMapProfiles
					.size());
			for (MRMapProfile prof : other.avgMapProfiles)
				avgMapProfiles.add(new MRMapProfile(prof));
		} else {
			avgMapProfiles = null;
		}

		if (other.avgReduceProfile != null) {
			avgReduceProfile = new MRReduceProfile(other.avgReduceProfile);
		} else {
			avgReduceProfile = null;
		}
	}

	/* ***************************************************************
	 * GETTERS & SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the cluster name where this profile was obtained
	 */
	public String getClusterName() {
		return clusterName;
	}

	/**
	 * @return the map-reduce job
	 */
	public String getJobId() {
		return jobId;
	}

	/**
	 * @return the job input paths
	 */
	public String[] getJobInputs() {
		return jobInputs;
	}

	/**
	 * @return the map profiles
	 */
	public List<MRMapProfile> getMapProfiles() {
		if (mapProfiles == null)
			mapProfiles = new ArrayList<MRMapProfile>(0);
		return mapProfiles;
	}

	/**
	 * @return the reduce profiles
	 */
	public List<MRReduceProfile> getReduceProfiles() {
		if (reduceProfiles == null)
			reduceProfiles = new ArrayList<MRReduceProfile>(0);
		return reduceProfiles;
	}

	/**
	 * @return the average map profiles
	 */
	public List<MRMapProfile> getAvgMapProfiles() {
		initializeAvgMapProfiles();
		return avgMapProfiles;
	}

	/**
	 * @return the average reduce profile
	 */
	public MRReduceProfile getAvgReduceProfile() {
		if (avgReduceProfile == null)
			avgReduceProfile = new MRReduceProfile(AVG_REDUCE + jobId);
		return avgReduceProfile;
	}

	/**
	 * @param clusterName
	 *            the cluster name to set
	 */
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	/**
	 * @param jobInputs
	 *            the job input paths to set
	 */
	public void setJobInputs(String[] jobInputs) {
		this.jobInputs = jobInputs;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add a map profile in the job profile
	 * 
	 * @param mapProfile
	 *            the map profile to add
	 */
	public void addMapProfile(MRMapProfile mapProfile) {
		if (mapProfiles == null)
			mapProfiles = new ArrayList<MRMapProfile>();
		mapProfiles.add(mapProfile);
	}

	/**
	 * Add a reduce profile in the job profile
	 * 
	 * @param reduceProfile
	 *            the reduce profile to add
	 */
	public void addReduceProfile(MRReduceProfile reduceProfile) {
		if (reduceProfiles == null)
			reduceProfiles = new ArrayList<MRReduceProfile>();
		reduceProfiles.add(reduceProfile);
	}

	/**
	 * Prints out all the execution profiling information
	 * 
	 * @param out
	 *            The print stream to print to
	 * @param printTaskProfiles
	 *            flag to print the task profiles as well
	 */
	public void printProfile(PrintStream out, boolean printTaskProfiles) {
		out.println("JOB PROFILE:\n\tID:\t" + jobId);

		// Print out the cluster name
		if (clusterName != null) {
			out.println("\tCluster Name:\t" + clusterName);
		}

		// Print out the input paths
		if (jobInputs != null) {
			for (int i = 0; i < jobInputs.length; ++i)
				out.println("\tInput Path " + i + ":\t" + jobInputs[i]);
		}

		// Print out task statistics
		out.println("\tTotal Mappers:\t" + getCounter(MRCounter.MAP_TASKS, 0l));
		out.println("\tProfiled Mappers:\t"
				+ ((mapProfiles == null) ? 0 : mapProfiles.size()));
		out.println("\tTotal Reducers:\t"
				+ getCounter(MRCounter.REDUCE_TASKS, 0l));
		out.println("\tProfiled Reducers:\t"
				+ ((reduceProfiles == null) ? 0 : reduceProfiles.size()));
		out.println("");

		// Print out the average profiles
		if (avgMapProfiles != null)
			for (MRMapProfile avgMapProfile : avgMapProfiles)
				avgMapProfile.printProfile(out);

		if (avgReduceProfile != null)
			avgReduceProfile.printProfile(out);

		// Print out the task profiles
		if (printTaskProfiles) {
			if (mapProfiles != null)
				for (MRMapProfile mapProfile : mapProfiles)
					mapProfile.printProfile(out);

			if (reduceProfiles != null)
				for (MRReduceProfile reduceProfile : reduceProfiles)
					reduceProfile.printProfile(out);

			out.println("");
		}
	}

	/**
	 * Updates the job's profiles by averaging all the information from the task
	 * profiles. This method should be called after adding new task profiles or
	 * updating the existing ones.
	 */
	public void updateProfile() {

		// Save the num of mappers and reducers
		long numMappers = this.getCounter(MRCounter.MAP_TASKS, 0l);
		long numReducers = this.getCounter(MRCounter.REDUCE_TASKS, 0l);

		List<MRTaskProfile> allProfiles = new ArrayList<MRTaskProfile>();
		if (mapProfiles != null)
			allProfiles.addAll(mapProfiles);
		if (reduceProfiles != null)
			allProfiles.addAll(reduceProfiles);

		// Update the number of unique groups produced per map
		long maxUniqueGroups = 0l;
		if (reduceProfiles != null) {
			for (MRReduceProfile redProfile : reduceProfiles) {
				maxUniqueGroups += redProfile.getNumTasks()
						* redProfile.getCounter(MRCounter.REDUCE_INPUT_GROUPS,
								1l);
			}
		}
		if (mapProfiles != null) {
			for (MRMapProfile mapProfile : mapProfiles) {
				mapProfile.addCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS,
						maxUniqueGroups);
			}
		}

		// Average all the new values for the job and the tasks
		updateExecProfile(this, allProfiles);

		// Average the map profiles given they input they processed
		if (mapProfiles != null) {
			MRMapProfile avgMapProfile;
			List<MRMapProfile> avgProfiles = getAvgMapProfiles();
			List<List<MRMapProfile>> sepProfiles = separateMapProfilesBasedOnInput();
			for (int i = 0; i < avgProfiles.size(); i++) {
				avgMapProfile = avgProfiles.get(i);
				updateTaskProfile(avgMapProfile, sepProfiles.get(i));

				// Update the input index (note: all profiles at pos i refer to
				// the same input)
				if (sepProfiles.get(i).size() != 0) {
					avgMapProfile.setInputIndex(sepProfiles.get(i).get(0)
							.getInputIndex());
				}

				// Add some global costs to the average mapper profile
				for (MRCostFactors cost : missingMapCosts) {
					if (containsCostFactor(cost)
							&& !avgMapProfile.containsCostFactor(cost)) {
						avgMapProfile.addCostFactor(cost, getCostFactor(cost));
					}
				}
			}
		}

		// Average the reduce profiles
		if (reduceProfiles != null) {
			MRReduceProfile avgRedProfile = getAvgReduceProfile();
			updateTaskProfile(avgRedProfile, reduceProfiles);

			// Add combiner statistics to the average reducer profile
			for (MRStatistics stat : missingReduceStats) {
				if (containsStatistic(stat)
						&& !avgRedProfile.containsStatistic(stat)) {
					avgRedProfile.addStatistic(stat, getStatistic(stat));
				}
			}

			// Add some global costs to the average reducer profile
			for (MRCostFactors cost : missingReduceCosts) {
				if (containsCostFactor(cost)
						&& !avgRedProfile.containsCostFactor(cost)) {
					avgRedProfile.addCostFactor(cost, getCostFactor(cost));
				}
			}
		}

		// Reset the num of mappers and reducers
		this.addCounter(MRCounter.MAP_TASKS, numMappers);
		this.addCounter(MRCounter.REDUCE_TASKS, numReducers);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + ((jobId == null) ? 0 : jobId.hashCode());
		result = 37 * result
				+ ((mapProfiles == null) ? 0 : mapProfiles.hashCode());
		result = 41 * result
				+ ((reduceProfiles == null) ? 0 : reduceProfiles.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRJobProfile))
			return false;
		MRJobProfile other = (MRJobProfile) obj;
		if (jobId == null) {
			if (other.jobId != null)
				return false;
		} else if (!jobId.equals(other.jobId))
			return false;
		if (mapProfiles == null) {
			if (other.mapProfiles != null)
				return false;
		} else if (!mapProfiles.equals(other.mapProfiles))
			return false;
		if (reduceProfiles == null) {
			if (other.reduceProfiles != null)
				return false;
		} else if (!reduceProfiles.equals(other.reduceProfiles))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MRJobProfile [job=" + jobId + ", counters="
				+ getCounters().size() + ", stats=" + getStatistics().size()
				+ ", costs=" + getCostFactors().size() + "]";
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Average the task counters
	 * 
	 * @param counters
	 *            the map to place in the averaged counters
	 * @param taskProfiles
	 *            the list with the task profiles
	 */
	private void averageCounters(Map<MRCounter, Long> counters,
			List<? extends MRTaskProfile> taskProfiles) {

		double sumValues;
		int numValues;

		// Average the value of each counter from all task profiles
		for (MRCounter counter : MRCounter.values()) {
			sumValues = 0;
			numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsCounter(counter)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile.getCounter(counter, 0l);
					numValues += taskProfile.getNumTasks();
				}
			}

			// Add the averaged counter
			if (numValues != 0) {
				counters.put(counter, (long) Math.round(sumValues / numValues));
			}
		}

	}

	/**
	 * Average the task statistics
	 * 
	 * @param stats
	 *            the map to place in the averaged statistics
	 * @param taskProfiles
	 *            the list with the task profiles
	 */
	private void averageStatistics(Map<MRStatistics, Double> stats,
			List<? extends MRTaskProfile> taskProfiles) {

		double sumValues;
		int numValues;

		// Average the values of each statistic from all task profiles
		for (MRStatistics stat : MRStatistics.values()) {
			sumValues = 0;
			numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsStatistic(stat)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile.getStatistic(stat, 0d);
					numValues += taskProfile.getNumTasks();
				}
			}

			// Add the averaged counter
			if (numValues != 0) {
				stats.put(stat, sumValues / numValues);
			}
		}

	}

	/**
	 * Average the task cost factors
	 * 
	 * @param costs
	 *            the map to place in the averaged cost factors
	 * @param taskProfiles
	 *            the list with the task profiles
	 */
	private void averageCostFactors(Map<MRCostFactors, Double> costs,
			List<? extends MRTaskProfile> taskProfiles) {

		double sumValues;
		int numValues;

		// Average the values of each cost factor from all task profiles
		for (MRCostFactors cost : MRCostFactors.values()) {
			sumValues = 0;
			numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsCostFactor(cost)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile.getCostFactor(cost, 0d);
					numValues += taskProfile.getNumTasks();
				}
			}

			// Add the averaged counter
			if (numValues != 0) {
				costs.put(cost, sumValues / numValues);
			}
		}

	}

	/**
	 * Average the task phase timings
	 * 
	 * @param timings
	 *            the map to place in the averaged timings
	 * @param taskProfiles
	 *            the list with the map or reduce task profiles
	 */
	private void averageTimings(Map<MRTaskPhase, Double> timings,
			List<? extends MRTaskProfile> taskProfiles) {

		double sumValues;
		int numValues;

		// Average the values of each cost factor from all task profiles
		for (MRTaskPhase phase : MRTaskPhase.values()) {
			sumValues = 0;
			numValues = 0;

			for (MRTaskProfile taskProfile : taskProfiles) {
				if (taskProfile.containsTiming(phase)) {
					sumValues += taskProfile.getNumTasks()
							* taskProfile.getTiming(phase, 0d);
					numValues += taskProfile.getNumTasks();
				}
			}

			// Add the averaged counter
			if (numValues != 0) {
				timings.put(phase, sumValues / numValues);
			}
		}
	}

	/**
	 * Initialize the list of lists that will hold the averaged map profiles
	 */
	private void initializeAvgMapProfiles() {

		int numProfiles = (jobInputs == null || jobInputs.length == 0) ? 1
				: jobInputs.length;
		if (avgMapProfiles == null || avgMapProfiles.size() != numProfiles) {
			// Create the empty profiles
			avgMapProfiles = new ArrayList<MRMapProfile>(numProfiles);

			for (int i = 0; i < numProfiles; ++i) {
				avgMapProfiles.add(new MRMapProfile(AVG_MAP + i + "_" + jobId));
			}
		}

	}

	/**
	 * Split the list of map profiles into a list of lists of map profiles,
	 * where each sublist corresponds to the maps that originated from the same
	 * job input.
	 * 
	 * @return a list of lists of map profiles, one for each job input
	 */
	private List<List<MRMapProfile>> separateMapProfilesBasedOnInput() {

		// Create the list of lists of map profiles
		int numProfiles = (jobInputs == null || jobInputs.length == 0) ? 1
				: jobInputs.length;
		List<List<MRMapProfile>> sepProfiles = new ArrayList<List<MRMapProfile>>(
				numProfiles);

		if (numProfiles == 1) {
			// There is only 1 job input
			sepProfiles.add(mapProfiles);
		} else {
			// Initialize the lists that will contain the profiles
			for (int i = 0; i < numProfiles; ++i) {
				sepProfiles.add(new ArrayList<MRMapProfile>());
			}

			// Add the map profile to the appropriate list
			for (MRMapProfile mapProfile : mapProfiles) {
				sepProfiles.get(mapProfile.getInputIndex()).add(mapProfile);
			}
		}

		return sepProfiles;
	}

	/**
	 * Update the execution profile with the averaged mappings from the list of
	 * the task profiles
	 * 
	 * @param profile
	 *            the profile to update
	 * @param taskProfiles
	 *            a list with task profiles
	 */
	private void updateExecProfile(MRExecProfile profile,
			List<? extends MRTaskProfile> taskProfiles) {
		// Clear all the existing values
		profile.clearProfile();

		// Averages all the new values
		averageCounters(profile.getCounters(), taskProfiles);
		averageStatistics(profile.getStatistics(), taskProfiles);
		averageCostFactors(profile.getCostFactors(), taskProfiles);
	}

	/**
	 * Update the task profile with the averaged mappings from the list of the
	 * task profiles
	 * 
	 * @param profile
	 *            the profile to update
	 * @param taskProfiles
	 *            a list with task profiles
	 */
	private void updateTaskProfile(MRTaskProfile profile,
			List<? extends MRTaskProfile> taskProfiles) {
		// Averages all the new values
		profile.clearProfile();
		updateExecProfile(profile, taskProfiles);
		averageTimings(profile.getTimings(), taskProfiles);

		// Set the number of task this profile is applicable to
		int numTasks = 0;
		for (MRTaskProfile prof : taskProfiles) {
			numTasks += prof.getNumTasks();
		}
		profile.setNumTasks(numTasks);
	}

}
