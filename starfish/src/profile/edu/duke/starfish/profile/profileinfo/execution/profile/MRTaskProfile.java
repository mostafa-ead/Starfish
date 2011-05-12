package edu.duke.starfish.profile.profileinfo.execution.profile;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.EnumMap;
import java.util.Map;

import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;

/**
 * Encapsulates all the profile information regarding the execution of a
 * map-reduce task attempt. It contains counters, statistics, cost factors, and
 * timings.
 * 
 * @author hero
 */
public class MRTaskProfile extends MRExecProfile {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private String taskId; // The task to profile
	private Map<MRTaskPhase, Double> timings; // The phase timings
	private int numTasks; // The number of tasks it is applicable to

	/**
	 * Constructor
	 * 
	 * @param taskId
	 *            the task id of the task to profile
	 */
	public MRTaskProfile(String taskId) {
		super();

		this.taskId = taskId;
		this.timings = null;
		this.numTasks = 1;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 *            a task profile to copy from
	 */
	public MRTaskProfile(MRTaskProfile other) {
		super(other);

		this.taskId = other.taskId;
		if (other.timings != null)
			this.timings = new EnumMap<MRTaskPhase, Double>(other.timings);
		this.numTasks = other.numTasks;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * Prints out all the execution profiling information
	 * 
	 * @param out
	 *            The print stream to write to
	 */
	public void printProfile(PrintStream out) {

		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMaximumFractionDigits(0);

		// Print out the number of tasks
		out.println("Tasks:\n\t" + numTasks);

		// Print out the counters
		if (!getCounters().isEmpty()) {
			out.println("Counters:");
			printEnumToNumberMap(out, getCounters(), nf);
		}

		nf.setMinimumFractionDigits(6);
		nf.setMaximumFractionDigits(6);

		// Print out the statistics
		if (!getStatistics().isEmpty()) {
			out.println("Statistics:");
			printEnumToNumberMap(out, getStatistics(), nf);
		}

		// Print out the costs
		if (!getCostFactors().isEmpty()) {
			out.println("Cost Factors:");
			printEnumToNumberMap(out, getCostFactors(), nf);
		}

		// Print out the time breakdowns
		if (!getTimings().isEmpty()) {
			out.println("Timings:");
			printEnumToNumberMap(out, getTimings(), nf);
		}
		out.println("");
	}

	/* ***************************************************************
	 * GETTERS & SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the task id
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * @return the timings
	 */
	public Map<MRTaskPhase, Double> getTimings() {
		if (timings == null) // Create on demand
			timings = new EnumMap<MRTaskPhase, Double>(MRTaskPhase.class);
		return timings;
	}

	/**
	 * @return the number of tasks
	 */
	public int getNumTasks() {
		return numTasks;
	}

	/**
	 * @param taskId
	 *            the task ID to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	/**
	 * @param numTasks
	 *            the number of tasks applicable for this profile
	 */
	public void setNumTasks(int numTasks) {
		this.numTasks = numTasks;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add a phase timing
	 * 
	 * @param phase
	 *            the phase timing to add
	 * @param value
	 *            the value of the phase timing
	 */
	public void addTiming(MRTaskPhase phase, Double value) {
		if (timings == null) // Create on demand
			timings = new EnumMap<MRTaskPhase, Double>(MRTaskPhase.class);
		timings.put(phase, value);
	}

	/**
	 * Add all phase timings
	 * 
	 * @param timings
	 *            the timings to add
	 */
	public void addTimings(Map<MRTaskPhase, Double> timings) {
		if (this.timings == null) // Create on demand
			this.timings = new EnumMap<MRTaskPhase, Double>(MRTaskPhase.class);
		this.timings.putAll(timings);
	}

	/**
	 * Check for the existence of the input phase timing
	 * 
	 * @param phase
	 *            the phase timing to check
	 * @return true if the phase timing exists
	 */
	public boolean containsTiming(MRTaskPhase phase) {
		return (timings != null && timings.containsKey(phase));
	}

	/**
	 * Get a phase timing
	 * 
	 * @param phase
	 *            the phase timing to get
	 * @param defaultValue
	 *            the default value to get if the phase timing is not found
	 * @return the value of the phase timing
	 */
	public Double getTiming(MRTaskPhase phase, Double defaultValue) {
		if (timings == null || !timings.containsKey(phase))
			return defaultValue;
		else
			return timings.get(phase);
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * Check if the profile is empty, i.e., there are no counters, stats, costs,
	 * or timings.
	 * 
	 * @return true if profile is empty
	 */
	@Override
	public boolean isEmpty() {
		return super.isEmpty() && (timings == null || timings.isEmpty());
	}

	/**
	 * Clears the entire profile (timings, counters, statistics, and cost
	 * factors)
	 */
	@Override
	public void clearProfile() {
		super.clearProfile();
		if (timings != null)
			timings.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int hash = super.hashCode();
		hash = 31 * hash + ((taskId == null) ? 0 : taskId.hashCode());
		hash = 37 * hash + ((timings == null) ? 0 : timings.hashCode());
		return hash;
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
		if (!(obj instanceof MRTaskProfile))
			return false;
		MRTaskProfile other = (MRTaskProfile) obj;
		if (taskId == null) {
			if (other.taskId != null)
				return false;
		} else if (!taskId.equals(other.taskId))
			return false;
		if (timings == null) {
			if (other.timings != null)
				return false;
		} else if (!timings.equals(other.timings))
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
		return "MRTaskProfile [task=" + taskId + ", counters="
				+ getCounters().size() + ", stats=" + getStatistics().size()
				+ ", costs=" + getCostFactors().size() + ", timings="
				+ getTimings().size() + "]";
	}

}
