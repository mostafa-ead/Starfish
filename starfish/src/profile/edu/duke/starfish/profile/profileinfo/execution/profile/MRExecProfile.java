package edu.duke.starfish.profile.profileinfo.execution.profile;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;

import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;

/**
 * Encapsulates all the profile information regarding the execution of a
 * map-reduce task attempt, task, or job. It contains counters, statistics, and
 * cost factors.
 * 
 * @author hero
 */
public abstract class MRExecProfile {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private Map<MRCounter, Long> counters; // The counters
	private Map<MRStatistics, Double> stats; // The statistics
	private Map<MRCostFactors, Double> costs; // The costs

	/**
	 * Default constructor
	 */
	public MRExecProfile() {
		this.counters = null;
		this.stats = null;
		this.costs = null;
	}

	/**
	 * Copy constructor
	 * 
	 * @param other
	 *            an execution profile to copy from
	 */
	public MRExecProfile(MRExecProfile other) {
		this();
		if (other.counters != null)
			this.counters = new EnumMap<MRCounter, Long>(other.counters);
		if (other.stats != null)
			this.stats = new EnumMap<MRStatistics, Double>(other.stats);
		if (other.costs != null)
			this.costs = new EnumMap<MRCostFactors, Double>(other.costs);
	}

	/* ***************************************************************
	 * GETTERS & SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the counters
	 */
	public Map<MRCounter, Long> getCounters() {
		if (counters == null) // Create on demand
			counters = new EnumMap<MRCounter, Long>(MRCounter.class);
		return counters;
	}

	/**
	 * @return the statistics
	 */
	public Map<MRStatistics, Double> getStatistics() {
		if (stats == null) // Create on demand
			stats = new EnumMap<MRStatistics, Double>(MRStatistics.class);
		return stats;
	}

	/**
	 * @return the costs
	 */
	public Map<MRCostFactors, Double> getCostFactors() {
		if (costs == null) // Create on demand
			costs = new EnumMap<MRCostFactors, Double>(MRCostFactors.class);
		return costs;
	}

	/**
	 * @param counters
	 *            the counters to set
	 */
	public void setCounters(Map<MRCounter, Long> counters) {
		this.counters = counters;
	}

	/**
	 * @param stats
	 *            the statistics to set
	 */
	public void setStatistics(Map<MRStatistics, Double> stats) {
		this.stats = stats;
	}

	/**
	 * @param costs
	 *            the cost factors to set
	 */
	public void setCostFactors(Map<MRCostFactors, Double> costs) {
		this.costs = costs;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add a counter
	 * 
	 * @param counter
	 *            the counter to add
	 * @param value
	 *            the value of the counter
	 */
	public void addCounter(MRCounter counter, Long value) {
		if (counters == null) // Create on demand
			counters = new EnumMap<MRCounter, Long>(MRCounter.class);
		counters.put(counter, value);
	}

	/**
	 * Add a statistic
	 * 
	 * @param stat
	 *            the statistic to add
	 * @param value
	 *            the value of the statistic
	 */
	public void addStatistic(MRStatistics stat, Double value) {
		if (stats == null) // Create on demand
			stats = new EnumMap<MRStatistics, Double>(MRStatistics.class);
		stats.put(stat, value);
	}

	/**
	 * Add a cost factor
	 * 
	 * @param cost
	 *            the cost factor to add
	 * @param value
	 *            the value of the cost factor
	 */
	public void addCostFactor(MRCostFactors cost, Double value) {
		if (costs == null) // Create on demand
			costs = new EnumMap<MRCostFactors, Double>(MRCostFactors.class);
		costs.put(cost, value);
	}

	/**
	 * Add all counters
	 * 
	 * @param counters
	 *            the counters to add
	 */
	public void addCounters(Map<MRCounter, Long> counters) {
		if (this.counters == null) // Create on demand
			this.counters = new EnumMap<MRCounter, Long>(MRCounter.class);
		this.counters.putAll(counters);
	}

	/**
	 * Add all statistics
	 * 
	 * @param stats
	 *            the statistics to add
	 */
	public void addStatistics(Map<MRStatistics, Double> stats) {
		if (this.stats == null) // Create on demand
			this.stats = new EnumMap<MRStatistics, Double>(MRStatistics.class);
		this.stats.putAll(stats);
	}

	/**
	 * Add all cost factors
	 * 
	 * @param costs
	 *            the cost factors to add
	 */
	public void addCostFactors(Map<MRCostFactors, Double> costs) {
		if (this.costs == null) // Create on demand
			this.costs = new EnumMap<MRCostFactors, Double>(MRCostFactors.class);
		this.costs.putAll(costs);
	}

	/**
	 * Check for the existence of the input counter
	 * 
	 * @param counter
	 *            the counter to check
	 * @return true if the counter exists
	 */
	public boolean containsCounter(MRCounter counter) {
		return (counters != null && counters.containsKey(counter));
	}

	/**
	 * Check for the existence of the input statistic
	 * 
	 * @param stat
	 *            the statistic to check
	 * @return true if the statistic exists
	 */
	public boolean containsStatistic(MRStatistics stat) {
		return (stats != null && stats.containsKey(stat));
	}

	/**
	 * Check for the existence of the input cost factor
	 * 
	 * @param cost
	 *            the cost factor to check
	 * @return true if the cost factor exists
	 */
	public boolean containsCostFactor(MRCostFactors cost) {
		return (costs != null && costs.containsKey(cost));
	}

	/**
	 * Get a counter
	 * 
	 * @param counter
	 *            the counter to get
	 * @return the value of the counter
	 */
	public Long getCounter(MRCounter counter) {
		return counters == null ? null : counters.get(counter);
	}

	/**
	 * Get a counter
	 * 
	 * @param counter
	 *            the counter to get
	 * @param defaultValue
	 *            the default value to get if the counter is not found
	 * @return the value of the counter
	 */
	public Long getCounter(MRCounter counter, Long defaultValue) {
		if (counters == null || !counters.containsKey(counter))
			return defaultValue;
		else
			return counters.get(counter);
	}

	/**
	 * Get a statistic
	 * 
	 * @param stat
	 *            the statistic to get
	 * @return the value of the statistic
	 */
	public Double getStatistic(MRStatistics stat) {
		return stats == null ? null : stats.get(stat);
	}

	/**
	 * Get a statistic
	 * 
	 * @param stat
	 *            the statistic to get
	 * @param defaultValue
	 *            the default value to get if the statistic is not found
	 * @return the value of the statistic
	 */
	public Double getStatistic(MRStatistics stat, Double defaultValue) {
		if (stats == null || !stats.containsKey(stat))
			return defaultValue;
		else
			return stats.get(stat);
	}

	/**
	 * Get a cost factor
	 * 
	 * @param cost
	 *            the cost factor to get
	 * @return the value of the cost factor
	 */
	public Double getCostFactor(MRCostFactors cost) {
		return costs == null ? null : costs.get(cost);
	}

	/**
	 * Get a cost factor
	 * 
	 * @param cost
	 *            the cost factor to get
	 * @param defaultValue
	 *            the default value to get if the cost factor is not found
	 * @return the value of the cost factor
	 */
	public Double getCostFactor(MRCostFactors cost, Double defaultValue) {
		if (costs == null || !costs.containsKey(cost))
			return defaultValue;
		else
			return costs.get(cost);
	}

	/**
	 * Check if the profile is empty, i.e., there are no counters, stats, or
	 * costs.
	 * 
	 * @return true if profile is empty
	 */
	public boolean isEmpty() {
		return (counters == null || counters.isEmpty())
				&& (stats == null || stats.isEmpty())
				&& (costs == null || costs.isEmpty());
	}

	/**
	 * Clears the entire profile (counters, statistics, and cost factors)
	 */
	public void clearProfile() {
		if (counters != null)
			counters.clear();
		if (stats != null)
			stats.clear();
		if (costs != null)
			costs.clear();
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int hash = 1;
		hash = 31 * hash + ((costs == null) ? 0 : costs.hashCode());
		hash = 37 * hash + ((counters == null) ? 0 : counters.hashCode());
		hash = 41 * hash + ((stats == null) ? 0 : stats.hashCode());
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
		if (obj == null)
			return false;
		if (!(obj instanceof MRExecProfile))
			return false;
		MRExecProfile other = (MRExecProfile) obj;
		if (costs == null) {
			if (other.costs != null)
				return false;
		} else if (!costs.equals(other.costs))
			return false;
		if (counters == null) {
			if (other.counters != null)
				return false;
		} else if (!counters.equals(other.counters))
			return false;
		if (stats == null) {
			if (other.stats != null)
				return false;
		} else if (!stats.equals(other.stats))
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
		return "MRExecProfile [counters="
				+ ((counters == null) ? 0 : counters.size()) + ", stats="
				+ ((stats == null) ? 0 : stats.size()) + ", costs="
				+ ((costs == null) ? 0 : costs.size()) + "]";
	}

	/* ***************************************************************
	 * PROTECTED METHODS
	 * ***************************************************************
	 */

	/**
	 * Prints out the content (key-value pairs) of an Enum-to-Number map using
	 * the provided input format for the values. It prints the pairs in the
	 * ordinal order of the enum
	 * 
	 * @param out
	 *            The print stream to print to
	 * @param map
	 *            enum-to-number map to print out
	 * @param nf
	 *            number format for the values
	 */
	protected void printEnumToNumberMap(PrintStream out,
			Map<? extends Enum<?>, ?> map, NumberFormat nf) {

		// Print out the key-value pairs
		for (Entry<? extends Enum<?>, ?> entry : map.entrySet()) {
			out.println("\t" + entry.getKey() + "\t"
					+ nf.format(entry.getValue()));
		}

	}

}
