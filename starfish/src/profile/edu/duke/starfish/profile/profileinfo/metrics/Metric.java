package edu.duke.starfish.profile.profileinfo.metrics;

import java.util.Date;

/**
 * This class represents a single timed metric value.
 * 
 * @author hero
 * 
 */
public class Metric implements Comparable<Metric> {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private Date time; // The time this metric was obtained
	private double value; // The value of the metric

	private int hash = -1; // The hash value for this object

	/**
	 * Constructor
	 * 
	 * @param time
	 *            The time this metric was obtained
	 * @param value
	 *            The value of the metric
	 */
	public Metric(Date time, double value) {
		this.time = time;
		this.value = value;
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public Metric(Metric other) {
		this.time = other.time == null ? null : new Date(other.time.getTime());
		this.value = other.value;
	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return the time this metric was obtained
	 */
	public Date getTime() {
		return time;
	}

	/**
	 * @return the value of the metric
	 */
	public double getValue() {
		return value;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == -1) {
			hash = 1;
			hash = 31 * hash + ((time == null) ? 0 : time.hashCode());
			long temp = Double.doubleToLongBits(value);
			hash = 37 * hash + (int) (temp ^ (temp >>> 32));
		}
		return hash;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Metric))
			return false;
		Metric other = (Metric) obj;
		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;
		if (Double.doubleToLongBits(value) != Double
				.doubleToLongBits(other.value))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Metric [time=" + time + ", value=" + value + "]";
	}

	/**
	 * @see java.lang.Comparable#compareTo(Object)
	 */
	@Override
	public int compareTo(Metric o) {
		return this.getTime().compareTo(o.getTime());
	}

}
