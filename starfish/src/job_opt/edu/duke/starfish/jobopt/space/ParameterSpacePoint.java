package edu.duke.starfish.jobopt.space;

import java.util.EnumMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.jobopt.params.HadoopParameter;

/**
 * This class represents a point in the Hadoop configuration parameter space.
 * 
 * @author hero
 */
public class ParameterSpacePoint {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private EnumMap<HadoopParameter, String> values;

	/**
	 * Default Constructor
	 */
	public ParameterSpacePoint() {
		this.values = new EnumMap<HadoopParameter, String>(
				HadoopParameter.class);
	}

	/**
	 * Constructor that also adds a parameter value
	 * 
	 * @param param
	 *            the parameter
	 * @param value
	 *            the value
	 */
	public ParameterSpacePoint(HadoopParameter param, String value) {
		this.values = new EnumMap<HadoopParameter, String>(
				HadoopParameter.class);
		this.values.put(param, value);
	}

	/**
	 * Copy constructor
	 * 
	 * @param point
	 *            the point to duplicate
	 */
	public ParameterSpacePoint(ParameterSpacePoint point) {
		this.values = new EnumMap<HadoopParameter, String>(point.values);
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Set a parameter value
	 * 
	 * @param param
	 *            the parameter
	 * @param value
	 *            the value
	 */
	public void addParamValue(HadoopParameter param, String value) {
		values.put(param, value);
	}

	/**
	 * Get all the parameters in the space point
	 * 
	 * @return the hadoop parameters
	 */
	public Set<HadoopParameter> getAllParameters() {
		return values.keySet();
	}

	/**
	 * Get the parameter value of the provided parameter
	 * 
	 * @param param
	 *            the parameter
	 * @return the value
	 */
	public String getParameterValue(HadoopParameter param) {
		return values.get(param);
	}

	/**
	 * Populate the configuration with the parameter values in this parameter
	 * space point
	 * 
	 * @param conf
	 *            the job configuration
	 */
	public void populateConfiguration(Configuration conf) {
		for (Entry<HadoopParameter, String> entry : values.entrySet()) {
			conf.set(entry.getKey().toString(), entry.getValue());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 31 + ((values == null) ? 0 : values.hashCode());
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
		if (!(obj instanceof ParameterSpacePoint))
			return false;
		ParameterSpacePoint other = (ParameterSpacePoint) obj;
		if (values == null) {
			if (other.values != null)
				return false;
		} else if (!values.equals(other.values))
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
		return "ParameterSpacePoint [values=" + values + "]";
	}

}
