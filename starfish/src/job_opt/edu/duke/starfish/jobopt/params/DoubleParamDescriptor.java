package edu.duke.starfish.jobopt.params;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A parameter descriptor for a parameter with a numeric range domain
 * 
 * @author hero
 */
public class DoubleParamDescriptor extends ParameterDescriptor {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private double minValue;
	private double maxValue;

	/**
	 * Constructor
	 * 
	 * @param parameter
	 *            the parameter
	 * @param effect
	 *            the parameter's effect
	 * @param minValue
	 *            the min value
	 * @param maxValue
	 *            the max value
	 */
	public DoubleParamDescriptor(HadoopParameter parameter,
			ParamTaskEffect effect, double minValue, double maxValue) {
		super(parameter, effect);
		this.minValue = minValue;
		this.maxValue = maxValue;

		if (maxValue < minValue)
			throw new RuntimeException("ERROR: The max " + maxValue
					+ " is less than min " + minValue + " for param "
					+ parameter.toString());
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the minValue
	 */
	public double getMinValue() {
		return minValue;
	}

	/**
	 * @return the maxValue
	 */
	public double getMaxValue() {
		return maxValue;
	}

	/**
	 * @param minValue
	 *            the minValue to set
	 */
	public void setMinValue(double minValue) {
		this.minValue = minValue;

		if (maxValue < minValue)
			throw new RuntimeException("ERROR: The max " + maxValue
					+ " is less than min " + minValue + " for param "
					+ getParameter().toString());
	}

	/**
	 * @param maxValue
	 *            the maxValue to set
	 */
	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;

		if (maxValue < minValue)
			throw new RuntimeException("ERROR: The max " + maxValue
					+ " is less than min " + minValue + " for param "
					+ getParameter().toString());
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getNumUniqueValues()
	 */
	@Override
	public int getNumUniqueValues() {
		return Integer.MAX_VALUE;
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getEquiSpacedValues(int)
	 */
	@Override
	public List<String> getEquiSpacedValues(int numValues) {

		// If min equals max, there is only one possible value
		List<String> values;
		if (minValue == maxValue) {
			values = new ArrayList<String>(1);
			values.add(Double.toString(minValue));
			return values;
		}

		values = new ArrayList<String>(numValues);
		if (numValues == 1) {
			// Add only the median
			values.add(Double.toString((maxValue + minValue) / 2));
		} else {
			// Add the values
			double step = (maxValue - minValue) / (numValues - 1);
			for (int i = 0; i < numValues; ++i) {
				values.add(Double.toString(minValue + i * step));
			}
		}

		return values;
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValues(int)
	 */
	@Override
	public List<String> getRandomValues(int numValues) {

		// If min equals max, there is only one possible value
		if (minValue == maxValue) {
			List<String> values = new ArrayList<String>(1);
			values.add(Double.toString(minValue));
			return values;
		}

		HashSet<String> values = new HashSet<String>(numValues);
		while (values.size() < numValues) {
			values.add(getRandomValue());
		}

		return new ArrayList<String>(values);
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValue()
	 */
	@Override
	public String getRandomValue() {
		return Double.toString(minValue + (maxValue - minValue)
				* random.nextDouble());
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValue(String,
	 *      double)
	 */
	@Override
	public String getRandomValue(String center, double scale) {
		double c = Double.parseDouble(center);
		double r = scale * (maxValue - minValue) / 2.0;
		double min = Math.max(c - r, minValue);
		double max = Math.min(c + r, maxValue);

		return Double.toString(min + (max - min) * random.nextDouble());
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "BooleanParamDescriptor [Parameter=" + getParameter()
				+ " Effect=" + getEffect() + " Min=" + minValue + " Max="
				+ maxValue + "]";
	}

}
