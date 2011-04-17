package edu.duke.starfish.jobopt.params;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A parameter descriptor for a parameter with an integer range domain
 * 
 * @author hero
 */
public class IntegerParamDescriptor extends ParameterDescriptor {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private int minValue;
	private int maxValue;

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
	public IntegerParamDescriptor(HadoopParameter parameter,
			ParamTaskEffect effect, int minValue, int maxValue) {
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
	public int getMinValue() {
		return minValue;
	}

	/**
	 * @return the maxValue
	 */
	public int getMaxValue() {
		return maxValue;
	}

	/**
	 * @param minValue
	 *            the minValue to set
	 */
	public void setMinValue(int minValue) {
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
	public void setMaxValue(int maxValue) {
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
		return maxValue - minValue + 1;
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getEquiSpacedValues(int)
	 */
	@Override
	public List<String> getEquiSpacedValues(int numValues) {

		// Cannot return more values than the range
		if (numValues > maxValue - minValue + 1)
			numValues = maxValue - minValue + 1;

		List<String> values = new ArrayList<String>(numValues);
		if (numValues == 1) {
			// Add only the median
			values.add(Integer.toString((maxValue + minValue) / 2));
		} else {
			// Add the values
			double step = (maxValue - minValue) / (double) (numValues - 1);
			for (int i = 0; i < numValues; ++i) {
				values.add(Integer.toString((int) Math.round(minValue + i
						* step)));
			}
		}

		return values;
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValues(int)
	 */
	@Override
	public List<String> getRandomValues(int numValues) {

		// Cannot return more values than the range
		if (numValues >= maxValue - minValue + 1)
			return getEquiSpacedValues(numValues);

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
		return Integer.toString(minValue
				+ random.nextInt(maxValue - minValue + 1));
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValue(String,
	 *      double)
	 */
	@Override
	public String getRandomValue(String center, double scale) {
		int c = Integer.parseInt(center);
		int r = (int) Math.round(scale * (maxValue - minValue) / 2.0);
		int min = Math.max(c - r, minValue);
		int max = Math.min(c + r, maxValue);

		return Integer.toString(min + random.nextInt(max - min + 1));
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "IntegerParamDescriptor [Parameter=" + getParameter()
				+ " Effect=" + getEffect() + " Min=" + minValue + " Max="
				+ maxValue + "]";
	}
}
