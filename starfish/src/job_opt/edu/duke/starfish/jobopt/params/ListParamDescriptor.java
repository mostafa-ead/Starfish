package edu.duke.starfish.jobopt.params;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * A parameter descriptor for a parameter with a domain consisting of a list of
 * values
 * 
 * @author hero
 */
public class ListParamDescriptor extends ParameterDescriptor {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private String[] values;
	private List<String> cacheValues;

	/**
	 * Constructor
	 * 
	 * @param parameter
	 *            the parameter
	 * @param effect
	 *            the parameter's effect
	 * @param values
	 *            the values in the domain
	 */
	public ListParamDescriptor(HadoopParameter parameter,
			ParamTaskEffect effect, String... values) {
		super(parameter, effect);
		this.values = values;
		this.cacheValues = Arrays.asList(values);
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the values
	 */
	public String[] getValues() {
		return values;
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
		return values.length;
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getEquiSpacedValues(int)
	 */
	@Override
	public List<String> getEquiSpacedValues(int numValues) {

		// Cannot return more values than the range
		if (numValues >= values.length)
			return cacheValues;

		List<String> result = new ArrayList<String>(numValues);
		if (numValues == 1) {
			// Add only the median
			result.add(values[values.length / 2]);
		} else {
			// Add the values
			double step = values.length / (double) (numValues - 1);
			for (int i = 0; i < numValues; ++i) {
				result.add(values[(int) Math.round(i * step)]);
			}
		}

		return result;
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValues(int)
	 */
	@Override
	public List<String> getRandomValues(int numValues) {

		// Cannot return more values than the range
		if (numValues >= values.length)
			return cacheValues;

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
		return values[random.nextInt(values.length)];
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValue(String,
	 *      double)
	 */
	@Override
	public String getRandomValue(String center, double scale) {
		int c = cacheValues.indexOf(center);
		int r = (int) Math.round(scale * values.length / 2.0);
		int min = Math.max(c - r, 0);
		int max = Math.min(c + r, values.length);

		if (min == max)
			return values[c];
		else
			return values[min + random.nextInt(max - min)];
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ListParamDescriptor [Parameter=" + getParameter() + " Effect="
				+ getEffect() + " Values=" + Arrays.toString(values) + "]";
	}
}
