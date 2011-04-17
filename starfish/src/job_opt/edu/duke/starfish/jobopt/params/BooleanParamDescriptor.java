package edu.duke.starfish.jobopt.params;

import java.util.ArrayList;
import java.util.List;

/**
 * A parameter descriptor for a parameter with a boolean domain
 * 
 * @author hero
 */
public class BooleanParamDescriptor extends ParameterDescriptor {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private List<String> cacheOneValue;
	private List<String> cacheTwoValues;

	/**
	 * Constructor
	 * 
	 * @param parameter
	 *            the Hadoop parameter
	 * @param effect
	 *            the parameter's effect
	 */
	public BooleanParamDescriptor(HadoopParameter parameter,
			ParamTaskEffect effect) {
		super(parameter, effect);
		this.cacheOneValue = null;
		this.cacheTwoValues = null;
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
		return 2;
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getEquiSpacedValues(int)
	 */
	@Override
	public List<String> getEquiSpacedValues(int numValues) {
		if (numValues < 2) {
			if (cacheOneValue == null) {
				cacheOneValue = new ArrayList<String>(1);
				cacheOneValue.add("false");
			}
			return cacheOneValue;
		} else {
			if (cacheTwoValues == null) {
				cacheTwoValues = new ArrayList<String>(2);
				cacheTwoValues.add("false");
				cacheTwoValues.add("true");
			}
			return cacheTwoValues;
		}
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValues(int)
	 */
	@Override
	public List<String> getRandomValues(int numValues) {
		return getEquiSpacedValues(numValues);
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValue()
	 */
	@Override
	public String getRandomValue() {
		if (random.nextFloat() < 0.5)
			return "false";
		else
			return "true";
	}

	/**
	 * @see edu.duke.starfish.jobopt.params.ParameterDescriptor#getRandomValue(String,
	 *      double)
	 */
	@Override
	public String getRandomValue(String center, double scale) {
		if (random.nextFloat() < 0.5 / scale)
			return center;
		else
			return center == "true" ? "false" : "true";
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "BooleanParamDescriptor [Parameter=" + getParameter()
				+ " Effect=" + getEffect() + "]";
	}

}
