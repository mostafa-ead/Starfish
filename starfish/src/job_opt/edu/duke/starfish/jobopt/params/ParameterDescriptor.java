package edu.duke.starfish.jobopt.params;

import java.util.List;
import java.util.Random;

/**
 * This class forms the basis for describing the domain (e.g., integer range
 * [1,5]) and the effect (e.g., effects only map tasks) of a particular Hadoop
 * parameter.
 * 
 * @author hero
 */
public abstract class ParameterDescriptor {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private HadoopParameter parameter;
	private ParamTaskEffect effect;

	protected static final Random random = new Random();

	/**
	 * Constructor
	 * 
	 * @param parameter
	 *            the parameter
	 * @param effect
	 *            the parameter's effect
	 */
	public ParameterDescriptor(HadoopParameter parameter, ParamTaskEffect effect) {
		this.parameter = parameter;
		this.effect = effect;
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Set the seed to the random generator
	 * 
	 * @param seed
	 *            the seed
	 */
	public static void setRandomSeed(long seed) {
		random.setSeed(seed);
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the parameter
	 */
	public HadoopParameter getParameter() {
		return parameter;
	}

	/**
	 * @return the effect
	 */
	public ParamTaskEffect getEffect() {
		return effect;
	}

	/* ***************************************************************
	 * ABSTRACT METHODS
	 * ***************************************************************
	 */

	/**
	 * Get the number of unique value from the parameter's domain
	 * 
	 * Note: It's possible for the number of unique values to equal
	 * Integer.MAX_VALUE
	 * 
	 * @return the number of unique values
	 */
	public abstract int getNumUniqueValues();

	/**
	 * Returns a list of equi-spaced values from the parameter's domain.
	 * 
	 * Note that the list returned may have less than numValues values. For
	 * example, if numValues = 10 but the domain is the integer numbers between
	 * 1 and 5, than the returned list will only contain 5 values.
	 * 
	 * @param numValues
	 *            the number of values
	 * @return a list of values
	 */
	public abstract List<String> getEquiSpacedValues(int numValues);

	/**
	 * Returns a list of random values from the parameter's domain.
	 * 
	 * Note that the list returned may have less than numValues values. For
	 * example, if numValues = 10 but the domain is the integer numbers between
	 * 1 and 5, than the returned list will only contain 5 values.
	 * 
	 * @param numValues
	 *            the number of values
	 * @return a list of values
	 */
	public abstract List<String> getRandomValues(int numValues);

	/**
	 * Returns a random value from the parameter's domain.
	 * 
	 * @return a random value
	 */
	public abstract String getRandomValue();

	/**
	 * Returns a random value z from the parameter's domain D = (lower, upper)
	 * such that |z - center| < scale * (upper - lower)
	 * 
	 * @param center
	 *            the center value
	 * @param scale
	 *            the scale
	 * @return a random value
	 */
	public abstract String getRandomValue(String center, double scale);

}
