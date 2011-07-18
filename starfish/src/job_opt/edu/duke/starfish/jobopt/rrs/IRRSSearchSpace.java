package edu.duke.starfish.jobopt.rrs;

import java.util.List;

/**
 * Interface representing the search space used by recursive random search.
 * 
 * @author hero
 */
public interface IRRSSearchSpace<P> {

	/**
	 * Get an empty space point
	 * 
	 * @return an empty space point
	 */
	public P getEmptySpacePoint();

	/**
	 * Get the number of dimensions in the space
	 * 
	 * @return the number of dimensions in the space
	 */
	public int getNumDimensions();

	/**
	 * Get the number of unique points in the space (the product of the unique
	 * parameter values from each parameter's domain)
	 * 
	 * @return the number of unique points in the space
	 */
	public int getNumUniqueSpacePoints();

	/**
	 * Returns a random parameter space point in the space
	 * 
	 * @return a parameter space point
	 */
	public P getRandomSpacePoint();

	/**
	 * Returns a random parameter space point Z = z_1, ..., z_k in the space
	 * such that |z_i - center_i| < scale * (upper_i - lower_i) for i=1...k
	 * 
	 * The scale factor shows how much we want to scale down the entire space.
	 * For example, if scale = 0.5, then we want to draw sample from half the
	 * space. This mean that we need to scale each parameter by 'scale ^ (1 /
	 * n)', where n = the number of parameters in the space.
	 * 
	 * @param center
	 *            the center point
	 * @param scale
	 *            the scale factor for the parameter space
	 * @return a random space point
	 */
	public P getRandomSpacePoint(P center, double scale);

	/**
	 * Generate a list of parameter space points that represents the Cartesian
	 * product of parameter values (a.k.a. Gridding).
	 * 
	 * Typically, gridding is done using equi-spaced values from the parameter
	 * domains. This function also support using random values from the
	 * parameter domains.
	 * 
	 * @param random
	 *            whether to generate random or equi-spaced values
	 * @param numValuesPerParam
	 *            the max number of values to obtain from each parameter
	 * @return a list of parameter space points
	 */
	public List<P> getSpacePointGrid(boolean random, int numValuesPerParam);

}
