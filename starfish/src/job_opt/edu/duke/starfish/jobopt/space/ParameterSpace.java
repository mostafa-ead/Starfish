package edu.duke.starfish.jobopt.space;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;

import edu.duke.starfish.jobopt.params.HadoopParameter;
import edu.duke.starfish.jobopt.params.ParamTaskEffect;
import edu.duke.starfish.jobopt.params.ParameterDescriptor;

/**
 * Represents the entire Hadoop configuration parameter space
 * 
 * @author hero
 */
public class ParameterSpace {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private EnumMap<HadoopParameter, ParameterDescriptor> paramDescriptors;

	/**
	 * Default Constructor. The parameter space is empty.
	 */
	public ParameterSpace() {
		paramDescriptors = new EnumMap<HadoopParameter, ParameterDescriptor>(
				HadoopParameter.class);
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add a parameter descriptor in the space
	 * 
	 * @param descriptor
	 *            the parameter descriptor
	 */
	public void addParameterDescriptor(ParameterDescriptor descriptor) {
		paramDescriptors.put(descriptor.getParameter(), descriptor);

	}

	/**
	 * Returns true if the parameter space contains a descriptor for the
	 * provided parameter
	 * 
	 * @param param
	 *            the parameter of interest
	 * @return true or false
	 */
	public boolean containsParamDescriptor(HadoopParameter param) {
		return paramDescriptors.containsKey(param);
	}

	/**
	 * Get the number of parameters in the space
	 * 
	 * @return the number of parameters in the space
	 */
	public int getNumParameters() {
		return paramDescriptors.size();
	}

	/**
	 * Get the number of unique points in the space (the product of the unique
	 * parameter values from each parameter's domain)
	 * 
	 * @return the number of unique points in the space
	 */
	public int getNumUniqueSpacePoints() {

		if (paramDescriptors.size() == 0)
			return 0;

		long numPoints = 1;
		for (ParameterDescriptor desc : paramDescriptors.values()) {
			if (desc.getNumUniqueValues() == Integer.MAX_VALUE)
				return Integer.MAX_VALUE;
			else
				numPoints *= (long) desc.getNumUniqueValues();
		}

		if (numPoints > Integer.MAX_VALUE)
			return Integer.MAX_VALUE;
		else
			return (int) numPoints;
	}

	/**
	 * Get a particular parameter descriptor
	 * 
	 * @param param
	 *            the parameter
	 * @return the parameter descriptor
	 */
	public ParameterDescriptor getParameterDescriptor(HadoopParameter param) {
		return paramDescriptors.get(param);
	}

	/**
	 * Get all the parameter descriptors in the parameter space
	 * 
	 * @return the parameter descriptors
	 */
	public Collection<ParameterDescriptor> getParameterDescriptors() {
		return paramDescriptors.values();
	}

	/**
	 * Get all the parameter descriptors in the parameter space with the
	 * provided effect
	 * 
	 * @param effect
	 *            the task effect of the parameters
	 * @return the parameter descriptors
	 */
	public Collection<ParameterDescriptor> getParameterDescriptors(
			ParamTaskEffect effect) {
		ArrayList<ParameterDescriptor> params = new ArrayList<ParameterDescriptor>();

		for (ParameterDescriptor param : paramDescriptors.values()) {
			if (param.getEffect() == effect)
				params.add(param);
		}

		return params;
	}

	/**
	 * Returns a random parameter space point in the space
	 * 
	 * @return a parameter space point
	 */
	public ParameterSpacePoint getRandomSpacePoint() {
		ParameterSpacePoint point = new ParameterSpacePoint();

		for (ParameterDescriptor descr : paramDescriptors.values()) {
			point.addParamValue(descr.getParameter(), descr.getRandomValue());
		}

		return point;
	}

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
	public ParameterSpacePoint getRandomSpacePoint(ParameterSpacePoint center,
			double scale) {
		ParameterSpacePoint point = new ParameterSpacePoint();
		double pScale = Math.pow(scale, 1.0d / paramDescriptors.size());

		for (ParameterDescriptor descr : paramDescriptors.values()) {
			point.addParamValue(descr.getParameter(), descr.getRandomValue(
					center.getParameterValue(descr.getParameter()), pScale));
		}

		return point;
	}

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
	public List<ParameterSpacePoint> getSpacePointGrid(boolean random,
			int numValuesPerParam) {
		ArrayList<ParameterSpacePoint> points = new ArrayList<ParameterSpacePoint>();

		// Generate all the parameter space points
		for (ParameterDescriptor descriptor : paramDescriptors.values()) {

			HadoopParameter param = descriptor.getParameter();
			List<String> values = (random) ? descriptor
					.getRandomValues(numValuesPerParam) : descriptor
					.getEquiSpacedValues(numValuesPerParam);

			int numValues = values.size();
			if (numValues == 0)
				continue;

			if (points.size() == 0) {
				// First parameter. Add one point for each value
				for (String value : values) {
					ParameterSpacePoint point = new ParameterSpacePoint();
					point.addParamValue(param, value);
					points.add(point);
				}
			} else {
				// Add the first value to all existing points
				for (ParameterSpacePoint point : points) {
					point.addParamValue(param, values.get(0));
				}

				// Perform the Cartesian product of points with rest values
				int numExistingPoints = points.size();
				for (int i = 1; i < numValues; ++i) {

					// Duplicate the existing points
					points.ensureCapacity(points.size() + numExistingPoints);
					for (int j = 0; j < numExistingPoints; ++j) {
						points.add(new ParameterSpacePoint(points.get(j)));
					}

					// Set the values
					int start = i * numExistingPoints;
					int end = (i + 1) * numExistingPoints;
					for (int j = start; j < end; ++j) {
						points.get(j).addParamValue(param, values.get(i));
					}
				}
			}
		}

		return points;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Parameter Space:\n");

		for (ParameterDescriptor descr : paramDescriptors.values()) {
			sb.append(descr.toString());
			sb.append("\n");
		}

		return sb.toString();
	}

}
