package edu.duke.starfish.jobopt.space;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.duke.starfish.jobopt.rrs.IRRSSearchSpace;

/**
 * Represents a space that corresponds to the union of job-level parameter
 * spaces for multiple jobs.
 * 
 * @author hero
 */
public class MultiJobParameterSpace implements
		IRRSSearchSpace<MultiJobParamSpacePoint> {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private Map<Integer, ParameterSpace> spaces;

	/**
	 * Default Constructor
	 */
	public MultiJobParameterSpace() {
		this.spaces = new HashMap<Integer, ParameterSpace>(2);
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add a new parameter space associated with a single job
	 * 
	 * @param jobId
	 *            the job id
	 * @param space
	 *            the parameter space to set
	 */
	public void addParamSpace(Integer jobId, ParameterSpace space) {
		spaces.put(jobId, space);
	}

	/**
	 * Get the parameter space associated with a single job
	 * 
	 * @param jobId
	 *            the job id
	 * @return the parameter space
	 */
	public ParameterSpace getParamSpace(Integer jobId) {
		return spaces.get(jobId);
	}

	/**
	 * Get an empty multi-job space point defined as an empty job space for each
	 * job id.
	 * 
	 * @return an empty multi-job space point
	 */
	@Override
	public MultiJobParamSpacePoint getEmptySpacePoint() {

		MultiJobParamSpacePoint emptyPoint = new MultiJobParamSpacePoint();
		for (Entry<Integer, ParameterSpace> entry : spaces.entrySet())
			emptyPoint.addJobSpacePoint(entry.getKey(), entry.getValue()
					.getEmptySpacePoint());

		return emptyPoint;
	}

	/**
	 * Get the number of dimensions in the space as the sum of the dimensions of
	 * the job-level parameter spaces
	 * 
	 * @return the number of dimensions in the space
	 */
	@Override
	public int getNumDimensions() {
		// Add the dimensions from each job space (i.e., union)
		int dims = 0;
		for (ParameterSpace space : spaces.values()) {
			dims += space.getNumDimensions();
		}

		return dims;
	}

	/**
	 * Get the number of unique points in the space (the product of the unique
	 * parameter points from each parameter space)
	 * 
	 * @return the number of unique points in the space
	 */
	@Override
	public int getNumUniqueSpacePoints() {

		if (spaces.size() == 0)
			return 0;

		// The number of unique points is the product of the individual
		// parameter spaces
		long numPoints = 1;
		for (ParameterSpace space : spaces.values()) {
			if (space.getNumUniqueSpacePoints() == Integer.MAX_VALUE)
				return Integer.MAX_VALUE;
			else if (space.getNumUniqueSpacePoints() != 0)
				numPoints *= (long) space.getNumUniqueSpacePoints();
		}

		if (numPoints > Integer.MAX_VALUE)
			return Integer.MAX_VALUE;
		else
			return (int) numPoints;
	}

	/**
	 * Returns a random multi-job parameter space point in the space
	 * 
	 * @return a parameter space point
	 */
	@Override
	public MultiJobParamSpacePoint getRandomSpacePoint() {

		MultiJobParamSpacePoint point = new MultiJobParamSpacePoint();

		for (Entry<Integer, ParameterSpace> entry : spaces.entrySet()) {
			point.addJobSpacePoint(entry.getKey(), entry.getValue()
					.getRandomSpacePoint());
		}

		return point;
	}

	/**
	 * Returns a random parameter space point Z = z_1, ..., z_k in the space
	 * such that |z_i - center_i| < scale * (upper_i - lower_i) for i=1...k
	 * 
	 * The scale factor shows how much we want to scale down the entire space.
	 * For example, if scale = 0.5, then we want to draw sample from half the
	 * multi-job space. This mean that we need to scale each parameter space by
	 * 'scale ^ (1 / n)', where n = the number of parameter spaces in the
	 * multi-job space.
	 * 
	 * @param center
	 *            the center point
	 * @param scale
	 *            the scale factor for the multi-job parameter space
	 * @return a random multi-job space point
	 */
	@Override
	public MultiJobParamSpacePoint getRandomSpacePoint(
			MultiJobParamSpacePoint center, double scale) {

		MultiJobParamSpacePoint point = new MultiJobParamSpacePoint();
		double pScale = Math.pow(scale, 1.0d / spaces.size());

		for (Entry<Integer, ParameterSpace> entry : spaces.entrySet()) {
			point.addJobSpacePoint(entry.getKey(), entry.getValue()
					.getRandomSpacePoint(
							center.getJobSpacePoint(entry.getKey()), pScale));
		}

		return point;
	}

	/**
	 * Generate a list of multi-job parameter space points that represents the
	 * Cartesian product of parameter space points (a.k.a. Gridding).
	 * 
	 * @param random
	 *            whether to generate random or equi-spaced values
	 * @param numValuesPerParam
	 *            the max number of values to obtain from each parameter in each
	 *            parameter space
	 * @return a list of parameter space points
	 */
	@Override
	public List<MultiJobParamSpacePoint> getSpacePointGrid(boolean random,
			int numValuesPerParam) {

		ArrayList<MultiJobParamSpacePoint> result = new ArrayList<MultiJobParamSpacePoint>();

		// Generate all the parameter space points
		for (Entry<Integer, ParameterSpace> entry : spaces.entrySet()) {

			Integer jobId = entry.getKey();
			List<ParameterSpacePoint> jobPoints = entry.getValue()
					.getSpacePointGrid(random, numValuesPerParam);

			int numPoints = jobPoints.size();
			if (numPoints == 0) {
				jobPoints.add(entry.getValue().getEmptySpacePoint());
				++numPoints;
			}

			if (result.size() == 0) {
				// First parameter. Add one point for each value
				for (ParameterSpacePoint jobPoint : jobPoints) {
					MultiJobParamSpacePoint point = new MultiJobParamSpacePoint();
					point.addJobSpacePoint(jobId, jobPoint);
					result.add(point);
				}
			} else {
				// Add the first value to all existing points
				for (MultiJobParamSpacePoint point : result) {
					point.addJobSpacePoint(jobId, jobPoints.get(0));
				}

				// Perform the Cartesian product of points with rest values
				int numExistingPoints = result.size();
				for (int i = 1; i < numPoints; ++i) {

					// Duplicate the existing points
					result.ensureCapacity(result.size() + numExistingPoints);
					for (int j = 0; j < numExistingPoints; ++j) {
						result.add(new MultiJobParamSpacePoint(result.get(j)));
					}

					// Set the values
					int start = i * numExistingPoints;
					int end = (i + 1) * numExistingPoints;
					for (int j = start; j < end; ++j) {
						result.get(j).addJobSpacePoint(jobId, jobPoints.get(i));
					}
				}
			}
		}

		return result;
	}

}
