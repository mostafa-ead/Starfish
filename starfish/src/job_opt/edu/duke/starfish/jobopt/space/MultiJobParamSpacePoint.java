package edu.duke.starfish.jobopt.space;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents a space point that is formed from the union of parameter points
 * for multiple jobs.
 * 
 * @author hero
 */
public class MultiJobParamSpacePoint {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private Map<Integer, ParameterSpacePoint> jobPoints;

	/**
	 * Default Constructor
	 */
	public MultiJobParamSpacePoint() {
		this.jobPoints = new HashMap<Integer, ParameterSpacePoint>(2);
	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 *            the other multi-job parameter space point to copy
	 */
	public MultiJobParamSpacePoint(MultiJobParamSpacePoint other) {
		jobPoints = new HashMap<Integer, ParameterSpacePoint>(
				other.jobPoints.size());

		for (Entry<Integer, ParameterSpacePoint> entry : other.jobPoints
				.entrySet()) {
			jobPoints.put(entry.getKey(),
					new ParameterSpacePoint(entry.getValue()));
		}
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add a job-specific parameter space point
	 * 
	 * @param jobId
	 *            the job ID
	 * @param jobPoint
	 *            the job parameter space point to set
	 */
	public void addJobSpacePoint(int jobId, ParameterSpacePoint jobPoint) {
		jobPoints.put(jobId, jobPoint);
	}

	/**
	 * Get the parameter space point for a job
	 * 
	 * @param jobId
	 *            the job id of interest
	 * @return the job parameter space point
	 */
	public ParameterSpacePoint getJobSpacePoint(int jobId) {
		return jobPoints.get(jobId);
	}

	/**
	 * Merge the provided multi-job parameter space point with the current point
	 * by copying (shallow copy) the other's parameter space points.
	 * 
	 * @param other
	 *            the multi-job parameter space point to merge.
	 */
	public void mergeMultiJobParamSpacePoint(MultiJobParamSpacePoint other) {

		for (Entry<Integer, ParameterSpacePoint> entry : other.jobPoints
				.entrySet()) {
			Integer key = entry.getKey();
			if (this.jobPoints.containsKey(key)) {
				this.jobPoints.get(key).addParamValues(entry.getValue());
			} else {
				this.jobPoints.put(key, entry.getValue());
			}
		}
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 31 + 37 * ((jobPoints == null) ? 0 : jobPoints.hashCode());
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
		if (getClass() != obj.getClass())
			return false;
		MultiJobParamSpacePoint other = (MultiJobParamSpacePoint) obj;
		if (jobPoints == null) {
			if (other.jobPoints != null)
				return false;
		} else if (!jobPoints.equals(other.jobPoints))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MultiJobParamSpacePoint [points=" + jobPoints + "]";
	}

}
