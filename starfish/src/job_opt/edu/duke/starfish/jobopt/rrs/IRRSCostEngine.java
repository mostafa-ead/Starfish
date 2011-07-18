package edu.duke.starfish.jobopt.rrs;

/**
 * Interface for a cost engine knows how to cost a space point
 * 
 * @author hero
 */
public interface IRRSCostEngine<P> {

	/**
	 * Return the cost of this provided space point
	 * 
	 * @param point
	 *            the space point to cost
	 * @return the cost
	 */
	public double costSpacePoint(P point);
}
