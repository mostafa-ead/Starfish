package edu.duke.starfish.jobopt.rrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * Recursive Random Search is a black-box global optimization algorithm that
 * tries to find the optimal space point in a search space.
 * 
 * Reference: A recursive random search algorithm for large-scale network
 * parameter configuration, Tao Ye and Shivkumar Kalyanaraman, Proceeding
 * SIGMETRICS '03, ACM
 * 
 * Parameters:
 * <ul>
 * <li>p = starfish.job.optimizer.explore.confidence.prob (0.99)</li>
 * <li>r = starfish.job.optimizer.explore.percentile (0.1)</li>
 * <li>q = starfish.job.optimizer.exploit.confidence.prob (0.99)</li>
 * <li>v = starfish.job.optimizer.exploit.expected.value (0.8)</li>
 * <li>c = starfish.job.optimizer.exploit.reduction.ratio (0.5)</li>
 * <li>st = starfish.job.optimizer.exploit.termination.size (0.001)</li>
 * </ul>
 * 
 * Pseudocode:
 * 
 * <pre>
 * Initialize exploration parameters p, r, n <- ln(1 − p)/ ln(1 − r) ;
 * Initialize exploitation parameters q, υ, c, st , l <- ln(1 − q)/ ln(1 − υ);
 * Take n random samples xi, i = 1 . . . n from parameter space D;
 * x0 <- arg min 1<=i<=n (f(xi)), yr <- f(x0), add f(x0) to the threshold set F;
 * i <- 0, exploit_flag <- 1, xopt <- x0 ;
 * while stopping criterion is not satisﬁed do
 *     if exploit_flag = 1 then
 *         // Exploit ﬂag is set, start exploitation process
 *         j <- 0, fc <- f(x0), xl <- x0 , ρ <- r;
 *         while ρ > st do
 *             Take a random sample x' from ND,ρ(xl);
 *             if f(x') < fc then
 *                 // Find a better point, re-align the center of sample space to the new point
 *                 xl <- x' , fc <- f(x');
 *                 j <- 0;
 *             else
 *                 j <- j + 1;
 *             endif
 *             if j = l then
 *                 // Fail to ﬁnd a better point, shrink the sample space
 *                 ρ <- c * ρ, j <- 0;
 *             endif
 *         endw
 *         exploit_flag <- 0, update xopt if f(xl) < f(xopt);
 *     endif
 *     Take a random sample x0 from D;
 *     if f(x0) < yr then
 *         // Find a promising point, set the ﬂag to exploit
 *         exploit_flag <- 1;
 *     endif
 *     if i = n then
 *         // Update the exploitation threshold every n samples in the parameter space
 *         Add min 1<=i<=n (f(xi)) to the threshold set F;
 *         yr <- mean(F), i <- 0;
 *     endif
 *     i <- i + 1;
 * endw
 * 
 * </pre>
 * 
 * @author hero
 */
public class RecursiveRandomSearch<P> {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Constants
	public static final String RRS_EXPLORE_CONF_PROB = "starfish.job.optimizer.explore.confidence.prob";
	public static final String RRS_EXPLORE_PERC = "starfish.job.optimizer.explore.percentile";
	public static final String RRS_EXPLOIT_CONF_PROB = "starfish.job.optimizer.exploit.confidence.prob";
	public static final String RRS_EXPLOIT_EXP_VALUE = "starfish.job.optimizer.exploit.expected.value";
	public static final String RRS_EXPLOIT_RED_RATIO = "starfish.job.optimizer.exploit.reduction.ratio";
	public static final String RRS_EXPLOIT_TERM_SIZE = "starfish.job.optimizer.exploit.termination.size";

	// Default values
	public static final float RRS_DEF_EXPLORE_CONF_PROB = 0.99f;
	public static final float RRS_DEF_EXPLORE_PERC = 0.1f;
	public static final float RRS_DEF_EXPLOIT_CONF_PROB = 0.99f;
	public static final float RRS_DEF_EXPLOIT_EXP_VALUE = 0.8f;
	public static final float RRS_DEF_EXPLOIT_RED_RATIO = 0.5f;
	public static final float RRS_DEF_EXPLOIT_TERM_SIZE = 0.001f;

	// Exploration parameters
	float p;
	float r;
	int n;

	// Exploitation parameters
	float q;
	float v;
	float c;
	float s_t;
	int l;

	/**
	 * Constructor
	 * 
	 * @param conf
	 *            settings with (optional) RRS parameters
	 */
	public RecursiveRandomSearch(Configuration conf) {
		// Initialize exploration parameters
		p = conf.getFloat(RRS_EXPLORE_CONF_PROB, RRS_DEF_EXPLORE_CONF_PROB);
		r = conf.getFloat(RRS_EXPLORE_PERC, RRS_DEF_EXPLORE_PERC);
		n = (int) Math.round(Math.log(1 - p) / Math.log(1 - r));

		// Initialize exploitation parameters
		q = conf.getFloat(RRS_EXPLOIT_CONF_PROB, RRS_DEF_EXPLOIT_CONF_PROB);
		v = conf.getFloat(RRS_EXPLOIT_EXP_VALUE, RRS_DEF_EXPLOIT_EXP_VALUE);
		c = conf.getFloat(RRS_EXPLOIT_RED_RATIO, RRS_DEF_EXPLOIT_RED_RATIO);
		s_t = conf.getFloat(RRS_EXPLOIT_TERM_SIZE, RRS_DEF_EXPLOIT_TERM_SIZE);
		l = (int) Math.round(Math.log(1 - q) / Math.log(1 - v));
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Performs recursive random search to find the best point in the provided
	 * space
	 * 
	 * @param space
	 *            the parameter space
	 * @param costEngine
	 *            the cost engine
	 * @return the best parameter point
	 */
	public P findBestSpacePoint(IRRSSearchSpace<P> space,
			IRRSCostEngine<P> costEngine) {

		// Check for empty space
		if (space.getNumDimensions() == 0)
			return space.getEmptySpacePoint();

		// Special case for very small spaces
		if (space.getNumUniqueSpacePoints() < n) {
			// Enumerate the full space
			List<P> points = space.getSpacePointGrid(false, n);
			return findBestSpacePoint(points, costEngine);
		}

		// Take n random samples from the parameter space
		int countWhatIf = 0;
		List<P> x_array = new ArrayList<P>(n);
		double[] f_x_array = new double[n];
		for (int i = 0; i < n; ++i) {
			x_array.add(space.getRandomSpacePoint());
			f_x_array[i] = costEngine.costSpacePoint(x_array.get(i));
			++countWhatIf;
		}

		// Find the min point
		int minIndex = findMinIndex(f_x_array);
		P x_0 = x_array.get(minIndex);
		double f_x_0 = f_x_array[minIndex];
		double y_r = f_x_0;

		// Initialize the threshold list
		ArrayList<Double> thresList = new ArrayList<Double>();
		thresList.add(f_x_0);

		// Initialize the optimal point
		P x_opt = x_0;
		double f_x_opt = f_x_0;
		int lastCountOptChange = countWhatIf;

		// Calculate termination criteria as scaled exponential functions
		// of the number of parameters in the space
		int MAX_COUNT_WHAT_IF = (int) Math.ceil(150 * Math.pow(
				space.getNumDimensions(), 1.2));
		int MAX_COUNT_OPT_CHANGE = (int) Math.ceil(80 * Math.pow(
				space.getNumDimensions(), 1.2));

		// Start the main exploration search
		int i = 0;
		boolean exploit = true;

		while (countWhatIf < MAX_COUNT_WHAT_IF
				&& countWhatIf - lastCountOptChange < MAX_COUNT_OPT_CHANGE) {

			if (exploit) {
				// Start the exploitation process
				int j = 0;
				P x_l = x_0;
				double f_x_l = f_x_0;
				P x_prime;
				double f_x_prime = 0;
				float ro = r;

				while (ro > s_t) {
					x_prime = space.getRandomSpacePoint(x_l, ro);
					f_x_prime = costEngine.costSpacePoint(x_prime);
					++countWhatIf;

					if (f_x_prime < f_x_l) {
						// Re-align the center of sample space
						x_l = x_prime;
						f_x_l = f_x_prime;
						j = 0;
					} else {
						++j;
					}

					if (j == l) {
						// Shrink the sample space
						ro *= c;
						j = 0;
					}
				} // End while

				// Update the optimal point
				exploit = false;
				if (f_x_l < f_x_opt) {
					x_opt = x_l;
					f_x_opt = f_x_l;
					lastCountOptChange = countWhatIf;
				}
			} // End exploitation

			// Take a new random space point
			x_0 = space.getRandomSpacePoint();
			f_x_0 = costEngine.costSpacePoint(x_0);
			++countWhatIf;

			x_array.set(i, x_0);
			f_x_array[i] = f_x_0;

			if (f_x_0 < y_r) {
				// Found a promising point, set the flag to exploit
				exploit = true;
			}

			++i;
			if (i == n) {
				// Update the exploitation threshold y_r
				minIndex = findMinIndex(f_x_array);
				thresList.add(f_x_array[minIndex]);
				y_r = findMean(f_x_array);
				i = 0;
			}
		} // End exploration

		return x_opt;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Find the best space point from the collection of points
	 * 
	 * @param points
	 *            the collection of space points
	 * @param costEngine
	 *            the cost engine
	 * @return the best space point
	 */
	private P findBestSpacePoint(Collection<P> points,
			IRRSCostEngine<P> costEngine) {

		// Find the best parameter space point
		double minTime = Double.MAX_VALUE;
		double currTime = 0;
		P bestPoint = null;

		for (P point : points) {

			// Cost each parameter space point
			currTime = costEngine.costSpacePoint(point);

			if (currTime < minTime) {
				minTime = currTime;
				bestPoint = point;
			}
		}

		return bestPoint;
	}

	/**
	 * Find the index of the smallest value in the array
	 * 
	 * @param values
	 *            the array of values
	 * @return the index of the smallest value
	 */
	private int findMinIndex(double[] values) {
		int minIndex = 0;
		double minValue = values[0];

		for (int i = 1; i < values.length; ++i) {
			if (values[i] < minValue) {
				minValue = values[i];
				minIndex = i;
			}
		}

		return minIndex;
	}

	/**
	 * Find the mean of the values in the array
	 * 
	 * @param values
	 *            array of values
	 * @return the mean
	 */
	private double findMean(double[] values) {
		double sum = 0;
		for (int i = 0; i < values.length; ++i) {
			sum += values[i];
		}

		return sum / values.length;
	}

}
