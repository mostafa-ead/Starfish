package edu.duke.starfish.visualizer.model;

/**
 * Builds an equi-space histogram
 * 
 * @author hero
 */
public class Histogram {

	private Double data[];
	private int numBuckets;

	private int freq[];
	private Double minValue;
	private Double maxValue;
	private Double unit;

	/**
	 * @param data
	 * @param numBuckets
	 */
	public Histogram(Double[] data, int numBuckets) {
		this.data = data;
		this.numBuckets = numBuckets;

		this.freq = new int[numBuckets];
		if (data.length > 0 && numBuckets > 0) {
			buildHistogram();
		}
	}

	/**
	 * @return the frequencies
	 */
	public int[] getFrequencies() {
		return freq;
	}

	/**
	 * @return the minValue
	 */
	public Double getMinValue() {
		return minValue;
	}

	/**
	 * @return the maxValue
	 */
	public Double getMaxValue() {
		return maxValue;
	}

	/**
	 * @return the unit
	 */
	public Double getUnit() {
		return unit;
	}

	/**
	 * Main function to build the histogram
	 */
	private void buildHistogram() {

		// Initialize the buckets
		initBuckets();

		// Count the frequencies
		int index;
		for (int i = 0; i < data.length; ++i) {
			index = (int) Math.floor((data[i] - minValue) / unit);
			++freq[index];
		}
	}

	/**
	 * Initialize the buckets, i.e., the min, max, and unit
	 */
	private void initBuckets() {

		// Find the min and max values
		minValue = data[0];
		maxValue = data[0];
		for (int i = 1; i < data.length; ++i) {
			if (data[i] < minValue)
				minValue = data[i];
			if (data[i] > maxValue)
				maxValue = data[i];
		}

		// Expand the min and max values
		minValue = Math.floor(minValue * 0.9);
		maxValue = Math.ceil(maxValue * 1.02);

		// Do some rounding
		if (maxValue - minValue > 50) {
			minValue = Math.floor(minValue / 10) * 10;
			maxValue = Math.ceil(maxValue / 10) * 10;
		}

		// Calculate the unit
		unit = Math.ceil((maxValue - minValue) / numBuckets);
		maxValue = minValue + unit * numBuckets;
	}

}
