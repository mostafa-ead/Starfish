package edu.duke.starfish.visualizer.model.transfers;

/**
 * Contains the data transfers between two hosts. We keep track of the data
 * going both ways separately. We also keep track of how much data was transfer
 * in pre-specified time intervals.
 * 
 * @author hero
 */
public class HostTransfers {

	private String host1;
	private String host2;
	private long totalData1;
	private long totalData2;
	private long[] data1PerInterval;
	private long[] data2PerInterval;
	private long maxData1InInterval;
	private long maxData2InInterval;

	/**
	 * @param host1
	 * @param host2
	 * @param numIntervals
	 */
	public HostTransfers(String host1, String host2, int numIntervals) {
		this.host1 = host1;
		this.host2 = host2;
		this.totalData1 = 0l;
		this.totalData2 = 0l;
		this.data1PerInterval = new long[numIntervals];
		this.data2PerInterval = new long[numIntervals];
		this.maxData1InInterval = 0l;
		this.maxData2InInterval = 0l;

		for (int i = 0; i < numIntervals; ++i) {
			data1PerInterval[i] = 0l;
			data2PerInterval[i] = 0l;
		}
	}

	/**
	 * @return the host1
	 */
	public String getHost1() {
		return host1;
	}

	/**
	 * @return the host2
	 */
	public String getHost2() {
		return host2;
	}

	/**
	 * @return the data1
	 */
	public long getTotalData1() {
		return totalData1;
	}

	/**
	 * @return the data2
	 */
	public long getTotalData2() {
		return totalData2;
	}

	/**
	 * @return the data1PerInterval
	 */
	public long[] getData1PerInterval() {
		return data1PerInterval;
	}

	/**
	 * @return the data2PerInterval
	 */
	public long[] getData2PerInterval() {
		return data2PerInterval;
	}

	/**
	 * @return the max data size1 in an interval
	 */
	public long getMaxData1InInterval() {
		return maxData1InInterval;
	}

	/**
	 * @return the max data size3 in an interval
	 */
	public long getMaxData2InInterval() {
		return maxData2InInterval;
	}

	/**
	 * 
	 * @param beginInterval
	 * @param endInterval
	 * @param data1
	 */
	public void addData1(int beginInterval, int endInterval, long data1) {
		totalData1 += data1;

		for (int i = beginInterval; i <= endInterval; ++i) {
			data1PerInterval[i] += data1;

			if (data1PerInterval[i] > maxData1InInterval)
				maxData1InInterval = data1PerInterval[i];
		}
	}

	/**
	 * 
	 * @param beginInterval
	 * @param endInterval
	 * @param data2
	 */
	public void addData2(int beginInterval, int endInterval, long data2) {
		totalData2 += data2;

		for (int i = beginInterval; i <= endInterval; ++i) {
			data2PerInterval[i] += data2;

			if (data2PerInterval[i] > maxData2InInterval)
				maxData2InInterval = data2PerInterval[i];
		}
	}

}
