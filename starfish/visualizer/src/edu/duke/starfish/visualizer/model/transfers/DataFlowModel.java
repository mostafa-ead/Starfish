package edu.duke.starfish.visualizer.model.transfers;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;

/**
 * Represents the model for the Data Flow view
 * 
 * @author hero
 */
public class DataFlowModel {

	private MRJobInfo mrJob;
	private HashMap<HostPair, HostTransfers> transfers;
	private long minTransfer;
	private long maxTransfer;
	private long maxTransferInInteval;
	private int numIntervals;

	/**
	 * Constructor
	 * 
	 * @param mrJob
	 *            the MR job
	 * @param numIntervals
	 *            the number of time intervals to use
	 */
	public DataFlowModel(MRJobInfo mrJob, int numIntervals) {
		this.mrJob = mrJob;
		this.numIntervals = numIntervals;
		this.transfers = new HashMap<HostPair, HostTransfers>();

		loadDataTransfers();
		calcTransferStats();
	}

	/**
	 * @return the minTransfer
	 */
	public long getMinTransfer() {
		return minTransfer;
	}

	/**
	 * @return the maxTransfer
	 */
	public long getMaxTransfer() {
		return maxTransfer;
	}

	/**
	 * @return the maxTransferInInteval
	 */
	public long getMaxTransferInInterval() {
		return maxTransferInInteval;
	}

	/**
	 * @return the host transfers
	 */
	public Collection<HostTransfers> getHostTransfers() {
		return transfers.values();
	}

	/**
	 * Load the transfers among the attempts into the map with the transfers
	 * among the hosts
	 */
	private void loadDataTransfers() {

		List<DataTransfer> attemptTransfers = mrJob.getDataTransfers();
		long jobStartTime = mrJob.getStartTime().getTime();
		long jobEndTime = mrJob.getEndTime().getTime();
		double timePerInterval = (jobEndTime - jobStartTime)
				/ (double) numIntervals;

		String source, target;
		int startInterval, endInterval;
		HostPair pair;
		HostTransfers hostTranfer;

		for (DataTransfer transfer : attemptTransfers) {
			// Get the source and target hosts
			source = transfer.getSource().getTaskTracker().getHostName();
			target = transfer.getDestination().getTaskTracker().getHostName();
			pair = new HostPair(source, target);

			// Calculate the intervals
			startInterval = (int) Math
					.floor((transfer.getStartTime().getTime() - jobStartTime)
							/ timePerInterval);
			endInterval = (int) Math
					.floor((transfer.getEndTime().getTime() - jobStartTime)
							/ timePerInterval);

			// Add the transfer
			if (!transfers.containsKey(pair)) {
				transfers.put(pair, new HostTransfers(pair.getHost1(), pair
						.getHost2(), numIntervals));
			}

			hostTranfer = transfers.get(pair);
			if (hostTranfer.getHost1().equals(source)) {
				hostTranfer.addData1(startInterval, endInterval, transfer
						.getComprData());
			} else {
				hostTranfer.addData2(startInterval, endInterval, transfer
						.getComprData());
			}
		}
	}

	/**
	 * Calculate the min and max transfers
	 */
	private void calcTransferStats() {
		minTransfer = 0l;
		maxTransfer = 0l;
		maxTransferInInteval = 0l;
		long current = 0l;

		for (HostTransfers transfer : transfers.values()) {
			// Get the min and max total transfer
			current = transfer.getTotalData1() + transfer.getTotalData2();
			if (current < minTransfer) {
				minTransfer = current;
			}
			if (current > maxTransfer) {
				maxTransfer = current;
			}

			// Get the max transfer within an interval
			current = transfer.getMaxData1InInterval()
					+ transfer.getMaxData2InInterval();
			if (current > maxTransferInInteval) {
				maxTransferInInteval = current;
			}
		}
	}

	/**
	 * A simple class to represent a pair of hosts. The order of the hosts in
	 * the pair does not matter for comparison purposes
	 * 
	 * @author hero
	 * 
	 */
	private class HostPair {
		private String host1;
		private String host2;

		/**
		 * @param host1
		 * @param host2
		 */
		public HostPair(String host1, String host2) {
			this.host1 = host1;
			this.host2 = host2;
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

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return 31 * (host1.hashCode() + host2.hashCode());
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof HostPair))
				return false;
			HostPair other = (HostPair) obj;
			return (host1.equals(other.host1) && host2.equals(other.host2))
					|| (host1.equals(other.host2) && host2.equals(other.host1));
		}

	}
}
