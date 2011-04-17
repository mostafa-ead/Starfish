package edu.duke.starfish.visualizer.model.timeline;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRCleanupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;

/**
 * Represents a task tracker
 * 
 * @author hero
 */
public class TimelineTaskTracker {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private String hostName;
	private int hostId;
	private int numMapSlots;
	private int numReduceSlots;
	private TimelineTaskSlot[] mapSlots;
	private TimelineTaskSlot[] redSlots;
	private TimelineTaskSlot[] allSlots;

	/**
	 * Constructor
	 * 
	 * @param hostName
	 *            the host name
	 * @param hostId
	 *            the host id
	 * @param numMapSlots
	 *            the number of map slots
	 * @param numReduceSlots
	 *            the number of reduce slots
	 */
	public TimelineTaskTracker(String hostName, int hostId, int numMapSlots,
			int numReduceSlots) {
		this.hostName = hostName;
		this.hostId = hostId;
		this.numMapSlots = numMapSlots;
		this.numReduceSlots = numReduceSlots;

		mapSlots = new TimelineTaskSlot[numMapSlots];
		redSlots = new TimelineTaskSlot[numReduceSlots];
		allSlots = new TimelineTaskSlot[numMapSlots + numReduceSlots];

		// Create the reduce slots
		int slotId = 0;
		for (int i = 0; i < numReduceSlots; ++i) {
			redSlots[i] = new TimelineTaskSlot(slotId);
			allSlots[slotId] = redSlots[i];
			++slotId;
		}

		// Create the map slots
		for (int i = 0; i < numMapSlots; ++i) {
			mapSlots[i] = new TimelineTaskSlot(slotId);
			allSlots[slotId] = mapSlots[i];
			++slotId;
		}
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the hostName
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * @return the hostId
	 */
	public int getHostId() {
		return hostId;
	}

	/**
	 * @return the numMapSlots
	 */
	public int getNumMapSlots() {
		return numMapSlots;
	}

	/**
	 * @return the numReduceSlots
	 */
	public int getNumReduceSlots() {
		return numReduceSlots;
	}

	/**
	 * @param hostId
	 *            the hostId to set
	 */
	public void setHostId(int hostId) {
		this.hostId = hostId;
	}

	/**
	 * @return the allSlots
	 */
	public TimelineTaskSlot[] getAllSlots() {
		return allSlots;
	}

	/**
	 * Schedule this map attempt to the earliest possible slot
	 * 
	 * @param attempt
	 *            the map attempt
	 */
	public void scheduleMapTask(MRMapAttemptInfo attempt) {
		scheduleTask(mapSlots, attempt);
	}

	/**
	 * Schedule this reduce attempt to the earliest possible slot
	 * 
	 * @param attempt
	 *            the reduce attempt
	 */
	public void scheduleReduceTask(MRReduceAttemptInfo attempt) {
		scheduleTask(redSlots, attempt);
	}

	/**
	 * Schedule this setup attempt to the earliest possible slot
	 * 
	 * @param attempt
	 *            the setup attempt
	 */
	public void scheduleSetupTask(MRSetupAttemptInfo attempt) {
		if (attempt.getExecId().contains("_m_"))
			scheduleTask(mapSlots, attempt);
		else
			scheduleTask(redSlots, attempt);
	}

	/**
	 * Schedule this cleanup attempt to the earliest possible slot
	 * 
	 * @param attempt
	 *            the cleanup attempt
	 */
	public void scheduleCleanupTask(MRCleanupAttemptInfo attempt) {
		if (attempt.getExecId().contains("_m_"))
			scheduleTask(mapSlots, attempt);
		else
			scheduleTask(redSlots, attempt);
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TimelineTaskTracker [hostId=" + hostId + ", hostName="
				+ hostName + ", allSlots=" + allSlots.length + ", mapSlots="
				+ mapSlots.length + ", redSlots=" + redSlots.length + "]";
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Schedule the attempt to the earliest possible slot from the provided
	 * array of slots
	 * 
	 * @param slots
	 *            the array of slots
	 * @param attempt
	 *            the attempt
	 */
	private void scheduleTask(TimelineTaskSlot[] slots,
			MRTaskAttemptInfo attempt) {

		// Find the slot with the earlier end time
		TimelineTaskSlot slot = slots[0];
		long minEndTime = slot.getLastEndTime();
		for (int i = 0; i < slots.length; ++i) {
			if (slots[i].getLastEndTime() < minEndTime) {
				minEndTime = slots[i].getLastEndTime();
				slot = slots[i];
			}
		}

		// Add the attempt to the slot
		slot.addAttempt(attempt);
	}
}
