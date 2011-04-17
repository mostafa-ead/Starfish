package edu.duke.starfish.visualizer.model.timeline;

import java.util.ArrayList;
import java.util.List;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;

/**
 * Represents a map or reduce task slot in a task tracker
 * 
 * @author hero
 */
public class TimelineTaskSlot {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private int slotId;
	private long lastEndTime;
	private List<MRTaskAttemptInfo> attempts;

	/**
	 * Constructor
	 * 
	 * @param slotId
	 *            the slot id
	 */
	public TimelineTaskSlot(int slotId) {
		this.slotId = slotId;
		this.lastEndTime = 0;
		this.attempts = new ArrayList<MRTaskAttemptInfo>();
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the slotId
	 */
	public int getSlotId() {
		return slotId;
	}

	/**
	 * @return the lastEndTime
	 */
	public long getLastEndTime() {
		return lastEndTime;
	}

	/**
	 * @return the attempts
	 */
	public List<MRTaskAttemptInfo> getAttempts() {
		return attempts;
	}

	/**
	 * Add an attempt to this slot
	 * 
	 * @param attempt
	 *            the attempt to add
	 */
	public void addAttempt(MRTaskAttemptInfo attempt) {
		attempts.add(attempt);
		long endTime = attempt.getEndTime().getTime();
		lastEndTime = (endTime > lastEndTime) ? endTime : lastEndTime;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TimelineTaskSlot [slotId=" + slotId + ", attempts="
				+ attempts.size() + ", lastEndTime=" + lastEndTime + "]";
	}

}
