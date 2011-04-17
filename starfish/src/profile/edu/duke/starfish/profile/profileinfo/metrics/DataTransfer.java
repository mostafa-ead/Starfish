package edu.duke.starfish.profile.profileinfo.metrics;

import java.util.Date;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;

/**
 * This class represents a data transfer between map attempt and a reduce
 * attempt.
 * 
 * @author hero
 */
public class DataTransfer {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private MRMapAttemptInfo source; // The source of the transfer
	private MRReduceAttemptInfo destination; // The destination of the transfer
	private Date startTime; // The start time of the transfer
	private Date endTime; // The end time of the transfer
	private long comprData; // The amount of compressed data in bytes
	private long uncomprData; // The amount of uncompressed data in bytes

	private int hash = -1; // The hash value for this object

	/**
	 * Constructor
	 * 
	 * @param source
	 *            The source of the data transfer
	 * @param destination
	 *            The destination of the data transfer
	 * @param comprData
	 *            The amount of compressed data transferred in bytes
	 * @param uncomprData
	 *            The amount of uncompressed data in bytes
	 */
	public DataTransfer(MRMapAttemptInfo source,
			MRReduceAttemptInfo destination, long comprData, long uncomprData) {
		this.source = source;
		this.destination = destination;
		this.comprData = comprData;
		this.uncomprData = uncomprData;

		// Set the start and end times
		if (source.getEndTime().after(destination.getStartTime()))
			this.startTime = source.getEndTime();
		else
			this.startTime = destination.getStartTime();
		this.endTime = destination.getShuffleEndTime();

	}

	/**
	 * Copy Constructor
	 * 
	 * @param other
	 */
	public DataTransfer(DataTransfer other) {
		this.source = other.source == null ? null : new MRMapAttemptInfo(
				other.source);
		this.destination = other.destination == null ? null
				: new MRReduceAttemptInfo(other.destination);
		this.comprData = other.comprData;
		this.uncomprData = other.uncomprData;

		this.startTime = other.startTime == null ? null : new Date(
				other.startTime.getTime());
		this.endTime = other.endTime == null ? null : new Date(other.endTime
				.getTime());

	}

	/* ***************************************************************
	 * GETTERS AND SETTERS
	 * ***************************************************************
	 */

	/**
	 * @return The source of the data transfer
	 */
	public MRMapAttemptInfo getSource() {
		return source;
	}

	/**
	 * @return The destination of the data transfer
	 */
	public MRReduceAttemptInfo getDestination() {
		return destination;
	}

	/**
	 * @return The duration of the data transfer
	 */
	public long getDuration() {
		if (endTime != null && startTime != null)
			return endTime.getTime() - startTime.getTime();
		else
			return 0l;
	}

	/**
	 * @return The start time of the data transfer
	 */
	public Date getStartTime() {
		return startTime;
	}

	/**
	 * @return The end time of the data transfer
	 */
	public Date getEndTime() {
		return endTime;
	}

	/**
	 * @return The amount of compressed data transferred in bytes
	 */
	public long getComprData() {
		return comprData;
	}

	/**
	 * @return The amount of uncompressed data in bytes
	 */
	public long getUncomprData() {
		return uncomprData;
	}

	/**
	 * @param startTime
	 *            the start time for the transfer
	 */
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	/**
	 * @param endTime
	 *            the end time for the transfer
	 */
	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash == -1) {
			hash = 1;
			hash = 31 * hash + (int) (comprData ^ (comprData >>> 32));
			hash = 37 * hash + (int) (uncomprData ^ (uncomprData >>> 32));
			hash = 41 * hash
					+ ((destination == null) ? 0 : destination.hashCode());
			hash = 43 * hash + ((source == null) ? 0 : source.hashCode());
		}
		return hash;
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
		if (!(obj instanceof DataTransfer))
			return false;
		DataTransfer other = (DataTransfer) obj;
		if (comprData != other.comprData)
			return false;
		if (uncomprData != other.uncomprData)
			return false;
		if (destination == null) {
			if (other.destination != null)
				return false;
		} else if (!destination.equals(other.destination))
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equals(other.source))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DataTransfer [source=" + source.getExecId() + ", destination="
				+ destination.getExecId() + ", compressed data=" + comprData
				+ ", uncompressed data=" + uncomprData + "]";
	}

}
