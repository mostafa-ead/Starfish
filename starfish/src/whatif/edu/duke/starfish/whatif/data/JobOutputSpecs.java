package edu.duke.starfish.whatif.data;

/**
 * Represents the specifications of the data that is output by the job. It
 * consists of:
 * <ul>
 * <li>how many map or reduce tasks output data with such properties</li>
 * <li>the average output size for each map or reduce task</li>
 * <li>the average number of records for each map or reduce task</li>
 * </ul>
 * 
 * @author hero
 */
public class JobOutputSpecs {
	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private int numTasks; // The number of map or reduce tasks
	private long size; // The output size of a single task
	private long records; // The number of records output by a single task

	/**
	 * Constructor
	 * 
	 * @param numTasks
	 *            the number of map or reduce tasks
	 * @param size
	 *            the output size
	 * @param records
	 *            the output records
	 */
	public JobOutputSpecs(int numTasks, long size, long records) {
		this.numTasks = numTasks;
		this.size = size;
		this.records = records;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the numTasks
	 */
	public int getNumTasks() {
		return numTasks;
	}

	/**
	 * @return the size
	 */
	public long getSize() {
		return size;
	}

	/**
	 * @return the records
	 */
	public long getRecords() {
		return records;
	}

	/**
	 * @param numTasks
	 *            the number of tasks to set
	 */
	public void setNumTasks(int numTasks) {
		this.numTasks = numTasks;
	}

	/**
	 * @param size
	 *            the size to set
	 */
	public void setSize(long size) {
		this.size = size;
	}

	/**
	 * @param records
	 *            the records to set
	 */
	public void setRecords(long records) {
		this.records = records;
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
		int result = 1;
		result = 31 * result + numTasks;
		result = 37 * result + (int) (records ^ (records >>> 32));
		result = 41 * result + (int) (size ^ (size >>> 32));
		return result;
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
		if (!(obj instanceof JobOutputSpecs))
			return false;
		JobOutputSpecs other = (JobOutputSpecs) obj;
		if (numTasks != other.numTasks)
			return false;
		if (records != other.records)
			return false;
		if (size != other.size)
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ReduceOutputSpecs [numTasks=" + numTasks + ", records="
				+ records + ", size=" + size + "]";
	}

}
