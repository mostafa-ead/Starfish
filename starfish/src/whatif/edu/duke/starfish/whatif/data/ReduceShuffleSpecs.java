package edu.duke.starfish.whatif.data;

/**
 * Represents the shuffle specifications of the data that will reach the reduce
 * tasks. It consists of:
 * <ul>
 * <li>how many map tasks generated this data</li>
 * <li>how many reduce tasks will access this data</li>
 * <li>the average shuffle size for each reduce task</li>
 * <li>the average number of records for each reduce task</li>
 * </ul>
 * 
 * @author hero
 */
public class ReduceShuffleSpecs {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private int numMappers; // The number of mappers
	private int numReducers; // The number of reducers
	private long size; // The shuffle size to a single reducer
	private long records; // The number of records shuffled to a single reducer

	/**
	 * Constructor
	 * 
	 * @param numMappers
	 *            the number of mappers
	 * @param numReducers
	 *            the number of reducers
	 * @param size
	 *            the shuffle size to a single reducer
	 * @param records
	 *            number of records shuffled to a single reducer
	 */
	public ReduceShuffleSpecs(int numMappers, int numReducers, long size,
			long records) {
		this.numMappers = numMappers;
		this.numReducers = numReducers;
		this.size = size;
		this.records = records;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the numMappers
	 */
	public int getNumMappers() {
		return numMappers;
	}

	/**
	 * @return the numReducers
	 */
	public int getNumReducers() {
		return numReducers;
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
	 * @param numMappers
	 *            the number of mappers to set
	 */
	public void setNumMappers(int numMappers) {
		this.numMappers = numMappers;
	}

	/**
	 * @param numReducers
	 *            the number of reducers to set
	 */
	public void setNumReducers(int numReducers) {
		this.numReducers = numReducers;
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
		result = 31 * result + numMappers;
		result = 37 * result + numReducers;
		result = 41 * result + (int) (records ^ (records >>> 32));
		result = 43 * result + (int) (size ^ (size >>> 32));
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
		if (!(obj instanceof ReduceShuffleSpecs))
			return false;
		ReduceShuffleSpecs other = (ReduceShuffleSpecs) obj;
		if (numMappers != other.numMappers)
			return false;
		if (numReducers != other.numReducers)
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
		return "ReduceShuffleSpecs [numMappers=" + numMappers
				+ ", numReducers=" + numReducers + ", records=" + records
				+ ", size=" + size + "]";
	}

}
