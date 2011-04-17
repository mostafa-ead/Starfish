package edu.duke.starfish.whatif.data;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;

/**
 * Represents the map input specification. It consists of:
 * <ul>
 * <li>the index of the job input</li>
 * <li>the number of splits for this input</li>
 * <li>the average map input size</li>
 * <li>whether the input is compressed or not</li>
 * <li>the data locality</li>
 * </ul>
 * 
 * @author hero
 */
public class MapInputSpecs {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private int inputIndex; // The index identifying the job input
	private int numSplits; // The number of input splits
	private long size; // The average map input size
	private boolean isCompressed; // Whether the input is compressed
	private DataLocality locality; // Data locality

	/**
	 * Constructor
	 * 
	 * @param inputIndex
	 *            The index identifying the job input
	 * @param numSplits
	 *            The number of map tasks
	 * @param size
	 *            The average map input size
	 * @param isCompressed
	 *            Whether the input is compressed
	 * @param locality
	 *            Data locality
	 */
	public MapInputSpecs(int inputIndex, int numSplits, long size,
			boolean isCompressed, DataLocality locality) {
		this.inputIndex = inputIndex;
		this.numSplits = numSplits;
		this.size = size;
		this.isCompressed = isCompressed;
		this.locality = locality;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the numSplits
	 */
	public int getNumSplits() {
		return numSplits;
	}

	/**
	 * @return the inputIndex
	 */
	public int getInputIndex() {
		return inputIndex;
	}

	/**
	 * @return the size
	 */
	public long getSize() {
		return size;
	}

	/**
	 * @return the isCompressed
	 */
	public boolean isCompressed() {
		return isCompressed;
	}

	/**
	 * @return the locality
	 */
	public DataLocality getLocality() {
		return locality;
	}

	/**
	 * @param inputIndex
	 *            the input index to set
	 */
	public void setInputIndex(int inputIndex) {
		this.inputIndex = inputIndex;
	}

	/**
	 * @param numSplits
	 *            the number of splits to set
	 */
	public void setNumSplits(int numSplits) {
		this.numSplits = numSplits;
	}

	/**
	 * @param size
	 *            the size to set
	 */
	public void setSize(long size) {
		this.size = size;
	}

	/**
	 * @param isCompressed
	 *            the compression flag to set
	 */
	public void setCompressed(boolean isCompressed) {
		this.isCompressed = isCompressed;
	}

	/**
	 * @param locality
	 *            the locality to set
	 */
	public void setLocality(DataLocality locality) {
		this.locality = locality;
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
		result = 31 * result + inputIndex;
		result = 37 * result + (isCompressed ? 1231 : 1237);
		result = 41 * result + ((locality == null) ? 0 : locality.hashCode());
		result = 43 * result + numSplits;
		result = 47 * result + (int) (size ^ (size >>> 32));
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
		if (!(obj instanceof MapInputSpecs))
			return false;
		MapInputSpecs other = (MapInputSpecs) obj;
		if (inputIndex != other.inputIndex)
			return false;
		if (isCompressed != other.isCompressed)
			return false;
		if (locality == null) {
			if (other.locality != null)
				return false;
		} else if (!locality.equals(other.locality))
			return false;
		if (numSplits != other.numSplits)
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
		return "MapInputSpecs [inputIndex=" + inputIndex + ", isCompressed="
				+ isCompressed + ", numSplits=" + numSplits + ", size=" + size
				+ "]";
	}

}
