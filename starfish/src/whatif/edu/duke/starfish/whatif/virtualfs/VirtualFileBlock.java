package edu.duke.starfish.whatif.virtualfs;

/**
 * Represents a virtual file block
 * 
 * @author hero
 */
public class VirtualFileBlock {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private long size;

	/**
	 * Constructor
	 * 
	 * @param size
	 *            block size
	 */
	public VirtualFileBlock(long size) {
		this.size = size;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the size (in bytes)
	 */
	public long getSize() {
		return size;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + (int) (size ^ (size >>> 32));
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
		if (!(obj instanceof VirtualFileBlock))
			return false;
		VirtualFileBlock other = (VirtualFileBlock) obj;
		if (size != other.size)
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "VirtualFileBlock [size=" + size + "]";
	}

}
