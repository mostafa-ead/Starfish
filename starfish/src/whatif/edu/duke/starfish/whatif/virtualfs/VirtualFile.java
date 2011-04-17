package edu.duke.starfish.whatif.virtualfs;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a virtual file
 * 
 * @author hero
 */
public class VirtualFile extends VirtualPath {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private long size;
	private boolean compress;
	private long blockSize;
	private int replication;
	private List<VirtualFileBlock> blocks;

	/**
	 * Constructor
	 * 
	 * @param parent
	 *            parent directory
	 * @param name
	 *            file name
	 * @param size
	 *            file size (bytes)
	 * @param compress
	 *            compression
	 * @param blockSize
	 *            block size (bytes)
	 * @param replication
	 *            replication factor
	 */
	public VirtualFile(VirtualDir parent, String name, long size,
			boolean compress, long blockSize, int replication) {
		super(parent, name);
		this.size = size;
		this.compress = compress;
		this.blockSize = blockSize;
		this.replication = replication;
		createFileBlocks();
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see edu.duke.starfish.whatif.virtualfs.VirtualPath#isDir()
	 */
	@Override
	public boolean isDir() {
		return false;
	}

	/**
	 * @see edu.duke.starfish.whatif.virtualfs.VirtualPath#isFile()
	 */
	@Override
	public boolean isFile() {
		return true;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
		result = 37 * result + ((blocks == null) ? 0 : blocks.hashCode());
		result = 41 * result + (compress ? 1231 : 1237);
		result = 43 * result + replication;
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
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof VirtualFile))
			return false;
		VirtualFile other = (VirtualFile) obj;
		if (blockSize != other.blockSize)
			return false;
		if (blocks == null) {
			if (other.blocks != null)
				return false;
		} else if (!blocks.equals(other.blocks))
			return false;
		if (compress != other.compress)
			return false;
		if (replication != other.replication)
			return false;
		if (size != other.size)
			return false;
		return true;
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
	 * @return the compress
	 */
	public boolean isCompress() {
		return compress;
	}

	/**
	 * @return the blockSize (in bytes)
	 */
	public long getBlockSize() {
		return blockSize;
	}

	/**
	 * @return the replication
	 */
	public int getReplication() {
		return replication;
	}

	/**
	 * @return the blocks
	 */
	public List<VirtualFileBlock> getBlocks() {
		return blocks;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Creates the file blocks
	 */
	private void createFileBlocks() {

		// Calculate the number of blocks
		int numBlocks = (int) Math.ceil(size / (double) blockSize);
		blocks = new ArrayList<VirtualFileBlock>(numBlocks);

		// Add the file blocks
		long remaining = size;
		while (remaining > blockSize) {
			blocks.add(new VirtualFileBlock(blockSize));
			remaining -= blockSize;
		}
		blocks.add(new VirtualFileBlock(remaining));
	}
}
