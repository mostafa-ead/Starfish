package edu.duke.starfish.whatif.virtualfs;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a virtual file system.
 * 
 * @author hero
 */
public class VirtualFileSystem {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private VirtualDir root;
	private long defaultBlockSize;
	private int defaultReplication;

	// Constants
	public static final String SEPARATOR = "/";

	/**
	 * Default constructor
	 */
	public VirtualFileSystem() {
		root = new VirtualDir(null, "");
		defaultBlockSize = 67108864; // 64MB
		defaultReplication = 3;
	}

	/**
	 * Constructor
	 * 
	 * @param defaultBlockSize
	 *            default block size
	 * @param defaultReplication
	 *            default replication
	 */
	public VirtualFileSystem(long defaultBlockSize, int defaultReplication) {
		this.root = new VirtualDir(null, "");
		this.defaultBlockSize = defaultBlockSize;
		this.defaultReplication = defaultReplication;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Create a new file in the file system
	 * 
	 * @param fullPath
	 *            the full path
	 * @param size
	 *            the file size
	 * @param compress
	 *            file compression flag
	 * @return the new virtual file
	 * @throws VirtualFSException
	 */
	public VirtualFile createFile(String fullPath, long size, boolean compress)
			throws VirtualFSException {
		return createFile(fullPath, size, compress, defaultBlockSize,
				defaultReplication);
	}

	/**
	 * Create a new file in the file system
	 * 
	 * @param fullPath
	 *            the full path
	 * @param size
	 *            the file size
	 * @param compress
	 *            file compression flag
	 * @param blockSize
	 *            the block size
	 * @param replication
	 *            replication factor
	 * @return the new virtual file
	 * @throws VirtualFSException
	 */
	public VirtualFile createFile(String fullPath, long size, boolean compress,
			long blockSize, int replication) throws VirtualFSException {

		if (!fullPath.startsWith(SEPARATOR))
			throw new VirtualFSException("Not a full path: " + fullPath);

		// Create the directory structure
		String[] paths = fullPath.split(SEPARATOR);
		VirtualDir parent = root;
		VirtualDir child = null;
		for (int i = 1; i < paths.length - 1; ++i) {
			if (parent.containsChildFile(paths[i])) {
				// Found a file with this name
				throw new VirtualFSException("Subpath already exists as file: "
						+ fullPath);
			} else if (parent.containsChildDir(paths[i])) {
				// Found a directory with this name
				List<VirtualPath> children = parent.getChildren(paths[i]);
				if (children.size() != 1)
					throw new VirtualFSException(
							"Found multiple paths matching: " + fullPath);
				child = (VirtualDir) children.get(0);
			} else {
				// Create a new directory
				child = new VirtualDir(parent, paths[i]);
				parent.addChild(child);
			}

			parent = child;
		}

		if (parent.containsChild(paths[paths.length - 1]))
			throw new VirtualFSException("File already exists: " + fullPath);

		// Create the file
		VirtualFile file = new VirtualFile(parent, paths[paths.length - 1],
				size, compress, blockSize, replication);
		parent.addChild(file);

		return file;
	}

	/**
	 * Delete all files represented by the provided full path
	 * 
	 * @param fullPath
	 *            the full path (supports glob)
	 * @param recursive
	 *            whether to delete the files recursively
	 * @return true if all files deleted successfully
	 * @throws VirtualFSException
	 */
	public boolean deleteFiles(String fullPath, boolean recursive)
			throws VirtualFSException {

		boolean delete = true;
		List<VirtualFile> files = listFiles(fullPath, recursive);

		for (VirtualFile file : files) {
			if (!((VirtualDir) file.getParent()).deleteChildFile(file))
				delete = false;
		}

		return delete;
	}

	/**
	 * List all files from the fullPath. If the full path is a file, then it is
	 * returned. If the full path is a directory, then the files within the
	 * directory are returned (recursively if specified)
	 * 
	 * @param fullPath
	 *            the full path (supports glob)
	 * @param recursive
	 *            whether to recurse the full path or not
	 * @return a list of files
	 * @throws VirtualFSException
	 *             if full path invalid or does not exist
	 */
	public List<VirtualFile> listFiles(String fullPath, boolean recursive)
			throws VirtualFSException {

		if (!fullPath.startsWith(SEPARATOR))
			throw new VirtualFSException("Not a full path: " + fullPath);

		// Find the path and create the result
		List<VirtualFile> result = new ArrayList<VirtualFile>();
		String[] paths = fullPath.split(SEPARATOR);
		if (paths.length == 0)
			gatherFiles(result, root, recursive);
		else
			gatherFiles(result, root, recursive, paths, 0);

		return result;
	}

	/**
	 * @return the defaultBlockSize
	 */
	public long getDefaultBlockSize() {
		return defaultBlockSize;
	}

	/**
	 * @return the defaultReplication
	 */
	public int getDefaultReplication() {
		return defaultReplication;
	}

	/**
	 * @param defaultBlockSize
	 *            the defaultBlockSize to set
	 */
	public void setDefaultBlockSize(long defaultBlockSize) {
		this.defaultBlockSize = defaultBlockSize;
	}

	/**
	 * @param defaultReplication
	 *            the defaultReplication to set
	 */
	public void setDefaultReplication(int defaultReplication) {
		this.defaultReplication = defaultReplication;
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
		result = 31 * result
				+ (int) (defaultBlockSize ^ (defaultBlockSize >>> 32));
		result = 37 * result + defaultReplication;
		result = 41 * result + ((root == null) ? 0 : root.hashCode());
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
		if (!(obj instanceof VirtualFileSystem))
			return false;
		VirtualFileSystem other = (VirtualFileSystem) obj;
		if (defaultBlockSize != other.defaultBlockSize)
			return false;
		if (defaultReplication != other.defaultReplication)
			return false;
		if (root == null) {
			if (other.root != null)
				return false;
		} else if (!root.equals(other.root))
			return false;
		return true;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Main recursive method to gather files matching the provided input paths.
	 * 
	 * @param files
	 *            the list to place the files in
	 * @param path
	 *            the current path
	 * @param recursive
	 *            whether the final file search is recursive or not
	 * @param paths
	 *            the array of the input paths
	 * @param index
	 *            the current index in the array of input paths
	 */
	private void gatherFiles(List<VirtualFile> files, VirtualPath path,
			boolean recursive, String[] paths, int index) {

		if (!path.matches(paths[index])) {
			// No match, stop here
			return;
		}

		if (index == paths.length - 1) {
			// We have reached a leaf in the path structure
			if (path.isFile())
				files.add((VirtualFile) path);
			else
				gatherFiles(files, (VirtualDir) path, recursive);
		} else {
			if (path.isDir()) {
				// We have reached an intermediate directory
				for (VirtualPath child : ((VirtualDir) path).getChildren()) {
					gatherFiles(files, child, recursive, paths, index + 1);
				}
			}
		}

	}

	/**
	 * Gather all virtual files from the input directory into the provided list
	 * 
	 * @param files
	 *            the input list to fill in
	 * @param dir
	 *            the current directory
	 * @param recursive
	 *            flag for recursion
	 */
	private void gatherFiles(List<VirtualFile> files, VirtualDir dir,
			boolean recursive) {

		// Traverse the children
		for (VirtualPath child : dir.getChildren()) {
			if (child.isFile()) {
				// Found a file
				files.add((VirtualFile) child);
			} else if (recursive) {
				// Recurse
				gatherFiles(files, (VirtualDir) child, recursive);
			}
		}

	}

	/* ***************************************************************
	 * PUBLIC EXCEPTION CLASS
	 * ***************************************************************
	 */

	/**
	 * Virtual File System Exception
	 * 
	 * @author hero
	 */
	public static class VirtualFSException extends Exception {

		private static final long serialVersionUID = 4038261176794514778L;

		/**
		 * Exception with error message
		 * 
		 * @param msg
		 */
		public VirtualFSException(String msg) {
			super(msg);
		}

		/**
		 * Exception with cause
		 * 
		 * @param msg
		 * @param cause
		 */
		public VirtualFSException(String msg, Throwable cause) {
			super(msg, cause);
		}
	}
}
