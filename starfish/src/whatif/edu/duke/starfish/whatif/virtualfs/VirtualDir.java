package edu.duke.starfish.whatif.virtualfs;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a virtual directory
 * 
 * @author hero
 */
public class VirtualDir extends VirtualPath {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private List<VirtualPath> children;

	/**
	 * @see VirtualPath#VirtualPath(VirtualPath, String)
	 */
	public VirtualDir(VirtualDir parent, String name) {
		super(parent, name);
		children = new ArrayList<VirtualPath>();
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
		return true;
	}

	/**
	 * @see edu.duke.starfish.whatif.virtualfs.VirtualPath#isFile()
	 */
	@Override
	public boolean isFile() {
		return false;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return super.hashCode();
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
		if (!(obj instanceof VirtualDir))
			return false;
		return true;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @param child
	 *            the child to add
	 */
	public void addChild(VirtualPath child) {
		this.children.add(child);
		child.setParent(this);
	}

	/**
	 * @param name
	 *            the child's name (supports glob)
	 * @return true if a child directory or file
	 */
	public boolean containsChild(String name) {
		for (VirtualPath child : children) {
			if (child.matches(name))
				return true;
		}
		return false;
	}

	/**
	 * @param name
	 *            the child's name (supports glob)
	 * @return true if a child directory
	 */
	public boolean containsChildDir(String name) {
		for (VirtualPath child : children) {
			if (child.matches(name) && child.isDir())
				return true;
		}
		return false;
	}

	/**
	 * @param name
	 *            the child's name (supports glob)
	 * @return true if a child file
	 */
	public boolean containsChildFile(String name) {
		for (VirtualPath child : children) {
			if (child.matches(name) && child.isFile())
				return true;
		}
		return false;
	}

	/**
	 * Delete a child file
	 * 
	 * @param file
	 *            the file to delete
	 * @return true if the directory contained this file
	 */
	public boolean deleteChildFile(VirtualFile file) {
		return children.remove(file);
	}

	/**
	 * @param name
	 *            (supports glob)
	 * @return the children
	 */
	public List<VirtualPath> getChildren(String name) {
		List<VirtualPath> result = new ArrayList<VirtualPath>(1);
		for (VirtualPath child : children) {
			if (child.matches(name))
				result.add(child);
		}
		return result;
	}

	/**
	 * @return the children
	 */
	public List<VirtualPath> getChildren() {
		return children;
	}

}
