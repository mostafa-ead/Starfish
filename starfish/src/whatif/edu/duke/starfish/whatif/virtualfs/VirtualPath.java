package edu.duke.starfish.whatif.virtualfs;

import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;

/**
 * Represents a virtual file or directory path. Also forms the base class for a
 * VirtualFile or a VirtualDirectory
 * 
 * @author hero
 */
public abstract class VirtualPath {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	protected VirtualPath parent;
	protected String name;

	/**
	 * Constructor
	 * 
	 * @param parent
	 *            parent directory
	 * @param name
	 *            directory or file name
	 */
	public VirtualPath(VirtualPath parent, String name) {
		this.parent = parent;
		this.name = name;
	}

	/* ***************************************************************
	 * ABSTRACT METHODS
	 * ***************************************************************
	 */

	/**
	 * @return true if a directory
	 */
	public abstract boolean isDir();

	/**
	 * @return true if a file
	 */
	public abstract boolean isFile();

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the parent
	 */
	public VirtualPath getParent() {
		return parent;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param parent
	 *            the parent to set
	 */
	public void setParent(VirtualPath parent) {
		this.parent = parent;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Checks if the name of this path matches the input name (supports glob)
	 * 
	 * @param name
	 *            the name to match
	 * @return if the name matches
	 */
	public boolean matches(String name) {
		if (this.name.equals(name))
			return true;

		return this.name.matches(ProfileUtils.convertGlobToRegEx(name, true));
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + ((name == null) ? 0 : name.hashCode());
		result = 37 * result + ((parent == null) ? 0 : parent.hashCode());
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
		if (!(obj instanceof VirtualPath))
			return false;
		VirtualPath other = (VirtualPath) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (parent == null) {
			if (other.parent != null)
				return false;
		} else if (!parent.equals(other.parent))
			return false;
		return true;
	}

	/**
	 * @see edu.duke.starfish.whatif.virtualfs.VirtualPath#toString()
	 */
	@Override
	public String toString() {
		return (parent == null) ? name : parent.toString()
				+ VirtualFileSystem.SEPARATOR + name;
	}

}
