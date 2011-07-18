package edu.duke.starfish.profile.utils;

import java.text.NumberFormat;
import java.util.List;

import org.apache.hadoop.fs.Path;

/**
 * Static methods implementing several general-purpose utilities
 * 
 * @author hero
 */
public class GeneralUtils {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private static int MS_IN_SEC = 1000;
	private static int SEC_IN_MIN = 60;
	private static int MIN_IN_HR = 60;
	private static int SEC_IN_HR = 3600;

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Build the output file name for a MapReduce output file
	 * 
	 * The name will be of the form: /base/dir/part-r-0001
	 * 
	 * @param baseDir
	 *            the base directory
	 * @param id
	 *            the task id
	 * @param mapOnly
	 *            whether the job is map-only or not
	 * @param compress
	 *            whether the output is compressed or not
	 * @return the full output file name
	 */
	public static String buildMROutputName(String baseDir, int id,
			boolean mapOnly, boolean compress) {

		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);

		StringBuilder sb = new StringBuilder();
		sb.append(baseDir);
		sb.append(Path.SEPARATOR);
		sb.append("part-");
		sb.append(mapOnly ? "m-" : "r-");
		sb.append(nf.format(id));
		if (compress)
			sb.append(".deflate");

		return sb.toString();
	}

	/**
	 * Converts a line containing glob into the corresponding regex.
	 * 
	 * Note: The string.match method matches the entire string by default. If
	 * this method will be used to match an entire string, set matchAll to true.
	 * If this method will be used to match a substring of a string, set
	 * matchAll to false.
	 * 
	 * @param line
	 *            the input glob
	 * @param matchAll
	 *            whether the regex returned is intented to match an entire
	 *            string or a substring
	 * @return the corresponding regex
	 */
	public static String convertGlobToRegEx(String line, boolean matchAll) {

		line = line.trim();
		int strLen = line.length();
		StringBuilder sb = new StringBuilder(strLen);

		if (!matchAll)
			sb.append(".*");

		boolean escaping = false;
		int inCurlies = 0;
		for (char currentChar : line.toCharArray()) {
			switch (currentChar) {
			case '*':
				if (escaping)
					sb.append("\\*");
				else
					sb.append(".*");
				escaping = false;
				break;
			case '?':
				if (escaping)
					sb.append("\\?");
				else
					sb.append('.');
				escaping = false;
				break;
			case '.':
			case '(':
			case ')':
			case '+':
			case '|':
			case '^':
			case '$':
			case '@':
			case '%':
				sb.append('\\');
				sb.append(currentChar);
				escaping = false;
				break;
			case '\\':
				if (escaping) {
					sb.append("\\\\");
					escaping = false;
				} else
					escaping = true;
				break;
			case '{':
				if (escaping) {
					sb.append("\\{");
				} else {
					sb.append('(');
					inCurlies++;
				}
				escaping = false;
				break;
			case '}':
				if (inCurlies > 0 && !escaping) {
					sb.append(')');
					inCurlies--;
				} else if (escaping)
					sb.append("\\}");
				else
					sb.append("}");
				escaping = false;
				break;
			case ',':
				if (inCurlies > 0 && !escaping) {
					sb.append('|');
				} else if (escaping)
					sb.append("\\,");
				else
					sb.append(",");
				break;
			default:
				escaping = false;
				sb.append(currentChar);
			}
		}

		if (!matchAll)
			sb.append(".*");

		return sb.toString();
	}

	/**
	 * Get the position of a path in an array of paths. It supports globbing!
	 * 
	 * Example: A path "/usr/root/joins/orders/orders.tbl.1" will match a path
	 * "/usr/root/joins/orders" or "/usr/root/joins/orders/orders.*" or
	 * "/usr/root/joins/orders/orders.tbl.[1-2]
	 * 
	 * If the path matches multiple paths in the array, the first one is
	 * returned.
	 * 
	 * @param paths
	 *            the paths
	 * @param path
	 *            the path to search for
	 * @return the path's index in the array. -1 if not found
	 */
	public static int getIndexInPathArray(String[] paths, String path) {

		if (path == null || paths == null || paths.length == 0)
			return -1;

		int pos = -1;
		int numMatches = 0;

		// Find the matching job input
		for (int i = paths.length - 1; i >= 0; --i) {
			if (path.matches(GeneralUtils.convertGlobToRegEx(paths[i], false))) {
				pos = i;
				++numMatches;
			}
		}

		// Check for unique match or no match
		if (numMatches == 1 || pos == -1)
			return pos;

		// The most common case for multiple matches is when the name of a
		// directory is a substring of the name of another directory
		int newPos = -1;
		for (int i = paths.length - 1; i >= 0; --i) {
			if (path.matches(GeneralUtils.convertGlobToRegEx(paths[i] + "/",
					false))) {
				newPos = i;
			}
		}

		// If we don't find a new position, return the old one. This handles the
		// case were adding "/" cancels the matches.
		if (newPos != -1)
			return newPos;
		else
			return pos;
	}

	/**
	 * Formats the input duration in a human readable format. The output will be
	 * on of: ms, sec, min & sec, hr & min & sec depending on the duration.
	 * 
	 * @param duration
	 *            the duration in ms
	 * @return a formatted string
	 */
	public static String getFormattedDuration(long duration) {

		String result;
		long sec = Math.round(duration / (float) MS_IN_SEC);
		if (duration < MS_IN_SEC) {
			result = String.format("%d ms", duration);
		} else if (sec < SEC_IN_MIN) {
			result = String.format("%d sec %d ms", sec, duration % MS_IN_SEC);
		} else if (sec < SEC_IN_HR) {
			result = String.format("%d min %d sec", sec / SEC_IN_MIN, sec
					% SEC_IN_MIN);
		} else {
			result = String.format("%d hr %d min %d sec", sec / SEC_IN_HR,
					(sec / MIN_IN_HR) % SEC_IN_MIN, sec % SEC_IN_MIN);
		}

		return result;
	}

	/**
	 * Formats the input size in a human readable format. The output will be on
	 * of: Bytes, KB, MB, GB depending on the size.
	 * 
	 * @param bytes
	 *            the size in bytes
	 * @return a formatted string
	 */
	public static String getFormattedSize(long bytes) {

		String result;
		if (bytes < 1024) {
			result = String.format("%d Bytes", bytes);
		} else if (bytes < 1024l * 1024) {
			result = String.format("%.2f KB", bytes / 1024.0);
		} else if (bytes < 1024l * 1024 * 1024) {
			result = String.format("%.2f MB", bytes / (1024.0 * 1024));
		} else {
			result = String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
		}

		return result;
	}

	/**
	 * Checks if the provided filename has a known compression extension
	 * 
	 * @param fileName
	 *            the filename
	 * @return true if filename has compression extension
	 */
	public static boolean hasCompressionExtension(String fileName) {

		return fileName.endsWith(".deflate") || fileName.endsWith(".gz")
				|| fileName.endsWith(".tgz") || fileName.endsWith(".bz")
				|| fileName.endsWith(".bz2") || fileName.endsWith(".zip")
				|| fileName.endsWith(".lzo") || fileName.endsWith(".rar");
	}

	/**
	 * Checks if the provided filename has a known compression extension for a
	 * compression scheme that is not splittable
	 * 
	 * @param fileName
	 *            the filename
	 * @return true if filename has compression extension that is not splittable
	 */
	public static boolean hasNonSplittableComprExtension(String fileName) {

		return fileName.endsWith(".deflate") || fileName.endsWith(".gz")
				|| fileName.endsWith(".tgz") || fileName.endsWith(".zip")
				|| fileName.endsWith(".rar");
	}

	/**
	 * Checks if the provided filename has a known compression extension for a
	 * compression scheme that is splittable
	 * 
	 * @param fileName
	 *            the filename
	 * @return true if filename has compression extension that is splittable
	 */
	public static boolean hasSplittableComprExtension(String fileName) {

		return fileName.endsWith(".bz") || fileName.endsWith(".bz2")
				|| fileName.endsWith(".lzo");
	}

	/**
	 * Given the input path that may include URL specifications, extract the
	 * full path without the URL scheme and domain. Also, if the path is
	 * relative, it will be changed to a full path by adding 'user.home' to it.
	 * 
	 * Example: The input path hdfs://localhost:9000/some/dir with be changed to
	 * /some/dir
	 * 
	 * @param path
	 *            the path to process
	 * @return the full path
	 */
	public static String normalizePath(String path) {

		// Prepare the output
		String output = path.trim();

		// Remove any leading URL scheme and domain
		int index = output.indexOf("://");
		if (index > 0) {
			index = output.indexOf('/', index + 3);
			output = output.substring(index);
		}

		if (!output.startsWith(Path.SEPARATOR))
			output = System.getProperty("user.home") + Path.SEPARATOR + output;

		// Remove the final separator
		if (output.endsWith(Path.SEPARATOR) && output.length() > 1)
			output = output.substring(0, output.length() - 1);

		return output;
	}

	/**
	 * Union the provided paths with the paths inside 'union' and store the
	 * result in 'union'. This method respects the order of paths in 'union'.
	 * This method can also be called several times consecutively for the case
	 * where the paths become available in groups.
	 * 
	 * Note: This method also normalizes the paths using
	 * {@link GeneralUtils#normalizePath(String)}
	 * 
	 * @param union
	 *            a list of paths to store for union of paths
	 * @param paths
	 *            the paths to add into the union
	 */
	public static void unionPathsWithOrder(List<String> union, String... paths) {

		for (String path : paths) {
			path = GeneralUtils.normalizePath(path);
			if (!union.contains(path))
				union.add(path);
		}
	}

}
