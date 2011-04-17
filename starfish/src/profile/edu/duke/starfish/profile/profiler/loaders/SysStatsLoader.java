package edu.duke.starfish.profile.profiler.loaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This class is responsible for parsing monitoring files containing iostat and
 * vmstat output. It assumes that all the monitoring files are named using the
 * conventions "iostat_output-NODE" and "vmstat_output-NODE", where NODE is the
 * actual node name.
 * 
 * @author hero
 */
public class SysStatsLoader {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// DATA MEMBERS
	private String monitorDir; // The directory with the monitoring files

	private Map<String, File> ioStatFiles; // Maps node names to iostat files
	private Map<String, File> vmStatFiles; // Maps node names to vmstat files

	// CONSTANTS
	private final String IOSTAT_PREFIX = "iostat_output-";
	private final String VMSTAT_PREFIX = "vmstat_output-";
	private final String AVG_CPU = "avg-cpu";
	private final String DEVICE = "Device";
	private final String EMPTY = "";
	private final String TAB = "\t";
	private final String SW = "sw";

	private final Pattern p = Pattern.compile("\\s+"); // Empty space

	/**
	 * Constructor
	 * 
	 * @param monitorDir
	 *            directory with the monitoring data
	 */
	public SysStatsLoader(String monitorDir) {
		this.monitorDir = monitorDir;
		this.ioStatFiles = new HashMap<String, File>();
		this.vmStatFiles = new HashMap<String, File>();

		readMonitorDirectory();
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Export the CPU statistics in a tabular format of the form
	 * "time\t%user\t%system\t%iowait\t%idle"
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @return true if the export is successful
	 */
	public boolean exportCPUStats(PrintStream out, String nodeName) {
		return exportCPUStats(out, nodeName, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	/**
	 * Export the CPU statistics in a tabular format of the form
	 * "time\t%user\t%system\t%iowait\t%idle" only for the records between dates
	 * start and end
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @param start
	 *            the start date
	 * @param end
	 *            the end date
	 * @return true if the export is successful
	 */
	public boolean exportCPUStats(PrintStream out, String nodeName, Date start,
			Date end) {
		return exportCPUStats(out, nodeName, start.getTime() / 1000, end
				.getTime() / 1000);
	}

	/**
	 * Export the memory statistics in a tabular format of the form
	 * "time\tswpd\tfree\tbuff\tcache"
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @return true if the export is successful
	 */
	public boolean exportMemoryStats(PrintStream out, String nodeName) {
		return exportMemoryStats(out, nodeName, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	/**
	 * Export the memory statistics in a tabular format of the form
	 * "time\tswpd\tfree\tbuff\tcache" only for the records between dates start
	 * and end
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @param start
	 *            the start date
	 * @param end
	 *            the end date
	 * @return true if the export is successful
	 */
	public boolean exportMemoryStats(PrintStream out, String nodeName,
			Date start, Date end) {
		return exportMemoryStats(out, nodeName, start.getTime() / 1000, end
				.getTime() / 1000);
	}

	/**
	 * Export the IO statistics in a tabular format of the form
	 * "time\tMBRead/s\tMBWrite/s"
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @return true if the export is successful
	 */
	public boolean exportIOStats(PrintStream out, String nodeName) {
		return exportIOStats(out, nodeName, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	/**
	 * Export the IO statistics in a tabular format of the form
	 * "time\tMBRead/s\tMBWrite/s" only for the records between dates start and
	 * end
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @param start
	 *            the start date
	 * @param end
	 *            the end date
	 * @return true if the export is successful
	 */
	public boolean exportIOStats(PrintStream out, String nodeName, Date start,
			Date end) {
		return exportIOStats(out, nodeName, start.getTime() / 1000, end
				.getTime() / 1000);
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Export the CPU statistics in a tabular format of the form
	 * "time\t%user\t%system\t%iowait\t%idle" only for the records between times
	 * start and end (epoch time in seconds)
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @param start
	 *            the start time in seconds
	 * @param end
	 *            the end time in seconds
	 * @return true if the export is successful
	 */
	private boolean exportCPUStats(PrintStream out, String nodeName,
			long start, long end) {

		// Ensure we have such a file
		if (!ioStatFiles.containsKey(nodeName)) {
			System.err.println("Unable to find a file for node " + nodeName);
			return false;
		}

		try {
			BufferedReader input = new BufferedReader(new FileReader(
					ioStatFiles.get(nodeName)));

			// Parse the file
			String line = null;
			out.println("time\t%user\t%system\t%iowait\t%idle");
			while ((line = input.readLine()) != null) {
				if (line.contains(AVG_CPU)) {

					// The next line contains the data
					line = input.readLine();
					if (line != null) {
						// Line: time %user %nice %system %iowait %steal %idle
						String[] pieces = p.split(line);
						if (pieces.length == 7
								&& satisfyBounds(pieces[0], start, end)) {
							out.print(pieces[0]);
							out.print(TAB);
							out.print(pieces[1]);
							out.print(TAB);
							out.print(pieces[3]);
							out.print(TAB);
							out.print(pieces[4]);
							out.print(TAB);
							out.println(pieces[6]);
						}
					}
				}

			}

			// Close the input file
			input.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * Export the memory statistics in a tabular format of the form
	 * "time\tswpd\tfree\tbuff\tcache" only for the records between times start
	 * and end (epoch time in seconds)
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @param start
	 *            the start time in seconds
	 * @param end
	 *            the end time in seconds
	 * @return true if the export is successful
	 */
	private boolean exportMemoryStats(PrintStream out, String nodeName,
			long start, long end) {

		// Ensure we have such a file
		if (!vmStatFiles.containsKey(nodeName)) {
			System.err.println("Unable to find a file for node " + nodeName);
			return false;
		}

		try {
			File file = vmStatFiles.get(nodeName);
			BufferedReader input = new BufferedReader(new FileReader(file));

			// Parse the file
			String line = null;
			out.println("time\tswpd\tfree\tbuff\tcache");
			while ((line = input.readLine()) != null) {
				// Line: time r b swpd free buff cache si <others>
				String[] pieces = p.split(line);
				if (!line.contains(SW) && pieces.length == 17
						&& satisfyBounds(pieces[0], start, end)) {
					out.print(pieces[0]);
					out.print(TAB);
					out.print(pieces[3]);
					out.print(TAB);
					out.print(pieces[4]);
					out.print(TAB);
					out.print(pieces[5]);
					out.print(TAB);
					out.println(pieces[6]);
				}

			}
			input.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * Export the IO statistics in a tabular format of the form
	 * "time\tMBRead/s\tMBWrite/s" only for the records between times start and
	 * end (epoch time in seconds)
	 * 
	 * @param out
	 *            the print stream to write to
	 * @param nodeName
	 *            the node name
	 * @param start
	 *            the start time in seconds
	 * @param end
	 *            the end time in seconds
	 * @return true if the export is successful
	 */
	private boolean exportIOStats(PrintStream out, String nodeName, long start,
			long end) {

		// Ensure we have such a file
		if (!ioStatFiles.containsKey(nodeName)) {
			System.err.println("Unable to find a file for node " + nodeName);
			return false;
		}

		try {
			BufferedReader input = new BufferedReader(new FileReader(
					ioStatFiles.get(nodeName)));

			// Parse the file
			String line = null;
			boolean hasTime = false;
			out.println("time\tMBRead/s\tMBWrite/s");
			while ((line = input.readLine()) != null) {
				if (line.contains(DEVICE)) {

					// The next line contains the data
					line = input.readLine();
					if (line != null) {
						// Line: time sda2 tps MB_read/s MB_wrtn/s <others>
						String[] pieces = p.split(line);
						if (pieces.length == 6
								|| (pieces.length == 7 && satisfyBounds(
										pieces[0], start, end))) {
							hasTime = pieces.length == 7;
							out.print(hasTime ? pieces[0] : EMPTY);
							out.print(TAB);
							out.print(hasTime ? pieces[3] : pieces[2]);
							out.print(TAB);
							out.println(hasTime ? pieces[4] : pieces[3]);
						}
					}
				}

			}
			input.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * Read the files in the monitor directory and populate the maps with the
	 * node files. Note that the data in the files are not loaded at this time.
	 * The data for each node will be loaded when the user asks to get some
	 * statistics.
	 * 
	 * The file names assumed to be "iostat_output-NODE" or
	 * "vmstat_output-NODE", where NODE is the actual node name.
	 */
	private void readMonitorDirectory() {
		if (monitorDir == null)
			return;

		// Check for a valid directory
		File dir = new File(monitorDir);
		if (!dir.isDirectory()) {
			System.err.println(dir.getAbsolutePath() + " is not a directory!");
			return;
		}

		// List all relevant iostat files
		for (File file : dir.listFiles()) {
			if (file.isFile() && !file.isHidden()) {
				String name = file.getName();
				if (name.startsWith(IOSTAT_PREFIX)) {
					// Found an iostat file
					ioStatFiles.put(name.substring(IOSTAT_PREFIX.length()),
							file);
				} else if (name.startsWith(VMSTAT_PREFIX)) {
					// Found a vmstat file
					vmStatFiles.put(name.substring(VMSTAT_PREFIX.length()),
							file);
				}
			}
		}

	}

	/**
	 * Returns true if the string value represents a number between start and
	 * end inclusive.
	 * 
	 * @param value
	 *            the value to check
	 * @param start
	 *            the start of the bound
	 * @param end
	 *            the end of the bound
	 * @return true if value satisfies the bound
	 */
	private boolean satisfyBounds(String value, long start, long end) {
		try {
			// Empty string is a special case
			if (value.equals(EMPTY))
				return true;

			// Check the bound
			long num = Long.parseLong(value);
			return num >= start && num <= end;
		} catch (NumberFormatException e) {
			return false;
		}
	}

}
