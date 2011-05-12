package edu.duke.starfish.whatif.virtualfs;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.JobOutputSpecs;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.virtualfs.VirtualFileSystem.VirtualFSException;

/**
 * A DataSetModel that communicates with the VirtualFileSystem to gather
 * information about the input. The virtual input paths (comma-separated list)
 * are expected in the Hadoop parameter: starfish.virtual.input.dirs
 * 
 * @author hero
 */
public class VirtualFSDataSetModel extends DataSetModel {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */
	private VirtualFileSystem vfs;

	public static final String VIRTUAL_INPUT_DIRS = "starfish.virtual.input.dirs";

	private static final NumberFormat nf = NumberFormat.getNumberInstance();
	{
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);
	}

	/**
	 * Constructor
	 * 
	 * @param vfs
	 *            virtual file system
	 */
	public VirtualFSDataSetModel(VirtualFileSystem vfs) {
		this.vfs = vfs;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see edu.duke.starfish.whatif.data.DataSetModel#generateMapInputSpecs(Configuration)
	 */
	@Override
	public List<MapInputSpecs> generateMapInputSpecs(Configuration conf) {

		// Get the input directories
		String[] dirList = conf.getStrings(VIRTUAL_INPUT_DIRS);
		if (dirList == null || dirList.length == 0) {
			throw new RuntimeException(
					"ERROR: No input was specified in the conf file");
		}

		// Create the input specs
		List<MapInputSpecs> inputSpecs = new ArrayList<MapInputSpecs>();
		for (int i = 0; i < dirList.length; ++i) {
			try {
				List<VirtualFile> files = vfs.listFiles(dirList[i], true);

				for (VirtualFile file : files) {
					if (file.isCompress()
							&& !conf.get(MR_INPUT_FORMAT_CLASS, MR_TIF).equals(
									MR_SFIF)) {
						// File is compressed and cannot be split
						inputSpecs.add(new MapInputSpecs(i, 1, file.getSize(),
								file.isCompress(), DataLocality.DATA_LOCAL));
					} else {
						// Create the specs per blocks
						List<VirtualFileBlock> blocks = file.getBlocks();
						if (blocks.size() > 1) {
							inputSpecs
									.add(new MapInputSpecs(i,
											blocks.size() - 1, blocks.get(0)
													.getSize(), file
													.isCompress(),
											DataLocality.DATA_LOCAL));
						}

						inputSpecs.add(new MapInputSpecs(i, 1, blocks.get(
								blocks.size() - 1).getSize(),
								file.isCompress(), DataLocality.DATA_LOCAL));
					}
				}

			} catch (VirtualFSException e) {
				throw new RuntimeException(e);
			}
		}

		return inputSpecs;
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Set the virtual input paths in the conf
	 * 
	 * @param conf
	 *            the configuration
	 * @param inputPaths
	 *            the input paths
	 */
	public static void setVirtualInputPaths(Configuration conf,
			String... inputPaths) {
		if (inputPaths.length == 0)
			throw new RuntimeException("ERROR: No input paths for the job");

		// Build the virtual file paths
		String virtualFilePaths = null;
		for (String inputPath : inputPaths) {
			if (inputPath.startsWith(VirtualFileSystem.SEPARATOR)) {
				if (virtualFilePaths == null) {
					virtualFilePaths = inputPath.trim();
				} else {
					virtualFilePaths += "," + inputPath.trim();
				}
			}
		}
		if (virtualFilePaths == null)
			throw new RuntimeException(
					"ERROR: No valid input paths for the job");

		conf.set(VIRTUAL_INPUT_DIRS, virtualFilePaths);
	}

	/**
	 * Set the virtual output paths in the virtual file system
	 * 
	 * @param vfs
	 *            the virtual file system
	 * @param conf
	 *            the configuration
	 * @param outputDir
	 *            the output directory
	 * @param outSpecs
	 *            the job output specs
	 */
	public static void setVirtualOutputPaths(VirtualFileSystem vfs,
			Configuration conf, String outputDir, List<JobOutputSpecs> outSpecs) {

		boolean mapOnly = conf.getInt(MR_RED_TASKS, 1) == 0;
		boolean compress = conf.getBoolean(MR_COMPRESS_OUT, false);

		// Trim trailing separator
		outputDir.trim();
		if (outputDir.endsWith(VirtualFileSystem.SEPARATOR))
			outputDir = outputDir.substring(0, outputDir.length() - 1);

		int id = 0;
		try {
			// Delete the output directory if it exists
			vfs.deleteFiles(outputDir, true);

			// Create an output file for each reduce task
			for (JobOutputSpecs outSpec : outSpecs) {
				for (int i = 0; i < outSpec.getNumTasks(); ++i) {
					vfs.createFile(buildOutputName(outputDir, id, mapOnly,
							compress), outSpec.getSize(), compress);
					++id;
				}
			}
		} catch (VirtualFSException e) {
			e.printStackTrace();
		}
	}

	private static String buildOutputName(String baseDir, int id,
			boolean mapOnly, boolean compress) {
		StringBuilder sb = new StringBuilder();

		sb.append(baseDir);
		sb.append(VirtualFileSystem.SEPARATOR);
		sb.append("part-");
		sb.append(mapOnly ? "m-" : "r-");
		sb.append(nf.format(id));
		if (compress)
			sb.append(".deflate");

		return sb.toString();
	}
}
