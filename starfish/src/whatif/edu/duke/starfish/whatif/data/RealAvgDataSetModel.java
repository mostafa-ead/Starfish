package edu.duke.starfish.whatif.data;

import static edu.duke.starfish.profile.utils.Constants.MR_INPUT_FORMAT_CLASS;
import static edu.duke.starfish.profile.utils.Constants.MR_SFIF;
import static edu.duke.starfish.profile.utils.Constants.MR_TIF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.utils.GeneralUtils;
import edu.duke.starfish.profile.utils.ProfileUtils;

/**
 * This data set model assumes the input for this job is real and exists in the
 * default file system. It also averages out the data across the reducers.
 * 
 * @author hero
 */
public class RealAvgDataSetModel extends DataSetModel {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private static final Log LOG = LogFactory.getLog(RealAvgDataSetModel.class);

	private List<MapInputSpecs> inputSpecsCache;

	/**
	 * Default Constructor
	 */
	public RealAvgDataSetModel() {
		inputSpecsCache = null;
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * ***************************************************************
	 */

	/**
	 * @see DataSetModel#generateMapInputSpecs(Configuration)
	 */
	@Override
	public List<MapInputSpecs> generateMapInputSpecs(Configuration conf) {

		// Check the cache first
		if (inputSpecsCache != null)
			return inputSpecsCache;
		inputSpecsCache = new ArrayList<MapInputSpecs>();

		// Get the input format
		JobContext context = new JobContext(conf, null);
		conf = context.getConfiguration();
		InputFormat<?, ?> input = null;
		try {
			input = ReflectionUtils.newInstance(context.getInputFormatClass(),
					conf);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		try {
			// Get all the input splits
			List<InputSplit> allSplits = input.getSplits(context);
			if (allSplits == null || allSplits.size() == 0) {
				LOG.error("ERROR: No input splits were found!");
				return inputSpecsCache;
			}

			// Separate the input splits into groups based on input
			List<List<InputSplit>> sepSplits = separateInputSplits(conf,
					allSplits);

			// Convert a group of input splits into input specs
			for (int i = 0; i < sepSplits.size(); ++i) {
				convertInputSplitsToInputSpecs(conf, sepSplits.get(i),
						inputSpecsCache, i);
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		return inputSpecsCache;
	}

	/* ***************************************************************
	 * PROTECTED METHODS
	 * ***************************************************************
	 */

	/**
	 * Convert a list of input splits (that are associated with the same logical
	 * input) into input specs and add them to the provided list of input specs.
	 * 
	 * NOTE: For efficiency, we are not adding one input spec per split.
	 * Instead, we group the input splits based on size similarity and create
	 * one input spec per group.
	 * 
	 * ASSUMPTION: Either all splits are compressed or they are all uncompressed
	 * 
	 * @param conf
	 *            the job configuration
	 * @param splits
	 *            the input splits to convert
	 * @param inputSpecs
	 *            the list to input specs to populate
	 * @param inputIndex
	 *            the input index for this set of splits
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void convertInputSplitsToInputSpecs(Configuration conf,
			List<InputSplit> splits, List<MapInputSpecs> inputSpecs,
			int inputIndex) throws IOException, InterruptedException {

		if (splits.size() == 0) {
			// Nothing to do
			LOG.warn("No input splits for logical input " + inputIndex);
			return;
		}

		// Sort the splits based on length in decreasing order
		Collections.sort(splits, new Comparator<InputSplit>() {
			@Override
			public int compare(InputSplit arg0, InputSplit arg1) {
				try {
					if (arg0.getLength() == arg1.getLength())
						return 0;
					else
						return arg0.getLength() < arg1.getLength() ? 1 : -1;
				} catch (IOException e) {
				} catch (InterruptedException e) {
				}
				return 0;
			}
		});

		// Assume if one split is compressed, they all are
		boolean compression = isInputSplitCompressed(conf, splits.get(0));

		// Group splits with similar lengths and create the specs
		int count = 1;
		double groupSum = splits.get(0).getLength();
		double groupInitLength = splits.get(0).getLength();

		int numSplits = splits.size();
		for (int j = 0; j < numSplits - 1; ++j) {

			if ((groupInitLength - splits.get(j + 1).getLength())
					/ groupInitLength < 0.2) {
				// Split length within 20% of the first one in group
				++count;
				groupSum += splits.get(j + 1).getLength();
			} else {
				// Create an input spec for this group
				inputSpecs.add(new MapInputSpecs(inputIndex, count,
						(long) (groupSum / count), compression,
						DataLocality.DATA_LOCAL));
				count = 1;
				groupSum = splits.get(j + 1).getLength();
				groupInitLength = splits.get(j + 1).getLength();
			}
		}

		// Add the input specs for the last set of splits
		inputSpecs
				.add(new MapInputSpecs(inputIndex, count,
						(long) (groupSum / count), compression,
						DataLocality.DATA_LOCAL));
	}

	/**
	 * Returns true if the input split is for a compressed file
	 * 
	 * @param conf
	 *            the job configuration
	 * @param split
	 *            the input split
	 * @return true if the input split is for a compressed file
	 */
	protected boolean isInputSplitCompressed(Configuration conf,
			InputSplit split) {

		if (split instanceof FileSplit) {

			// Check the file extension first
			FileSplit fileSplit = (FileSplit) split;
			Path path = fileSplit.getPath();
			if (GeneralUtils.hasCompressionExtension(path.getName()))
				return true;

			if (conf.get(MR_INPUT_FORMAT_CLASS, MR_TIF).equals(MR_SFIF)) {
				// SequenceFile split
				try {
					FileSystem fs = path.getFileSystem(conf);
					SequenceFile.Reader in = new SequenceFile.Reader(fs, path,
							conf);
					return in.isBlockCompressed() || in.isCompressed();
				} catch (IOException e) {
				}
			} else {
				// Other file split
				CompressionCodec codec = new CompressionCodecFactory(conf)
						.getCodec(path);
				return codec != null;
			}
		}

		return false;
	}

	/**
	 * Separate the input splits into groups where all splits within a single
	 * group belong to the same logical input.
	 * 
	 * NOTE: By convention, if no separation is possible, the entire input split
	 * is returned as a single group.
	 * 
	 * @param conf
	 *            the job configuration
	 * @param allSplits
	 *            all input splits
	 * @return a list of lists of input splits
	 */
	protected List<List<InputSplit>> separateInputSplits(Configuration conf,
			List<InputSplit> allSplits) {

		String[] jobInputs = ProfileUtils.getInputDirs(conf);
		if (jobInputs == null || jobInputs.length == 0 || jobInputs.length == 1) {
			// No separation possible/necessary
			List<List<InputSplit>> result = new ArrayList<List<InputSplit>>(1);
			result.add(allSplits);
			return result;
		}

		// Create the list of lists of input splits
		List<List<InputSplit>> result = new ArrayList<List<InputSplit>>(
				jobInputs.length);
		for (int i = 0; i < jobInputs.length; ++i)
			result.add(new ArrayList<InputSplit>());

		for (InputSplit split : allSplits) {
			// Find the input index for this split
			int index = 0;
			if (split instanceof FileSplit) {
				String mapInput = ((FileSplit) split).getPath().toString();
				index = GeneralUtils.getIndexInPathArray(jobInputs, mapInput);
				if (index == -1)
					index = 0;
			}

			// Add the split to the appropriate list
			result.get(index).add(split);
		}

		return result;
	}
}
