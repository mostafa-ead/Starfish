package edu.duke.starfish.whatif.data;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
import org.apache.hadoop.util.StringUtils;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;

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

	private List<MapInputSpecs> inputSpecsCache;
	private String dirsCache;

	/**
	 * Default Constructor
	 */
	public RealAvgDataSetModel() {
		inputSpecsCache = null;
		dirsCache = null;
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

		// Check the cache first
		if (inputSpecsCache != null && dirsCache != null
				&& dirsCache.equals(conf.get(MR_INPUT_DIR, ""))) {
			return inputSpecsCache;
		}

		// Get the input format
		JobContext context = new JobContext(conf, null);
		InputFormat<?, ?> input = null;
		try {
			input = ReflectionUtils.newInstance(context.getInputFormatClass(),
					context.getConfiguration());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		// Get the list of input directories
		dirsCache = context.getConfiguration().get(MR_INPUT_DIR, "");
		String[] dirList = StringUtils.split(dirsCache);
		if (dirList.length == 0) {
			throw new RuntimeException(
					"ERROR: No input was specified in the conf file");
		}

		// Get the input splits for each input dir
		inputSpecsCache = new ArrayList<MapInputSpecs>(dirList.length);

		try {
			for (int i = 0; i < dirList.length; ++i) {

				context.getConfiguration().set(MR_INPUT_DIR, dirList[i]);
				List<InputSplit> splits = input.getSplits(context);
				if (splits != null && splits.size() > 0) {

					// Sort the splits based on length in decreasing order
					Collections.sort(splits, new Comparator<InputSplit>() {
						@Override
						public int compare(InputSplit arg0, InputSplit arg1) {
							try {
								if (arg0.getLength() == arg1.getLength())
									return 0;
								else
									return arg0.getLength() < arg1.getLength() ? 1
											: -1;
							} catch (IOException e) {
							} catch (InterruptedException e) {
							}
							return 0;
						}
					});

					// Assume if one split is compressed, they all are
					boolean compression = isInputSplitCompressed(context,
							splits.get(0));

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
							inputSpecsCache.add(new MapInputSpecs(i, count,
									(long) (groupSum / count), compression,
									DataLocality.DATA_LOCAL));
							count = 1;
							groupSum = splits.get(j + 1).getLength();
							groupInitLength = splits.get(j + 1).getLength();
						}
					}

					// Add the input specs for the last set of splits
					inputSpecsCache.add(new MapInputSpecs(i, count,
							(long) (groupSum / count), compression,
							DataLocality.DATA_LOCAL));
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		// Reset the input directories
		context.getConfiguration().set(MR_INPUT_DIR, dirsCache);

		return inputSpecsCache;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Returns true if the input split is for a compressed file
	 * 
	 * @param context
	 *            the job context
	 * @param split
	 *            the input split
	 * @return true if the input split is for a compressed file
	 */
	private boolean isInputSplitCompressed(JobContext context, InputSplit split) {

		if (split instanceof FileSplit) {
			FileSplit fileSplit = (FileSplit) split;
			Configuration conf = context.getConfiguration();

			if (conf.get(MR_INPUT_FORMAT_CLASS, MR_TIF).equals(MR_SFIF)) {
				// SequenceFile split
				Path path = fileSplit.getPath();
				try {
					FileSystem fs = path.getFileSystem(conf);
					SequenceFile.Reader in = new SequenceFile.Reader(fs, path,
							conf);
					return in.isBlockCompressed() || in.isCompressed();
				} catch (IOException e) {
				}
			} else {
				// Other file split
				CompressionCodec codec = new CompressionCodecFactory(context
						.getConfiguration()).getCodec(fileSplit.getPath());
				return codec != null;
			}
		}

		return false;
	}

}
