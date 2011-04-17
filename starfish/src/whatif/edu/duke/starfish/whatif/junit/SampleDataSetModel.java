package edu.duke.starfish.whatif.junit;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.MapInputSpecs;

/**
 * Simple oracle for job profiles, only for testing purposes
 * 
 * @author hero
 */
public class SampleDataSetModel extends DataSetModel {

	// Constants
	public static final String USE_AVG_PROFILE = "starfish.test.use.avg.profile";
	public static final String NUM_MAPPERS = "starfish.test.num.mappers";
	public static final String INPUT_SIZE = "starfish.test.input.size";
	public static final String INPUT_COMPR = "starfish.test.input.compressed";

	/**
	 * Generate map input specs based on input in the conf
	 */
	@Override
	public List<MapInputSpecs> generateMapInputSpecs(Configuration conf) {
		List<MapInputSpecs> inputSpecs = new ArrayList<MapInputSpecs>();

		boolean useAvg = conf.getBoolean(USE_AVG_PROFILE, true);
		int numMappers = conf.getInt(NUM_MAPPERS, 1);
		long size = conf.getLong(INPUT_SIZE, 0l);
		boolean isCompressed = conf.getBoolean(INPUT_COMPR, false);

		if (useAvg) {
			inputSpecs.add(new MapInputSpecs(0, numMappers, size, isCompressed,
					DataLocality.DATA_LOCAL));
		} else {
			for (int i = 0; i < numMappers; ++i)
				inputSpecs.add(new MapInputSpecs(0, 1, size, isCompressed,
						DataLocality.DATA_LOCAL));
		}

		return inputSpecs;
	}
}