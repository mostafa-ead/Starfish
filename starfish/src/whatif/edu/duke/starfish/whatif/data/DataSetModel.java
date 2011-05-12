package edu.duke.starfish.whatif.data;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;

/**
 * The base class for the Dataset Model, which is responsible for reasoning
 * about the input, intermediate, and output datasets of a MapReduce job.
 * 
 * The default implementation to generate reduce shuffle specs assumes no skew.
 * 
 * @author hero
 */
public abstract class DataSetModel {

	/**
	 * Generate the input specifications for the map oracles
	 * 
	 * @param conf
	 *            the configuration settings
	 * @return a list of input specifications
	 */
	public abstract List<MapInputSpecs> generateMapInputSpecs(Configuration conf);

	/**
	 * Generate the reduce shuffle specifications
	 * 
	 * @param conf
	 *            the job configuration
	 * @param mapProfiles
	 *            the map profiles
	 * @return the shuffle specifications
	 */
	public List<ReduceShuffleSpecs> generateReduceShuffleSpecs(
			Configuration conf, List<MRMapProfile> mapProfiles) {
		double shuffleSize = 0l;
		double shuffleRecs = 0l;
		int numMappers = 0;
		int numReducers = conf.getInt(MR_RED_TASKS, 1);

		for (MRMapProfile mapProf : mapProfiles) {
			// Add up the total shuffle size
			shuffleSize += mapProf.getNumTasks()
					* (mapProf.getCounter(MRCounter.FILE_BYTES_WRITTEN, 0l) - mapProf
							.getCounter(MRCounter.FILE_BYTES_READ, 0l));

			// Add up the total number of records
			shuffleRecs += mapProf.getNumTasks()
					* (mapProf.getCounter(MRCounter.COMBINE_OUTPUT_RECORDS, 0l)
							+ mapProf.getCounter(MRCounter.MAP_OUTPUT_RECORDS,
									0l) - mapProf.getCounter(
							MRCounter.COMBINE_INPUT_RECORDS, 0l));

			// Add up the total number of mappers
			numMappers += mapProf.getNumTasks();
		}

		// Assuming no skew, each reducer receives the same amount of data
		shuffleSize = Math.round(shuffleSize / numReducers);
		shuffleRecs = Math.round(shuffleRecs / numReducers);

		List<ReduceShuffleSpecs> shuffleSpecs = new ArrayList<ReduceShuffleSpecs>(
				1);
		shuffleSpecs.add(new ReduceShuffleSpecs(numMappers, numReducers,
				(long) shuffleSize, (long) shuffleRecs));
		return shuffleSpecs;
	}

	/**
	 * Generate the job output specifications
	 * 
	 * @param conf
	 *            the job configuration
	 * @param jobProfile
	 *            the job profile
	 * @return the data output specifications
	 */
	public List<JobOutputSpecs> generateJobOutputSpecs(Configuration conf,
			MRJobProfile jobProfile) {

		List<JobOutputSpecs> outputSpecs = null;
		if (jobProfile.getReduceProfiles().size() == 0) {
			// This is a map-only job
			List<MRMapProfile> mapProfiles = jobProfile.getMapProfiles();
			outputSpecs = new ArrayList<JobOutputSpecs>(mapProfiles.size());
			for (MRMapProfile prof : mapProfiles) {
				outputSpecs.add(new JobOutputSpecs(prof.getNumTasks(), prof
						.getCounter(MRCounter.MAP_OUTPUT_BYTES, 0l), prof
						.getCounter(MRCounter.MAP_OUTPUT_RECORDS, 0l)));
			}
		} else {
			// The is a map-reduce job
			List<MRReduceProfile> redProfiles = jobProfile.getReduceProfiles();
			outputSpecs = new ArrayList<JobOutputSpecs>(redProfiles.size());
			for (MRReduceProfile prof : redProfiles) {
				outputSpecs.add(new JobOutputSpecs(prof.getNumTasks(), prof
						.getCounter(MRCounter.REDUCE_OUTPUT_BYTES, 0l), prof
						.getCounter(MRCounter.REDUCE_OUTPUT_RECORDS, 0l)));
			}
		}

		return outputSpecs;
	}

}
