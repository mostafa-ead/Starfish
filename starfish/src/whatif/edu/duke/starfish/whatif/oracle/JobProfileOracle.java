package edu.duke.starfish.whatif.oracle;

import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_INPUT_DIR;
import static edu.duke.starfish.profile.profileinfo.utils.Constants.MR_RED_TASKS;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.whatif.data.DataSetModel;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.data.ReduceShuffleSpecs;

/**
 * This class is used to make predictions on how a job profile will change based
 * on a set of configuration settings.
 * 
 * @author hero
 */
public class JobProfileOracle {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private MRJobProfile sourceProf;// The source profile for the predictions
	private MRJobProfile virtualProf; // Cache the last predicted profile
	private Configuration conf; // Cache the last configuration

	private ArrayList<MapProfileOracle> mapOracles; // The map oracles
	private ReduceProfileOracle redOracle; // The reduce oracle

	private boolean ignoreReducers; // Flag to ignore reducers

	// Constants
	private static final String VIRTUAL = "virtual_";

	/**
	 * Constructor
	 * 
	 * @param sourceProf
	 *            the source job profile
	 */
	public JobProfileOracle(MRJobProfile sourceProf) {
		this.sourceProf = sourceProf;
		this.virtualProf = null;
		this.conf = null;
		this.ignoreReducers = false;

		// Create the map oracles
		mapOracles = new ArrayList<MapProfileOracle>(sourceProf
				.getAvgMapProfiles().size());
		for (MRMapProfile mapProf : sourceProf.getAvgMapProfiles()) {
			mapOracles.add(new MapProfileOracle(mapProf));
		}

		// Create the reduce oracles
		redOracle = new ReduceProfileOracle(sourceProf.getAvgReduceProfile());
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @param ignoreReducers
	 *            set ignore reducers flag
	 */
	public void setIgnoreReducers(boolean ignoreReducers) {
		this.ignoreReducers = ignoreReducers;
	}

	/**
	 * @return the sourceProf
	 */
	public MRJobProfile getSourceProf() {
		return sourceProf;
	}

	/**
	 * Generate and return a virtual job profile representing how the job will
	 * behave under the provided configuration settings.
	 * 
	 * @param conf
	 *            the configuration settings
	 * @param dataModel
	 *            the data model that can reason about the data
	 * @return a virtual job profile
	 */
	public MRJobProfile whatif(Configuration conf, DataSetModel dataModel) {
		this.virtualProf = new MRJobProfile(VIRTUAL + sourceProf.getJobId());
		this.conf = conf;

		// Set the cluster name and job inputs
		virtualProf.setClusterName(sourceProf.getClusterName());
		virtualProf.setJobInputs(StringUtils.split(conf.get(MR_INPUT_DIR, "")));

		// Get the input specs
		List<MapInputSpecs> inputSpecs = dataModel
				.generateMapInputSpecs(this.conf);

		// Predict the map execution
		int numMappers = 0;
		for (MapInputSpecs inputSpec : inputSpecs) {
			MRMapProfile mapProf = mapOracles.get(inputSpec.getInputIndex())
					.whatif(conf, inputSpec);
			numMappers += inputSpec.getNumSplits();
			virtualProf.addMapProfile(mapProf);
		}

		// Predict the reduce execution
		int numReducers = conf.getInt(MR_RED_TASKS, 1);
		if (numReducers > 0 && !ignoreReducers) {
			// Get the shuffle specs
			List<ReduceShuffleSpecs> shuffleSpecs = dataModel
					.generateReduceShuffleSpecs(conf, virtualProf
							.getMapProfiles());

			for (ReduceShuffleSpecs shuffleSpec : shuffleSpecs) {
				MRReduceProfile redProf = redOracle.whatif(conf, shuffleSpec);
				virtualProf.addReduceProfile(redProf);
			}
		}

		// Update the averaged task profiles
		virtualProf.updateProfile();
		virtualProf.addCounter(MRCounter.MAP_TASKS, (long) numMappers);
		virtualProf.addCounter(MRCounter.REDUCE_TASKS, (long) numReducers);

		return virtualProf;
	}

}
