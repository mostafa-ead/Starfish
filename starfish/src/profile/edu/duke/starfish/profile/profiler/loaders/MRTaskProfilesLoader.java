package edu.duke.starfish.profile.profiler.loaders;

import java.io.File;

import org.apache.hadoop.conf.Configuration;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.utils.Constants;
import edu.duke.starfish.profile.profiler.loaders.tasks.MRMapProfileLoader;
import edu.duke.starfish.profile.profiler.loaders.tasks.MRReduceProfileLoader;

/**
 * This class is responsible for parsing the BTrace profile files for all the
 * job's tasks and calculating all the profile information.
 * 
 * @author hero
 */
public class MRTaskProfilesLoader {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// DATA MEMBERS
	private MRJobInfo mrJob; // The MR job
	private Configuration conf; // The hadoop configuration
	private String inputDir; // the profiles or userlogs directory

	private boolean loaded; // Whether the files are loaded or not

	// CONSTANTS
	private static String PROFILE_OUT = "profile.out";
	private static String DOT_PROFILE = ".profile";

	/**
	 * Constructor
	 * 
	 * The profiles are expected in one of two places: (a)
	 * inputDir/attempt_id/profile.out (b) inputDir/attempt_id.profile
	 * 
	 * @param mrJob
	 *            the map-reduce job
	 * @param conf
	 *            the hadoop configuration
	 * @param inputDir
	 *            the profiles or userlogs directory
	 */
	public MRTaskProfilesLoader(MRJobInfo mrJob, Configuration conf,
			String inputDir) {
		this.mrJob = mrJob;
		this.conf = conf;
		this.inputDir = inputDir;
		this.loaded = false;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the profile
	 */
	public MRJobProfile getProfile() {
		if (!loaded)
			loadExecutionProfile(mrJob);

		return mrJob.getProfile();
	}

	/**
	 * @return the hadoop configuration
	 */
	public Configuration getConf() {
		return conf;
	}

	/**
	 * @return the profiles or userlogs directory
	 */
	public String getInputDir() {
		return inputDir;
	}

	/**
	 * Load the execution profiles for all the tasks in this job (for the tasks
	 * a profile file exists) and aggregates the job's profile
	 * 
	 * @param mrJob
	 *            the map-reduce job whose profile to load
	 * @return true if the loading was successful
	 */
	public boolean loadExecutionProfile(MRJobInfo mrJob) {
		if (!this.mrJob.getExecId().equalsIgnoreCase(mrJob.getExecId())) {
			return false;
		} else if (loaded && this.mrJob == mrJob) {
			return true;
		}

		// Load all data into the profile
		this.mrJob = mrJob;

		// Check the userlogs directory
		File filesDir = new File(inputDir);
		if (!filesDir.isDirectory()) {
			System.err.println(filesDir.getAbsolutePath()
					+ " is not a directory!");
			return false;
		}

		// Set the input paths
		MRJobProfile profile = new MRJobProfile(mrJob.getExecId());
		boolean success = false;

		// Load the map profiles
		for (MRMapAttemptInfo mrMap : mrJob
				.getMapAttempts(MRExecutionStatus.SUCCESS)) {
			success = loadTaskExecutionProfile(profile, filesDir, mrMap, true)
					|| success;
		}

		// Load the reduce profiles
		for (MRReduceAttemptInfo mrReduce : mrJob
				.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
			success = loadTaskExecutionProfile(profile, filesDir, mrReduce,
					false)
					|| success;
		}

		if (success) {
			// Update the job profile
			profile.setJobInputs(conf.getStrings(Constants.MR_INPUT_DIR, "NOT_FILE_SPLIT"));
			profile.updateProfile();

			// Set the number of map and reduce tasks
			profile.addCounter(MRCounter.MAP_TASKS, (long) mrJob.getMapTasks()
					.size());
			profile.addCounter(MRCounter.REDUCE_TASKS, (long) mrJob
					.getReduceTasks().size());

			// Retain the cluster name from the past profile, if any
			String clusterName = mrJob.getProfile().getClusterName();
			if (clusterName != null) {
				profile.setClusterName(clusterName);
			}

			// Set the job profile
			mrJob.setProfile(profile);
		}

		loaded = success;
		return success;
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Loads the execution profile for a task (for which a profile file exists).
	 * 
	 * @param filesDir
	 *            the profiles or userlogs directory
	 * @param task
	 *            the task to profile
	 * @param isMapTask
	 *            if the task is map or reduce
	 * @return true if all task profiles were loaded successfully
	 */
	private boolean loadTaskExecutionProfile(MRJobProfile profile,
			File filesDir, MRTaskAttemptInfo task, boolean isMapTask) {

		// Build the profile file path, which will be one of two options:
		// (a) filesDir/attemptDir/profile.out
		// (b) filesDir/attempt_id.profile
		File profileFile = null;
		File attemptDir = new File(filesDir, task.getExecId());
		if (attemptDir.isDirectory())
			profileFile = new File(attemptDir, PROFILE_OUT);
		else
			profileFile = new File(filesDir, task.getExecId() + DOT_PROFILE);

		// Ensure the profile file exists
		if (!profileFile.exists())
			return false;

		// Load the profile for the task
		if (isMapTask) {
			MRMapProfile mapProfile = (MRMapProfile) task.getProfile();
			MRMapProfileLoader loader = new MRMapProfileLoader(mapProfile,
					conf, profileFile.getAbsolutePath());
			if (loader.loadExecutionProfile(mapProfile)) {
				profile.addMapProfile(mapProfile);
				return true;
			}
		} else {
			MRReduceProfile reduceProfile = (MRReduceProfile) task.getProfile();
			MRReduceProfileLoader loader = new MRReduceProfileLoader(
					reduceProfile, conf, profileFile.getAbsolutePath());
			if (loader.loadExecutionProfile(reduceProfile)) {
				profile.addReduceProfile(reduceProfile);
				return true;
			}
		}
		return false;
	}

}
