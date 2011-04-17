package edu.duke.starfish.profile.profiler.loaders;

import java.io.File;

import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profiler.XMLProfileParser;

/**
 * This class is responsible for parsing the XML job profile file for a job and
 * populating the job profile.
 * 
 * @author hero
 */
public class MRJobProfileLoader {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// DATA MEMBERS
	private MRJobInfo mrJob; // The MR job
	private String jobProfilesDir; // the job profiles directory

	private boolean loaded; // Whether the profile is loaded or not

	/**
	 * Constructor
	 * 
	 * @param mrJob
	 *            the MR job
	 * @param jobProfilesDir
	 *            the job profiles directory
	 */
	public MRJobProfileLoader(MRJobInfo mrJob, String jobProfilesDir) {
		this.mrJob = mrJob;
		this.jobProfilesDir = jobProfilesDir;
		this.loaded = false;
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the adjusted job profile
	 */
	public MRJobProfile getAdjProfile() {
		if (!loaded)
			loadJobProfile(mrJob);

		return mrJob.getAdjProfile();
	}

	/**
	 * @return the job profile
	 */
	public MRJobProfile getProfile() {
		if (!loaded)
			loadJobProfile(mrJob);

		return mrJob.getProfile();
	}

	/**
	 * @return the job profiles directory
	 */
	public String getJobProfilesDir() {
		return jobProfilesDir;
	}

	/**
	 * Load the job profile for this job
	 * 
	 * @param mrJob
	 *            the map-reduce job whose profile to load
	 * @return true if the loading was successful
	 */
	public boolean loadJobProfile(MRJobInfo mrJob) {
		if (!this.mrJob.getExecId().equalsIgnoreCase(mrJob.getExecId())) {
			return false;
		} else if (loaded && this.mrJob == mrJob) {
			return true;
		}

		// Load all data into the profile
		this.mrJob = mrJob;

		// Ensure we have a valid job profiles directory
		File jobProfDir = new File(jobProfilesDir);
		if (!jobProfDir.isDirectory()) {
			System.err.println(jobProfDir.getAbsolutePath()
					+ " is not a directory!");
			return false;
		}

		// Get the adjusted job profile file
		boolean success = false;
		String jobId = mrJob.getExecId();
		File adjProfileXML = new File(jobProfDir, "adj_profile_" + jobId
				+ ".xml");
		if (adjProfileXML.exists()) {
			MRJobProfile jobProfile = XMLProfileParser
					.importJobProfile(adjProfileXML);
			mrJob.setAdjProfile(jobProfile);
			mrJob.setProfile(jobProfile);
			success = true;
		}

		// Get the regular job profile file
		File profileXML = new File(jobProfDir, "profile_" + jobId + ".xml");
		if (profileXML.exists()) {
			MRJobProfile jobProfile = XMLProfileParser
					.importJobProfile(profileXML);
			mrJob.setProfile(jobProfile);
			success = true;
		}

		loaded = success;
		return success;
	}

}
