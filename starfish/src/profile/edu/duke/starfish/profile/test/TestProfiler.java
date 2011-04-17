package edu.duke.starfish.profile.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profiler.MRJobLogsManager;
import edu.duke.starfish.profile.profiler.XMLProfileParser;

/**
 * A simple class for testing the profiler
 * 
 * @author hero
 */
public class TestProfiler {

	/**
	 * A simple main to test the profiler
	 * 
	 * @param args
	 *            command line arguments - none used
	 */
	public static void main(String[] args) {

		MRJobLogsManager manager = new MRJobLogsManager();
		manager.setHistoryDir("/home/hero/Starfish/starfish/results/history");
		manager
				.setJobProfilesDir("/home/hero/Starfish/starfish/results/job_profiles");
		manager
				.setTaskProfilesDir("/home/hero/Starfish/starfish/task_profiles");
		manager.setTransfersDir("/home/hero/Starfish/starfish/transfers");

		for (MRJobInfo mrJob : manager.getAllMRJobInfos()) {

			System.out.println("-----------------------------------");
			System.out.println(mrJob.getExecId());
			
			 TestUtils.printClusterConfiguration(manager
			 .getClusterConfiguration(mrJob.getExecId()));
			
			 if (manager.loadTaskDetailsForMRJob(mrJob))
			 TestUtils.printMRJobInfo(mrJob);
			
			 if (manager.loadDataTransfersForMRJob(mrJob))
			 TestUtils.printMRJobDataTransfers(mrJob);

			if (manager.loadProfilesForMRJob(mrJob))
				if (manager.loadProfilesForMRJob(mrJob))
					mrJob.getProfile().printProfile(System.out, false);

			// Write the XML output to a string
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);
			XMLProfileParser.exportJobProfile(mrJob.getProfile(), ps);

			// Read the XML input from the string
			String content = null;
			try {
				content = baos.toString("UTF-8");
				MRJobProfile prof = XMLProfileParser
						.importJobProfile(new ByteArrayInputStream(content
								.getBytes("UTF-8")));
				prof.printProfile(System.out, false);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

		}
	}

}
