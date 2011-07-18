package edu.duke.starfish.profile.junit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import org.junit.Test;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.utils.XMLProfileParser;

/**
 * Test the XML profile parser
 * 
 * @author hero
 */
public class TestXMLProfileParser extends TestCase {

	@Test
	public void testImportExportProfile() {
		MRJobProfile profile = JUnitUtils.getTeraSortJobProfile();

		// Write the XML output to a string
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		XMLProfileParser.exportJobProfile(profile, ps);

		// Read the XML input from the string
		String content = null;
		try {
			content = baos.toString("UTF-8");
			MRJobProfile newProfile = XMLProfileParser
					.importJobProfile(new ByteArrayInputStream(content
							.getBytes("UTF-8")));

			// Ensure we got the same profile back
			assertEquals(profile.getJobId(), newProfile.getJobId());
			assertEquals(profile.getAvgMapProfiles(),
					newProfile.getAvgMapProfiles());
			assertEquals(profile.getAvgReduceProfile(),
					newProfile.getAvgReduceProfile());
			assertEquals(profile.getCostFactors(), newProfile.getCostFactors());
			assertEquals(profile.getCounters(), newProfile.getCounters());
			assertEquals(profile.getStatistics(), newProfile.getStatistics());
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testImportExportProfileMapOnly() {
		MRJobProfile profile = JUnitUtils.getTeraSortJobProfile();

		// Don't have a sample map-only job so just remove the reduce profile
		// and adjust some counters to turn terasort into map-onlys
		profile.getAvgMapProfiles().get(0)
				.addCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS, 0l);
		profile.getMapProfiles().get(0)
				.addCounter(MRCounter.MAP_MAX_UNIQUE_GROUPS, 0l);
		profile.getReduceProfiles().clear();
		profile.getAvgReduceProfile().clearProfile();
		profile.updateProfile();

		// Write the XML output to a string
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		XMLProfileParser.exportJobProfile(profile, ps);

		// Read the XML input from the string
		String content = null;
		try {
			content = baos.toString("UTF-8");
			MRJobProfile newProfile = XMLProfileParser
					.importJobProfile(new ByteArrayInputStream(content
							.getBytes("UTF-8")));

			// Ensure we got the same profile back
			assertEquals(profile.getJobId(), newProfile.getJobId());
			assertEquals(profile.getAvgMapProfiles(),
					newProfile.getAvgMapProfiles());
			assertEquals(profile.getAvgReduceProfile(),
					newProfile.getAvgReduceProfile());
			assertEquals(profile.getCostFactors(), newProfile.getCostFactors());
			assertEquals(profile.getCounters(), newProfile.getCounters());
			assertEquals(profile.getStatistics(), newProfile.getStatistics());
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

	}

}
