package edu.duke.starfish.profile.junit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.utils.XMLClusterParser;

import junit.framework.TestCase;

/**
 * Test the XML cluster parser
 * 
 * @author hero
 */
public class TestXMLClusterParser extends TestCase {

	@Test
	public void testImportExportCluster() {
		ClusterConfiguration cluster = JUnitUtils.getClusterConfiguration();

		// Write the XML output to a string
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		XMLClusterParser.exportCluster(cluster, ps);

		// Read the XML input from the string
		String content = null;
		try {
			content = baos.toString("UTF-8");
			ClusterConfiguration newCluster = XMLClusterParser
					.importCluster(new ByteArrayInputStream(content
							.getBytes("UTF-8")));

			// Ensure we got the same cluster back
			assertEquals(cluster, newCluster);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			fail();
		}

	}

	@Test
	public void testImportClusterFromSpecs() {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
				+ "<cluster name=\"local\">"
				+ "<specs num_racks=\"2\" hosts_per_rack=\"3\" map_slots_per_host=\"3\" "
				+ "reduce_slots_per_host=\"1\" max_slot_memory=\"100\" />"
				+ "</cluster>";

		try {
			ClusterConfiguration expCluster = ClusterConfiguration
					.createClusterConfiguration("local", 2, 3, 3, 1, 100l << 20);

			ClusterConfiguration newCluster = XMLClusterParser
					.importCluster(new ByteArrayInputStream(xml
							.getBytes("UTF-8")));

			assertEquals(expCluster, newCluster);

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			fail();
		}

	}
}
