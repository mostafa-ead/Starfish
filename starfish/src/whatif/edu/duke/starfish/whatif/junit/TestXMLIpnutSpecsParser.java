package edu.duke.starfish.whatif.junit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.data.XMLInputSpecsParser;

/**
 * Test the XML Input Specs parser
 * 
 * @author hero
 */
public class TestXMLIpnutSpecsParser extends TestCase {

	@Test
	public void testImportExportInputSpecs() {
		ArrayList<MapInputSpecs> inputSpecs = new ArrayList<MapInputSpecs>();
		inputSpecs.add(new MapInputSpecs(0, 5, 67108864l, false,
				DataLocality.DATA_LOCAL));
		inputSpecs.add(new MapInputSpecs(0, 1, 33554432l, false,
				DataLocality.DATA_LOCAL));
		inputSpecs.add(new MapInputSpecs(1, 1, 67108864l, true,
				DataLocality.DATA_LOCAL));

		// Write the XML output to a string
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		XMLInputSpecsParser.exportMapInputSpecs(inputSpecs, ps);

		// Read the XML input from the string
		String content = null;
		try {
			content = baos.toString("UTF-8");
			List<MapInputSpecs> newInputSpecs = XMLInputSpecsParser
					.importMapInputSpecs(new ByteArrayInputStream(content
							.getBytes("UTF-8")));

			// Ensure we got the same specs back
			assertEquals(inputSpecs, newInputSpecs);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

	}

}
