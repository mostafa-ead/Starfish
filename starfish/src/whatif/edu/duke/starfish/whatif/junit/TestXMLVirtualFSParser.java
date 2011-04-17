package edu.duke.starfish.whatif.junit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import org.junit.Test;

import edu.duke.starfish.whatif.virtualfs.VirtualFileSystem;
import edu.duke.starfish.whatif.virtualfs.XMLVirtualFSParser;
import edu.duke.starfish.whatif.virtualfs.VirtualFileSystem.VirtualFSException;

/**
 * Test the XML Input Specs parser
 * 
 * @author hero
 */
public class TestXMLVirtualFSParser extends TestCase {

	@Test
	public void testImportExportVirtualFileSystem() {
		VirtualFileSystem vfs = new VirtualFileSystem();

		try {
			vfs.createFile("/dir_1/dir_1_1/file1111.txt", 128 << 20, false,
					64 << 20, 3);
			vfs.createFile("/dir_1/dir_1_1/file1112.txt", 64 << 20, true,
					64 << 20, 3);
			vfs.createFile("/dir_1/dir_2_1/file1211.txt", 32 << 20, true,
					64 << 20, 3);
			vfs.createFile("/dir_2/file21.txt", 378 << 20, false, 64 << 20, 3);
		} catch (VirtualFSException e) {
			fail(e.getMessage());
		}

		// Write the XML output to a string
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		XMLVirtualFSParser.exportVirtualFileSystem(vfs, ps);

		// Read the XML input from the string
		String content = null;
		try {
			content = baos.toString("UTF-8");
			VirtualFileSystem newVFS = XMLVirtualFSParser
					.importVirtualFileSystem(new ByteArrayInputStream(content
							.getBytes("UTF-8")));

			// Ensure we got the same specs back
			assertEquals(vfs, newVFS);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

	}

}
