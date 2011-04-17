package edu.duke.starfish.whatif.junit;

import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.virtualfs.VirtualFSDataSetModel;
import edu.duke.starfish.whatif.virtualfs.VirtualFile;
import edu.duke.starfish.whatif.virtualfs.VirtualFileSystem;
import edu.duke.starfish.whatif.virtualfs.VirtualFileSystem.VirtualFSException;

/**
 * Test the XML Input Specs parser
 * 
 * @author hero
 */
public class TestVirtualFileSystem extends TestCase {

	@Test
	public void testCreateFile() {
		VirtualFileSystem vfs = new VirtualFileSystem();

		try {
			vfs.createFile("/dir_1/dir_1_1/file1111.txt", 128 << 20, false,
					64 << 20, 3);
			vfs.createFile("/dir_1/dir_1_1/file1112.txt", 64 << 20, true,
					64 << 20, 3);
			vfs.createFile("/dir_1/dir_2_1/file1211.txt", 32 << 20, true,
					64 << 20, 3);
			vfs.createFile("/dir_2/file21.txt", 378 << 20, false, 64 << 20, 3);

			VirtualFile file = vfs.listFiles("/dir_1/dir_1_1/file1111.txt",
					false).get(0);
			assertEquals("/dir_1/dir_1_1/file1111.txt", file.toString());
			assertEquals(128 << 20, file.getSize());
			assertEquals(false, file.isCompress());
			assertEquals(2, file.getBlocks().size());

			file = vfs.listFiles("/dir_1/dir_1_1/file1112.txt", false).get(0);
			assertEquals("/dir_1/dir_1_1/file1112.txt", file.toString());
			assertEquals(64 << 20, file.getSize());
			assertEquals(true, file.isCompress());
			assertEquals(1, file.getBlocks().size());

			file = vfs.listFiles("/dir_1/dir_2_1/file1211.txt", false).get(0);
			assertEquals("/dir_1/dir_2_1/file1211.txt", file.toString());
			assertEquals(32 << 20, file.getSize());
			assertEquals(true, file.isCompress());
			assertEquals(1, file.getBlocks().size());

			file = vfs.listFiles("/dir_2/file21.txt", false).get(0);
			assertEquals("/dir_2/file21.txt", file.toString());
			assertEquals(378 << 20, file.getSize());
			assertEquals(false, file.isCompress());
			assertEquals(6, file.getBlocks().size());

		} catch (VirtualFSException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testListFiles() {
		VirtualFileSystem vfs = new VirtualFileSystem();

		try {
			vfs.createFile("/dir_1/dir_1_1/file1111.txt", 128 << 20, false,
					64 << 20, 3);
			vfs.createFile("/dir_1/dir_1_1/file1112.txt", 64 << 20, true,
					64 << 20, 3);
			vfs.createFile("/dir_1/dir_2_1/file1211.txt", 32 << 20, true,
					64 << 20, 3);
			vfs.createFile("/dir_2/file21.txt", 378 << 20, false, 64 << 20, 3);

			List<VirtualFile> files = vfs.listFiles("/dir_2", true);
			assertEquals(1, files.size());
			assertEquals("/dir_2/file21.txt", files.get(0).toString());

			files = vfs.listFiles("/dir_2", false);
			assertEquals(1, files.size());
			assertEquals("/dir_2/file21.txt", files.get(0).toString());

			files = vfs.listFiles("/dir_1", true);
			assertEquals(3, files.size());

			files = vfs.listFiles("/dir_1", false);
			assertEquals(0, files.size());

			files = vfs.listFiles("/", true);
			assertEquals(4, files.size());

			files = vfs.listFiles("/dir_1/dir_1_1/file*", false);
			assertEquals(2, files.size());

			files = vfs.listFiles("/dir_1/dir_*/file*", false);
			assertEquals(3, files.size());

			files = vfs.listFiles("/dir_1/dir_*/*", false);
			assertEquals(3, files.size());

			files = vfs.listFiles("/dir_1/dir_*", false);
			assertEquals(3, files.size());

			files = vfs.listFiles("/dir_1/dir_1_1/file111[0-2].txt", false);
			assertEquals(2, files.size());

			files = vfs.listFiles("/dir_1/dir_1_1/*{111}[0-2].txt", false);
			assertEquals(2, files.size());

		} catch (VirtualFSException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testVirtualAvgDataSetModel() {
		VirtualFileSystem vfs = new VirtualFileSystem();

		try {
			vfs.createFile("/dir_1/dir_1_1/file1111.txt", 128 << 20, false,
					64 << 20, 3);
			vfs.createFile("/dir_1/dir_1_1/file1112.txt", 86 << 20, true,
					64 << 20, 3);
			vfs.createFile("/dir_1/dir_2_1/file1211.txt", 32 << 20, true,
					64 << 20, 3);
			vfs.createFile("/dir_2/file21.txt", 378 << 20, false, 64 << 20, 3);
		} catch (VirtualFSException e) {
			fail(e.getMessage());
		}

		// Test for 1 input
		Configuration conf = new Configuration(false);
		conf.set(VirtualFSDataSetModel.VIRTUAL_INPUT_DIRS, "/dir_1/dir_1_1/");

		VirtualFSDataSetModel model = new VirtualFSDataSetModel(vfs);
		List<MapInputSpecs> specs = model.generateMapInputSpecs(conf);

		assertEquals(3, specs.size());
		assertEquals(1, specs.get(0).getNumSplits());
		assertEquals(64 << 20, specs.get(0).getSize());

		// Test for 2 input
		conf.setStrings(VirtualFSDataSetModel.VIRTUAL_INPUT_DIRS,
				"/dir_1/dir_1_1/", "/dir_2");

		model = new VirtualFSDataSetModel(vfs);
		specs = model.generateMapInputSpecs(conf);

		assertEquals(5, specs.size());
		assertEquals(0, specs.get(0).getInputIndex());
		assertEquals(1, specs.get(0).getNumSplits());
		assertEquals(64 << 20, specs.get(0).getSize());
		assertEquals(1, specs.get(3).getInputIndex());
		assertEquals(5, specs.get(3).getNumSplits());
		assertEquals(64 << 20, specs.get(3).getSize());
		assertEquals(1, specs.get(4).getInputIndex());
		assertEquals(1, specs.get(4).getNumSplits());
		assertEquals(58 << 20, specs.get(4).getSize());

	}

}
