package edu.duke.starfish.whatif.virtualfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.duke.starfish.whatif.virtualfs.VirtualFileSystem.VirtualFSException;

/**
 * An XML parser for creating and reading Virtual File System XML files. An
 * input spacs XML file contains a list of MapInputSpecs.
 * 
 * @author hero
 */
public class XMLVirtualFSParser {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Constants
	private static final String FILESYSTEM = "filesystem";
	private static final String DEF_BLOCK_SIZE = "defaultBlockSize";
	private static final String DEF_REPLICATION = "defaultReplication";

	private static final String FILE = "file";
	private static final String PATH = "path";
	private static final String SIZE = "size";
	private static final String COMPRESS = "compress";
	private static final String BLOCK_SIZE = "blockSize";
	private static final String REPLICATION = "replication";

	/* ***************************************************************
	 * PUBLID METHODS
	 * ***************************************************************
	 */

	/**
	 * Load the virtual file system from the XML file
	 * 
	 * @param inputFile
	 *            the input file to read from
	 * @return the virtual file system
	 */
	public static VirtualFileSystem importVirtualFileSystem(File inputFile) {
		try {
			return importVirtualFileSystem(new FileInputStream(inputFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Load the virtual file system from the XML representation
	 * 
	 * @param in
	 *            the input stream to read from
	 * @return the virtual file system
	 */
	public static VirtualFileSystem importVirtualFileSystem(InputStream in) {

		// Parse the input stream
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setIgnoringComments(true);
		Document doc = null;
		try {
			doc = dbf.newDocumentBuilder().parse(in);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
			return null;
		} catch (SAXException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		// Get the root element
		Element root = doc.getDocumentElement();
		if (!FILESYSTEM.equals(root.getTagName()))
			throw new RuntimeException(
					"ERROR: Bad XML File: top-level element not <filesystem>");

		// Get the file system properties
		VirtualFileSystem vfs = new VirtualFileSystem();
		vfs.setDefaultBlockSize(Long.parseLong(root
				.getAttribute(DEF_BLOCK_SIZE)));
		vfs.setDefaultReplication(Integer.parseInt(root
				.getAttribute(DEF_REPLICATION)));

		// Get the splits
		NodeList splits = root.getElementsByTagName(FILE);
		for (int i = 0; i < splits.getLength(); ++i) {
			if (splits.item(i) instanceof Element) {
				Element split = (Element) splits.item(i);

				// Get the file properties
				String path = split.getAttribute(PATH);
				long size = Long.parseLong(split.getAttribute(SIZE));
				boolean isCompressed = Boolean.parseBoolean(split
						.getAttribute(COMPRESS));
				long blockSize = (split.getAttribute(BLOCK_SIZE).equals("")) ? vfs
						.getDefaultBlockSize()
						: Long.parseLong(split.getAttribute(BLOCK_SIZE));
				int replication = (split.getAttribute(REPLICATION).equals("")) ? vfs
						.getDefaultReplication()
						: Integer.parseInt(split.getAttribute(REPLICATION));

				try {
					vfs.createFile(path, size, isCompressed, blockSize,
							replication);
				} catch (VirtualFSException e) {
					e.printStackTrace();
				}
			}
		}

		return vfs;
	}

	/**
	 * Create an XML DOM representing the virtual file system and write out to
	 * the provided output file
	 * 
	 * @param vfs
	 *            the virtual file system
	 * @param outFile
	 *            the output file
	 */
	public static void exportVirtualFileSystem(VirtualFileSystem vfs,
			File outFile) {
		try {
			exportVirtualFileSystem(vfs, new PrintStream(outFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create an XML DOM representing the virtual file system and write out to
	 * the provided output stream
	 * 
	 * @param vfs
	 *            the virtual file system
	 * @param out
	 *            the output stream
	 */
	public static void exportVirtualFileSystem(VirtualFileSystem vfs,
			PrintStream out) {

		Document doc = null;
		try {
			// Workaround to get the right FactoryBuilder
			System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
					"com.sun.org.apache.xerces."
							+ "internal.jaxp.DocumentBuilderFactoryImpl");
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
					.newDocument();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}

		// Create the file system element
		Element fsElem = doc.createElement(FILESYSTEM);
		fsElem.setAttribute(DEF_BLOCK_SIZE, Long.toString(vfs
				.getDefaultBlockSize()));
		fsElem.setAttribute(DEF_REPLICATION, Long.toString(vfs
				.getDefaultReplication()));
		doc.appendChild(fsElem);

		// Get all the files
		List<VirtualFile> files = null;
		try {
			files = vfs.listFiles("/", true);
		} catch (VirtualFSException e1) {
			e1.printStackTrace();
			return;
		}

		// Add the file elements
		for (VirtualFile file : files) {

			Element split = doc.createElement(FILE);
			split.setAttribute(PATH, file.toString());
			split.setAttribute(SIZE, Long.toString(file.getSize()));
			split.setAttribute(COMPRESS, Boolean.toString(file.isCompress()));
			split.setAttribute(BLOCK_SIZE, Long.toString(file.getBlockSize()));
			split.setAttribute(REPLICATION, Integer.toString(file
					.getReplication()));

			fsElem.appendChild(split);
		}

		// Write out the XML output
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(out);
		try {
			Transformer transformer = TransformerFactory.newInstance()
					.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(source, result);
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
	}

}
