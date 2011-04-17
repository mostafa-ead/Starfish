package edu.duke.starfish.whatif.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
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

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;

/**
 * An XML parser for creating and reading input specs XML files. An input spacs
 * XML file contains a list of MapInputSpecs.
 * 
 * @author hero
 */
public class XMLInputSpecsParser {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Constants
	private static final String INPUT = "input";
	private static final String SPLIT = "split";

	private static final String INPUT_INDEX = "input_index";
	private static final String NUM_SPLITS = "num_splits";
	private static final String SIZE = "size";
	private static final String COMPRESS = "compress";

	/* ***************************************************************
	 * PUBLID METHODS
	 * ***************************************************************
	 */

	/**
	 * Load the map input specifications from the XML file
	 * 
	 * @param inputFile
	 *            the input file to read from
	 * @return the map input specifications
	 */
	public static List<MapInputSpecs> importMapInputSpecs(File inputFile) {
		try {
			return importMapInputSpecs(new FileInputStream(inputFile));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Load the map input specifications from the XML representation
	 * 
	 * @param in
	 *            the input stream to read from
	 * @return the map input specifications
	 */
	public static List<MapInputSpecs> importMapInputSpecs(InputStream in) {

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
		if (!INPUT.equals(root.getTagName()))
			throw new RuntimeException(
					"ERROR: Bad XML File: top-level element not <input>");

		// Get the splits
		ArrayList<MapInputSpecs> inputSpecs = new ArrayList<MapInputSpecs>();
		NodeList splits = root.getElementsByTagName(SPLIT);
		for (int i = 0; i < splits.getLength(); ++i) {
			if (splits.item(i) instanceof Element) {
				Element split = (Element) splits.item(i);

				// Get the split
				int inputIndex = Integer.parseInt(split
						.getAttribute(INPUT_INDEX));
				int numSplits = Integer
						.parseInt(split.getAttribute(NUM_SPLITS));
				long size = Long.parseLong(split.getAttribute(SIZE));
				boolean isCompressed = Boolean.parseBoolean(split
						.getAttribute(COMPRESS));

				inputSpecs.add(new MapInputSpecs(inputIndex, numSplits, size,
						isCompressed, DataLocality.DATA_LOCAL));
			}
		}

		return inputSpecs;
	}

	/**
	 * Create an XML DOM representing the map input specifications and write out
	 * to the provided output file
	 * 
	 * @param inputSpecs
	 *            the map input specifications
	 * @param outFile
	 *            the output file
	 */
	public static void exportMapInputSpecs(List<MapInputSpecs> inputSpecs,
			File outFile) {
		try {
			exportMapInputSpecs(inputSpecs, new PrintStream(outFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create an XML DOM representing the map input specifications and write out
	 * to the provided output stream
	 * 
	 * @param inputSpecs
	 *            the map input specifications
	 * @param out
	 *            the output stream
	 */
	public static void exportMapInputSpecs(List<MapInputSpecs> inputSpecs,
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

		// Create the cluster element
		Element clusterElem = doc.createElement(INPUT);
		doc.appendChild(clusterElem);

		// Add the input specs elements
		for (MapInputSpecs specs : inputSpecs) {

			// Add the split attributes
			Element split = doc.createElement(SPLIT);
			split.setAttribute(INPUT_INDEX, Integer.toString(specs
					.getInputIndex()));
			split.setAttribute(NUM_SPLITS, Integer.toString(specs
					.getNumSplits()));
			split.setAttribute(SIZE, Long.toString(specs.getSize()));
			split
					.setAttribute(COMPRESS, Boolean.toString(specs
							.isCompressed()));

			clusterElem.appendChild(split);
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
