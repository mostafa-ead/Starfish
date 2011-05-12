package edu.duke.starfish.profile.profiler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

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

import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;

/**
 * An XML parser for creating and reading profile XML files.
 * 
 * @author hero
 */
public class XMLProfileParser {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Constants - XML tags
	private static final String JOB_PROFILE = "job_profile";
	private static final String MAP_PROFILE = "map_profile";
	private static final String REDUCE_PROFILE = "reduce_profile";
	private static final String INPUTS = "inputs";
	private static final String INPUT = "input";
	private static final String COUNTERS = "counters";
	private static final String STATS = "statistics";
	private static final String FACTORS = "cost_factors";
	private static final String TIMINGS = "timings";
	private static final String COUNTER = "counter";
	private static final String STAT = "statistic";
	private static final String FACTOR = "cost_factor";
	private static final String TIMING = "timing";

	// Constants - XML attributes
	private static final String ID = "id";
	private static final String CLUSTER_NAME = "cluster_name";
	private static final String NUM_MAPPERS = "num_mappers";
	private static final String NUM_REDUCERS = "num_reducers";
	private static final String INPUT_INDEX = "input_index";
	private static final String NUM_TASKS = "num_tasks";
	private static final String KEY = "key";
	private static final String VALUE = "value";

	/* ***************************************************************
	 * PUBLID METHODS
	 * ***************************************************************
	 */

	/**
	 * Load a job profile from the XML file
	 * 
	 * @param inputFile
	 *            the input file to read from
	 * @return the job profile
	 */
	public static MRJobProfile importJobProfile(File inputFile) {
		try {
			return importJobProfile(new FileInputStream(inputFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Load a job profile from the XML representation
	 * 
	 * @param in
	 *            the input stream to read from
	 * @return the job profile
	 */
	public static MRJobProfile importJobProfile(InputStream in) {

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
		if (!JOB_PROFILE.equals(root.getTagName()))
			throw new RuntimeException(
					"ERROR: Bad XML File: top-level element not <job_profile>");

		// Get the profile attributes
		MRJobProfile jobProfile = new MRJobProfile(root.getAttribute(ID));
		jobProfile.addCounter(MRCounter.MAP_TASKS, Long.parseLong(root
				.getAttribute(NUM_MAPPERS)));
		jobProfile.addCounter(MRCounter.REDUCE_TASKS, Long.parseLong(root
				.getAttribute(NUM_REDUCERS)));

		String clusterName = root.getAttribute(CLUSTER_NAME);
		if (clusterName != null && !clusterName.equals("")) {
			jobProfile.setClusterName(clusterName);
		}

		// Get the profile inputs
		NodeList inputs = root.getElementsByTagName(INPUTS).item(0)
				.getChildNodes();
		ArrayList<String> inputList = new ArrayList<String>(1);
		for (int i = 0; i < inputs.getLength(); ++i) {
			if (inputs.item(i) instanceof Element) {
				inputList.add(inputs.item(i).getTextContent());
			}
		}
		jobProfile.setJobInputs(inputList.toArray(new String[0]));

		// Get the map profiles
		NodeList maps = root.getElementsByTagName(MAP_PROFILE);
		for (int i = 0; i < maps.getLength(); ++i) {
			if (maps.item(i) instanceof Element) {
				Element map = (Element) maps.item(i);

				// Get the map profile attributes
				MRMapProfile mapProf = new MRMapProfile(map.getAttribute(ID));
				mapProf.setInputIndex(Integer.parseInt(map
						.getAttribute(INPUT_INDEX)));
				mapProf.setNumTasks(Integer.parseInt(map
						.getAttribute(NUM_TASKS)));

				// Get the enum maps
				loadTaskProfileCounters(mapProf, map);
				loadTaskProfileStatistics(mapProf, map);
				loadTaskProfileCostFactors(mapProf, map);
				loadTaskProfileTimings(mapProf, map);

				jobProfile.addMapProfile(mapProf);
			}
		}

		// Get the reduce profiles
		NodeList reducers = root.getElementsByTagName(REDUCE_PROFILE);
		for (int i = 0; i < reducers.getLength(); ++i) {
			if (reducers.item(i) instanceof Element) {
				Element reducer = (Element) reducers.item(i);

				// Get the reducer profile attributes
				MRReduceProfile redProf = new MRReduceProfile(reducer
						.getAttribute(ID));
				redProf.setNumTasks(Integer.parseInt(reducer
						.getAttribute(NUM_TASKS)));

				// Get the enum maps
				loadTaskProfileCounters(redProf, reducer);
				loadTaskProfileStatistics(redProf, reducer);
				loadTaskProfileCostFactors(redProf, reducer);
				loadTaskProfileTimings(redProf, reducer);

				jobProfile.addReduceProfile(redProf);
			}
		}

		// Update the profile to calculate the average task profiles
		jobProfile.updateProfile();
		return jobProfile;
	}

	/**
	 * Create an XML DOM representing the job profile and write out to the
	 * provided output file
	 * 
	 * @param jobProfile
	 *            the job profile
	 * @param outFile
	 *            the output file
	 */
	public static void exportJobProfile(MRJobProfile jobProfile, File outFile) {
		try {
			exportJobProfile(jobProfile, new PrintStream(outFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create an XML DOM representing the job profile and write out to the
	 * provided output stream
	 * 
	 * @param jobProfile
	 *            the job profile
	 * @param out
	 *            the output stream
	 */
	public static void exportJobProfile(MRJobProfile jobProfile, PrintStream out) {

		Document doc = null;
		try {
			// Workaround to get the right FactoryBuilder
			System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
					"com.sun.org.apache.xerces."
							+ "internal.jaxp.DocumentBuilderFactoryImpl");
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
					.newDocument();
			doc.setXmlStandalone(true);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}

		// Create the job profile element
		Element job = doc.createElement(JOB_PROFILE);
		doc.appendChild(job);

		// Add the job properties
		job.setAttribute(ID, jobProfile.getJobId());
		job.setAttribute(NUM_MAPPERS, jobProfile.getCounter(
				MRCounter.MAP_TASKS, 0l).toString());
		job.setAttribute(NUM_REDUCERS, jobProfile.getCounter(
				MRCounter.REDUCE_TASKS, 0l).toString());
		if (jobProfile.getClusterName() != null) {
			job.setAttribute(CLUSTER_NAME, jobProfile.getClusterName());
		}

		// Add the job inputs
		Element inputs = doc.createElement(INPUTS);
		job.appendChild(inputs);
		for (String jobInput : jobProfile.getJobInputs()) {
			Element input = doc.createElement(INPUT);
			inputs.appendChild(input);
			input.appendChild(doc.createTextNode(jobInput));
		}

		// Add the map elements
		for (MRMapProfile mapProfile : jobProfile.getAvgMapProfiles()) {
			Element map = buildTaskProfileElement(mapProfile, doc, MAP_PROFILE);
			map.setAttribute(INPUT_INDEX, Integer.toString(mapProfile
					.getInputIndex()));
			job.appendChild(map);
		}

		// Add the reduce element
		MRReduceProfile redProfile = jobProfile.getAvgReduceProfile();
		if (redProfile != null) {
			Element reducer = buildTaskProfileElement(redProfile, doc,
					REDUCE_PROFILE);
			job.appendChild(reducer);
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

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Build an XML element with the enum map information
	 * 
	 * @param map
	 *            the map of enum to number
	 * @param doc
	 *            the XML document
	 * @param parentName
	 *            the name of the parent element e.g., counters
	 * @param childName
	 *            the name of the child elements e.g., counter
	 * @return the XML element
	 */
	private static Element buildEnumMapElement(Map<? extends Enum<?>, ?> map,
			Document doc, String parentName, String childName) {

		Element counters = doc.createElement(parentName);
		for (Entry<? extends Enum<?>, ?> e : map.entrySet()) {
			Element counter = doc.createElement(childName);
			counters.appendChild(counter);

			counter.setAttribute(KEY, e.getKey().name());
			counter.setAttribute(VALUE, e.getValue().toString());
		}

		return counters;
	}

	/**
	 * Build an XML element with the task profile information
	 * 
	 * @param taskProfile
	 *            the task profile
	 * @param doc
	 *            the XML document
	 * @param name
	 *            the name of the element
	 * @return the XML element
	 */
	private static Element buildTaskProfileElement(MRTaskProfile taskProfile,
			Document doc, String name) {

		// Add the task attributes
		Element task = doc.createElement(name);
		task.setAttribute(ID, taskProfile.getTaskId());
		task.setAttribute(NUM_TASKS, Integer
				.toString(taskProfile.getNumTasks()));

		// Add the map enum maps
		task.appendChild(buildEnumMapElement(taskProfile.getCounters(), doc,
				COUNTERS, COUNTER));
		task.appendChild(buildEnumMapElement(taskProfile.getStatistics(), doc,
				STATS, STAT));
		task.appendChild(buildEnumMapElement(taskProfile.getCostFactors(), doc,
				FACTORS, FACTOR));
		task.appendChild(buildEnumMapElement(taskProfile.getTimings(), doc,
				TIMINGS, TIMING));

		return task;
	}

	/**
	 * Load the task profile counters from the XML element to the profile
	 * 
	 * @param taskProf
	 *            the task profile
	 * @param task
	 *            the task XML element
	 */
	private static void loadTaskProfileCounters(MRTaskProfile taskProf,
			Element task) {
		Element counters = (Element) task.getElementsByTagName(COUNTERS)
				.item(0);
		NodeList counterList = counters.getElementsByTagName(COUNTER);
		for (int j = 0; j < counterList.getLength(); ++j) {
			Element counter = (Element) counterList.item(j);
			taskProf.addCounter(MRCounter.valueOf(counter.getAttribute(KEY)),
					Long.parseLong(counter.getAttribute(VALUE)));
		}
	}

	/**
	 * Load the task profile statistics from the XML element to the profile
	 * 
	 * @param taskProf
	 *            the task profile
	 * @param task
	 *            the task XML element
	 */
	private static void loadTaskProfileStatistics(MRTaskProfile taskProf,
			Element task) {
		Element stats = (Element) task.getElementsByTagName(STATS).item(0);
		NodeList statList = stats.getElementsByTagName(STAT);
		for (int j = 0; j < statList.getLength(); ++j) {
			Element stat = (Element) statList.item(j);
			taskProf.addStatistic(MRStatistics.valueOf(stat.getAttribute(KEY)),
					Double.parseDouble(stat.getAttribute(VALUE)));
		}
	}

	/**
	 * Load the task profile cost factor from the XML element to the profile
	 * 
	 * @param taskProf
	 *            the task profile
	 * @param task
	 *            the task XML element
	 */
	private static void loadTaskProfileCostFactors(MRTaskProfile taskProf,
			Element task) {
		Element factors = (Element) task.getElementsByTagName(FACTORS).item(0);
		NodeList factorList = factors.getElementsByTagName(FACTOR);
		for (int j = 0; j < factorList.getLength(); ++j) {
			Element factor = (Element) factorList.item(j);
			taskProf.addCostFactor(MRCostFactors.valueOf(factor
					.getAttribute(KEY)), Double.parseDouble(factor
					.getAttribute(VALUE)));
		}

	}

	/**
	 * Load the task profile timings from the XML element to the profile
	 * 
	 * @param taskProf
	 *            the task profile
	 * @param task
	 *            the task XML element
	 */
	private static void loadTaskProfileTimings(MRTaskProfile taskProf,
			Element task) {
		Element timings = (Element) task.getElementsByTagName(TIMINGS).item(0);
		NodeList timingList = timings.getElementsByTagName(TIMING);
		for (int j = 0; j < timingList.getLength(); ++j) {
			Element timing = (Element) timingList.item(j);
			taskProf.addTiming(MRTaskPhase.valueOf(timing.getAttribute(KEY)),
					Double.parseDouble(timing.getAttribute(VALUE)));
		}

	}

}
