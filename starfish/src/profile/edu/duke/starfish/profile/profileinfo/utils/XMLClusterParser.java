package edu.duke.starfish.profile.profileinfo.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

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

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profileinfo.setup.JobTrackerInfo;
import edu.duke.starfish.profile.profileinfo.setup.MasterHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.RackInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * An XML parser for creating and reading cluster XML files. A cluster XML file
 * contains the structure (racks, hosts, trackers) of a virtual cluster.
 * 
 * @author hero
 */
public class XMLClusterParser {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	// Constants - XML tags
	private static final String CLUSTER = "cluster";
	private static final String RACK = "rack";
	private static final String MASTER_HOST = "master_host";
	private static final String SLAVE_HOST = "slave_host";
	private static final String JOB_TRACKER = "job_tracker";
	private static final String TASK_TRACKER = "task_tracker";
	private static final String SPECS = "specs";

	// Constants - XML attributes
	private static final String NAME = "name";
	private static final String IP_ADDRESS = "ip";
	private static final String PORT = "port";
	private static final String MAP_SLOTS = "map_slots";
	private static final String RED_SLOTS = "reduce_slots";
	private static final String MAX_SLOT_MEMORY = "max_slot_memory";

	private static final String NUM_RACKS = "num_racks";
	private static final String HOSTS_PER_RACK = "hosts_per_rack";
	private static final String MAP_SLOTS_PER_HOST = "map_slots_per_host";
	private static final String RED_SLOTS_PER_HOST = "reduce_slots_per_host";

	/* ***************************************************************
	 * PUBLID METHODS
	 * ***************************************************************
	 */

	/**
	 * Load a cluster from the XML file
	 * 
	 * @param inputFile
	 *            the input file to read from
	 * @return the cluster
	 */
	public static ClusterConfiguration importCluster(File inputFile) {
		try {
			return importCluster(new FileInputStream(inputFile));
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
	public static ClusterConfiguration importCluster(InputStream in) {

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
		if (!CLUSTER.equals(root.getTagName()))
			throw new RuntimeException(
					"ERROR: Bad XML File: top-level element not <cluster>");

		// Load the cluster configuration
		ClusterConfiguration cluster = null;
		String name = root.getAttribute(NAME);

		NodeList specs = root.getElementsByTagName(SPECS);
		if (specs.getLength() != 0) {
			// Create the cluster based on the specs
			cluster = loadClusterFromSpecs(name, (Element) specs.item(0));

		} else {
			// Load the racks
			cluster = new ClusterConfiguration();
			cluster.setClusterName(name);
			NodeList racks = root.getElementsByTagName(RACK);
			for (int i = 0; i < racks.getLength(); ++i) {
				if (racks.item(i) instanceof Element) {
					Element rack = (Element) racks.item(i);
					loadRack(cluster, rack);
				}
			}
		}

		return cluster;
	}

	/**
	 * Create an XML DOM representing the cluster and write out to the provided
	 * output file
	 * 
	 * @param cluster
	 *            the cluster
	 * @param outFile
	 *            the output file
	 */
	public static void exportCluster(ClusterConfiguration cluster, File outFile) {
		try {
			exportCluster(cluster, new PrintStream(outFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create an XML DOM representing the cluster and write out to the provided
	 * output stream
	 * 
	 * @param cluster
	 *            the cluster
	 * @param out
	 *            the output stream
	 */
	public static void exportCluster(ClusterConfiguration cluster,
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
		Element clusterElem = doc.createElement(CLUSTER);
		doc.appendChild(clusterElem);

		// Add the cluster name, if any
		if (cluster.getClusterName() != null) {
			clusterElem.setAttribute(NAME, cluster.getClusterName());
		}

		// Add the rack elements
		for (RackInfo rack : cluster.getAllRackInfos()) {
			clusterElem.appendChild(buildRackElement(rack, doc));
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
	 * Builds an XML element with the rack information
	 * 
	 * @param rack
	 *            the rack
	 * @param doc
	 *            the XML document
	 * @return the rack XML element
	 */
	private static Element buildRackElement(RackInfo rack, Document doc) {

		// Add the task attributes
		Element rackElem = doc.createElement(RACK);
		rackElem.setAttribute(NAME, rack.getName());

		// Add the master host
		MasterHostInfo masterHost = (MasterHostInfo) rack.getMasterHost();
		if (masterHost != null) {
			rackElem.appendChild(buildMasterHostElement(masterHost, doc));
		}

		// Add the slave hosts
		for (HostInfo host : rack.getSlaveHosts()) {
			rackElem.appendChild(buildSlaveHostElement((SlaveHostInfo) host,
					doc));
		}

		return rackElem;
	}

	/**
	 * Builds an XML element with the master host information
	 * 
	 * @param host
	 *            the master host
	 * @param doc
	 *            the XML document
	 * @return the host XML element
	 */
	private static Element buildMasterHostElement(MasterHostInfo host,
			Document doc) {

		// Add the task attributes
		Element hostElem = doc.createElement(MASTER_HOST);
		hostElem.setAttribute(NAME, host.getName());
		if (host.getIpAddress() != null)
			hostElem.setAttribute(IP_ADDRESS, host.getIpAddress());

		// Add the job tracker
		JobTrackerInfo jobTracker = host.getJobTracker();
		if (jobTracker != null) {
			Element jobTrackerElem = doc.createElement(JOB_TRACKER);
			jobTrackerElem.setAttribute(NAME, jobTracker.getName());
			jobTrackerElem.setAttribute(PORT, Integer.toString(jobTracker
					.getPort()));
			hostElem.appendChild(jobTrackerElem);
		}

		return hostElem;
	}

	/**
	 * Builds an XML element with the slave host information
	 * 
	 * @param host
	 *            the slave host
	 * @param doc
	 *            the XML document
	 * @return the host XML element
	 */
	private static Element buildSlaveHostElement(SlaveHostInfo host,
			Document doc) {

		// Add the task attributes
		Element hostElem = doc.createElement(SLAVE_HOST);
		hostElem.setAttribute(NAME, host.getName());
		if (host.getIpAddress() != null)
			hostElem.setAttribute(IP_ADDRESS, host.getIpAddress());

		// Add the task tracker
		TaskTrackerInfo taskTracker = host.getTaskTracker();
		if (taskTracker != null) {
			Element taskTrackerElem = doc.createElement(TASK_TRACKER);

			taskTrackerElem.setAttribute(NAME, taskTracker.getName());
			taskTrackerElem.setAttribute(PORT, Integer.toString(taskTracker
					.getPort()));
			taskTrackerElem.setAttribute(MAP_SLOTS, Integer
					.toString(taskTracker.getNumMapSlots()));
			taskTrackerElem.setAttribute(RED_SLOTS, Integer
					.toString(taskTracker.getNumReduceSlots()));
			taskTrackerElem.setAttribute(MAX_SLOT_MEMORY, Long
					.toString(taskTracker.getMaxTaskMemory() >> 20));

			hostElem.appendChild(taskTrackerElem);
		}

		return hostElem;
	}

	/**
	 * Load the cluster specifications from the XML element and create a new
	 * ClusterConfiguration.
	 * 
	 * @param clusterName
	 *            the cluster name
	 * @param specsElem
	 *            the specs XML element
	 * @return the cluster configuration
	 */
	private static ClusterConfiguration loadClusterFromSpecs(
			String clusterName, Element specsElem) {

		// Number of racks is optional
		int numRacks = 1;
		String numRacksStr = specsElem.getAttribute(NUM_RACKS);
		if (numRacksStr != null && !numRacksStr.equals("")) {
			numRacks = Integer.parseInt(numRacksStr);
		}

		// Get and parse the other specs
		int numHostsPerRack = Integer.parseInt(specsElem
				.getAttribute(HOSTS_PER_RACK));
		int numMapSlots = Integer.parseInt(specsElem
				.getAttribute(MAP_SLOTS_PER_HOST));
		int numRedSlots = Integer.parseInt(specsElem
				.getAttribute(RED_SLOTS_PER_HOST));
		long maxSlotMemory = Long.parseLong(specsElem
				.getAttribute(MAX_SLOT_MEMORY)) << 20;

		// Create and return the cluster
		return ClusterConfiguration.createClusterConfiguration(clusterName,
				numRacks, numHostsPerRack, numMapSlots, numRedSlots,
				maxSlotMemory);
	}

	/**
	 * Load the rack info from the XML element to the cluster
	 * 
	 * @param cluster
	 *            the cluster
	 * @param rack
	 *            the rack XML element
	 */
	private static void loadRack(ClusterConfiguration cluster, Element rack) {

		// Get the rack attributes
		RackInfo rackInfo = new RackInfo();
		rackInfo.setName(rack.getAttribute(NAME));

		// Get the master host information
		NodeList master_hosts = rack.getElementsByTagName(MASTER_HOST);
		for (int i = 0; i < master_hosts.getLength(); ++i) {
			if (master_hosts.item(i) instanceof Element) {
				Element host = (Element) master_hosts.item(i);
				loadMasterHost(rackInfo, host);
			}
		}

		// Get the slave host information
		NodeList slave_hosts = rack.getElementsByTagName(SLAVE_HOST);
		for (int i = 0; i < slave_hosts.getLength(); ++i) {
			if (slave_hosts.item(i) instanceof Element) {
				Element host = (Element) slave_hosts.item(i);
				loadSlaveHost(rackInfo, host);
			}
		}

		// Load the racks in the cluster
		cluster.addRackInfo(rackInfo);
	}

	/**
	 * Load the master host info from the XML element into the rack
	 * 
	 * @param rack
	 *            the rack
	 * @param host
	 *            the host XML element
	 */
	private static void loadMasterHost(RackInfo rack, Element host) {

		// Get the master host attributes
		MasterHostInfo hostInfo = new MasterHostInfo();
		hostInfo.setName(host.getAttribute(NAME));
		if (host.getAttribute(IP_ADDRESS) != null
				&& !host.getAttribute(IP_ADDRESS).equals(""))
			hostInfo.setIpAddress(host.getAttribute(IP_ADDRESS));

		// Get the job tracker information
		NodeList trackers = host.getElementsByTagName(JOB_TRACKER);
		for (int i = 0; i < trackers.getLength(); ++i) {
			if (trackers.item(i) instanceof Element) {
				Element tracker = (Element) trackers.item(i);
				loadJobTracker(hostInfo, tracker);
			}
		}

		// Add the host to the rack
		rack.setMasterHost(hostInfo);
	}

	/**
	 * Load the slave host info from the XML element into the rack
	 * 
	 * @param rack
	 *            the rack
	 * @param host
	 *            the host XML element
	 */
	private static void loadSlaveHost(RackInfo rack, Element host) {

		// Get the slave host attributes
		SlaveHostInfo hostInfo = new SlaveHostInfo();
		hostInfo.setName(host.getAttribute(NAME));
		if (host.getAttribute(IP_ADDRESS) != null
				&& !host.getAttribute(IP_ADDRESS).equals(""))
			hostInfo.setIpAddress(host.getAttribute(IP_ADDRESS));

		// Get the task tracker information
		NodeList trackers = host.getElementsByTagName(TASK_TRACKER);
		for (int i = 0; i < trackers.getLength(); ++i) {
			if (trackers.item(i) instanceof Element) {
				Element tracker = (Element) trackers.item(i);
				loadTaskTracker(hostInfo, tracker);
			}
		}

		// Add the host to the rack
		rack.addSlaveHost(hostInfo);
	}

	/**
	 * Load the job tracker info from the XML element into the host
	 * 
	 * @param host
	 *            the master host
	 * @param jobTracker
	 *            the job tracker XML element
	 */
	private static void loadJobTracker(MasterHostInfo host, Element jobTracker) {

		// Get the job tracker attributes
		JobTrackerInfo jobTrackerInfo = new JobTrackerInfo();

		jobTrackerInfo.setName(jobTracker.getAttribute(NAME));
		if (jobTracker.getAttribute(PORT) != null
				&& !jobTracker.getAttribute(PORT).equals(""))
			jobTrackerInfo.setPort(Integer.parseInt(jobTracker
					.getAttribute(PORT)));

		// Add the job tracker to the host
		host.setJobTracker(jobTrackerInfo);
	}

	/**
	 * Load the task tracker info from the XML element into the host
	 * 
	 * @param host
	 *            the master host
	 * @param taskTracker
	 *            the job tracker XML element
	 */
	private static void loadTaskTracker(SlaveHostInfo host, Element taskTracker) {

		// Get the task tracker attributes
		TaskTrackerInfo taskTrackerInfo = new TaskTrackerInfo();
		taskTrackerInfo.setName(taskTracker.getAttribute(NAME));

		String port = taskTracker.getAttribute(PORT);
		if (port != null && !port.equals(""))
			taskTrackerInfo.setPort(Integer.parseInt(port));

		taskTrackerInfo.setNumMapSlots(Integer.parseInt(taskTracker
				.getAttribute(MAP_SLOTS)));
		taskTrackerInfo.setNumReduceSlots(Integer.parseInt(taskTracker
				.getAttribute(RED_SLOTS)));

		String maxMem = taskTracker.getAttribute(MAX_SLOT_MEMORY);
		if (maxMem != null && !maxMem.equals(""))
			taskTrackerInfo.setMaxSlotMemory(Long.parseLong(maxMem) << 20);

		// Add the task tracker to the host
		host.setTaskTracker(taskTrackerInfo);
	}

}
