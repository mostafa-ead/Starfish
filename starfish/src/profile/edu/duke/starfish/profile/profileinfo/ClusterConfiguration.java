package edu.duke.starfish.profile.profileinfo;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobTracker;

import edu.duke.starfish.profile.profileinfo.setup.HostInfo;
import edu.duke.starfish.profile.profileinfo.setup.JobTrackerInfo;
import edu.duke.starfish.profile.profileinfo.setup.MasterHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.RackInfo;
import edu.duke.starfish.profile.profileinfo.setup.SlaveHostInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;
import edu.duke.starfish.profile.profiler.Profiler;

/**
 * Represents the entire cluster configuration i.e. the racks, the hosts, the
 * job tracker, and the task trackers.
 * 
 * This class contains methods to generate a cluster programmatically, or to
 * build it based on a live Hadoop cluster.
 * 
 * You can also build a cluster given a high-level description:
 * {@link ClusterConfiguration#createClusterConfiguration(String, int, int, int, int, long)}
 * 
 * @author hero
 * 
 */
public class ClusterConfiguration {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private String name; // The cluster name

	// The racks contain all the hosts and trackers
	private Map<String, RackInfo> racks; // the racks

	// Caching
	private MasterHostInfo masterHost; // the master host
	private Map<String, SlaveHostInfo> slaveHosts; // the slave hosts
	private JobTrackerInfo jobTracker; // the job tracker
	private Map<String, TaskTrackerInfo> taskTrackers; // the task trackers

	// Constants
	private static final String MASTER_RACK = "master-rack";
	private static final String DEFAULT_RACK = "default-rack";
	private static final String JOB_TRACKER = "job_tracker_";
	private static final String TRACKER = "tracker_";
	private static final String LOCALHOST = "localhost";
	private static final String TAB = "\t";

	/**
	 * Default constructor
	 */
	public ClusterConfiguration() {
		this.racks = new HashMap<String, RackInfo>();
		this.masterHost = null;
		this.slaveHosts = new HashMap<String, SlaveHostInfo>();
		this.jobTracker = null;
		this.taskTrackers = new HashMap<String, TaskTrackerInfo>();
		this.name = null;
	}

	/**
	 * Builds the cluster configuration of the current hadoop cluster
	 * 
	 * @param conf
	 *            the hadoop configuration
	 */
	public ClusterConfiguration(Configuration conf) {
		this();

		// Create the job tracker information
		InetSocketAddress jobTrackerAddr = JobTracker.getAddress(conf);
		RackInfo masterRack = new RackInfo(0, MASTER_RACK);
		racks.put(masterRack.getName(), masterRack);
		masterHost = new MasterHostInfo(0, jobTrackerAddr.getHostName(),
				jobTrackerAddr.getAddress().getHostAddress(), masterRack
						.getName());
		jobTracker = new JobTrackerInfo(0, JOB_TRACKER + masterHost.getName(),
				masterHost.getName(), jobTrackerAddr.getPort());
		masterRack.setMasterHost(masterHost);
		masterHost.setJobTracker(jobTracker);

		// Set the cluster
		name = conf.get(Profiler.PROFILER_CLUSTER_NAME);
		if (name == null) {
			name = masterHost.getName();
		}

		// Get the cluster information
		JobClient client = null;
		ClusterStatus cluster = null;
		try {
			client = new JobClient(jobTrackerAddr, conf);
			cluster = client.getClusterStatus(true);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		// Calculate the map and reduce slots
		int mapSlots = cluster.getMaxMapTasks() / cluster.getTaskTrackers();
		int redSlots = cluster.getMaxReduceTasks() / cluster.getTaskTrackers();
		long taskMem = ProfileUtils.getTaskMemory(conf);

		// Create the task trackers information
		int id = 1;
		for (String tracker : cluster.getActiveTrackerNames()) {

			// Parse the tracker information (e.g.,
			// tracker_hero-ubuntu:localhost/127.0.0.1:53100)
			String[] trackerPieces = tracker.split(":");
			if (trackerPieces.length != 3) {
				throw new RuntimeException("ERROR: The tracker name should be "
						+ "of the form 'tracker_name:host_name:port' and not "
						+ tracker);
			}

			// Parse the host information
			String hostInfo = trackerPieces[1];
			String rackName, hostName, hostIpAddr;

			String[] hostPieces = hostInfo.split("/");
			if (hostPieces.length == 3) {
				rackName = hostPieces[0];
				hostName = hostPieces[1];
				hostIpAddr = hostPieces[2];
			} else if (hostPieces.length == 2) {
				rackName = DEFAULT_RACK;
				hostName = hostPieces[0];
				hostIpAddr = hostPieces[1];
			} else {
				throw new RuntimeException("ERROR: The host name should be of "
						+ "the form '[rack_name/]host_name/ip_addr' and not "
						+ hostInfo);
			}

			// If the host is specified as 'localhost', we'll find the real name
			// in the tracker name
			String trackerName = trackerPieces[0];
			if (hostName.contains(LOCALHOST) && trackerName.startsWith(TRACKER)) {
				hostName = trackerName.substring(8);
			}

			// Create the rack, host, and task tracker
			RackInfo rack = addFindRackInfo(rackName);
			SlaveHostInfo host = new SlaveHostInfo(id, hostName, hostIpAddr,
					rack.getName());
			TaskTrackerInfo taskTracker = new TaskTrackerInfo(id, trackerName,
					host.getName(), Integer.parseInt(trackerPieces[2]),
					mapSlots, redSlots, taskMem);
			slaveHosts.put(host.getName(), host);
			taskTrackers.put(taskTracker.getName(), taskTracker);
			rack.addSlaveHost(host);
			host.setTaskTracker(taskTracker);

			++id;
		}
	}

	/**
	 * Copy constructor
	 * 
	 * @param other
	 */
	public ClusterConfiguration(ClusterConfiguration other) {
		this();

		this.name = other.name;
		this.racks = new HashMap<String, RackInfo>(other.racks.size());

		// The rack copy constructor copies all hosts (and trackers)
		// addRackInfo caches all hosts and trackers
		for (RackInfo rack : other.racks.values())
			addRackInfo(new RackInfo(rack));
	}

	/* ***************************************************************
	 * OVERRIDEN METHODS
	 * *************************************************************
	 */

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + ((racks == null) ? 0 : racks.hashCode());
		result = 37 * result
				+ ((masterHost == null) ? 0 : masterHost.hashCode());
		result = 41 * result
				+ ((slaveHosts == null) ? 0 : slaveHosts.hashCode());
		result = 43 * result
				+ ((jobTracker == null) ? 0 : jobTracker.hashCode());
		result = 47 * result
				+ ((taskTrackers == null) ? 0 : taskTrackers.hashCode());
		return result;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ClusterConfiguration))
			return false;
		ClusterConfiguration other = (ClusterConfiguration) obj;
		if (slaveHosts == null) {
			if (other.slaveHosts != null)
				return false;
		} else if (!slaveHosts.equals(other.slaveHosts))
			return false;
		if (masterHost == null) {
			if (other.masterHost != null)
				return false;
		} else if (!masterHost.equals(other.masterHost))
			return false;
		if (jobTracker == null) {
			if (other.jobTracker != null)
				return false;
		} else if (!jobTracker.equals(other.jobTracker))
			return false;
		if (racks == null) {
			if (other.racks != null)
				return false;
		} else if (!racks.equals(other.racks))
			return false;
		if (taskTrackers == null) {
			if (other.taskTrackers != null)
				return false;
		} else if (!taskTrackers.equals(other.taskTrackers))
			return false;
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ClusterConfiguration [Name=" + name + " Racks="
				+ getAllRackInfos() + ", jobTracker=" + getJobTrackerInfo()
				+ ", taskTrackers=" + getAllTaskTrackersInfos() + "]";
	}

	/* ***************************************************************
	 * Public Methods
	 * *************************************************************
	 */

	/**
	 * Add the rack into the cluster. Also ensures that all the hosts in this
	 * rack are also in the cluster.
	 * 
	 * @param rack
	 *            the rack to add
	 */
	public void addRackInfo(RackInfo rack) {
		racks.put(rack.getName(), rack);

		// Cache the hosts (and trackers)
		for (SlaveHostInfo host : rack.getSlaveHosts()) {
			slaveHosts.put(host.getName(), host);

			TaskTrackerInfo taskTracker = host.getTaskTracker();
			if (taskTracker != null)
				taskTrackers.put(taskTracker.getName(), taskTracker);

		}

		if (rack.getMasterHost() != null) {
			masterHost = rack.getMasterHost();
			if (masterHost.getJobTracker() != null)
				jobTracker = masterHost.getJobTracker();
		}
	}

	/**
	 * Add a master host into the cluster. The rack must already exist.
	 * 
	 * @param host
	 *            the host to add
	 */
	public void addMasterHostInfo(MasterHostInfo host) {
		RackInfo rack = racks.get(host.getRackName());
		rack.setMasterHost(host);

		// Cache the master host and job tracker
		masterHost = (MasterHostInfo) host;
		if (masterHost.getJobTracker() != null)
			jobTracker = masterHost.getJobTracker();
	}

	/**
	 * Add a slave host into the cluster. The rack must already exist.
	 * 
	 * @param host
	 *            the host to add
	 */
	public void addSlaveHostInfo(SlaveHostInfo host) {
		RackInfo rack = racks.get(host.getRackName());
		rack.addSlaveHost(host);

		// Cache the slave host and task tracker
		slaveHosts.put(host.getName(), host);
		TaskTrackerInfo taskTracker = host.getTaskTracker();
		if (taskTracker != null)
			taskTrackers.put(taskTracker.getName(), taskTracker);
	}

	/**
	 * Add the job tracker into the cluster. The master host must already exist.
	 * 
	 * @param tracker
	 *            the job tracker to add
	 */
	public void addJobTrackerInfo(JobTrackerInfo tracker) {
		// Set in the master host and cache
		this.masterHost.setJobTracker(tracker);
		this.jobTracker = tracker;
	}

	/**
	 * Add the task tracker into the cluster. The slave host must already exist.
	 * 
	 * @param taskTracker
	 *            the task tracker to add
	 */
	public void addTaskTrackerInfo(TaskTrackerInfo taskTracker) {
		// Set in the slave host and cache
		SlaveHostInfo slave = slaveHosts.get(taskTracker.getHostName());
		slave.setTaskTracker(taskTracker);

		taskTrackers.put(taskTracker.getName(), taskTracker);
	}

	/**
	 * If the rack is already in the cluster, it will simply return it.
	 * Otherwise, it will create a new rack in the cluster and return it.
	 * 
	 * @param rackName
	 *            the rack name of the rack to add
	 * @return the rack
	 */
	public RackInfo addFindRackInfo(String rackName) {
		if (racks.containsKey(rackName)) {
			// The rack is already in the cluster
			return racks.get(rackName);
		} else {
			// Create a new rack
			RackInfo rack = new RackInfo();
			rack.setName(rackName);
			racks.put(rackName, rack);
			return rack;
		}
	}

	/**
	 * If the host is already in the cluster, it will simply return it.
	 * Otherwise, it will create a new host in the cluster and return it. If
	 * necessary, it will also create the rack of this host.
	 * 
	 * The full host name is of the form: /rackName/hostName. Example:
	 * /default-rack/ip-10-196-215-0.ec2.internal
	 * 
	 * @param fullHostName
	 *            the full host name of the host to add
	 * @return the (new) host
	 */
	public MasterHostInfo addFindMasterHostInfo(String fullHostName) {

		// Parse the rack and host name
		String rackName = null;
		String hostName = null;
		String[] pieces = fullHostName.split("/");
		if (pieces.length == 3) {
			rackName = pieces[1];
			hostName = pieces[2];
		} else {
			// Last attempt to find the host
			if (masterHost != null && fullHostName.equals(masterHost.getName())) {
				rackName = masterHost.getRackName();
				hostName = masterHost.getName();
			} else {
				return null;
			}
		}

		MasterHostInfo host = null;
		RackInfo rack = addFindRackInfo(rackName);
		if (rack.getMasterHost() != null
				&& hostName.equals(rack.getMasterHost().getName())) {
			// The master host is already in the cluster
			host = rack.getMasterHost();

		} else {
			// Create a new master host
			host = new MasterHostInfo();
			host.setName(hostName);
			host.setRackName(rackName);
		}

		addMasterHostInfo(host);
		return host;
	}

	/**
	 * If the host is already in the cluster, it will simply return it.
	 * Otherwise, it will create a new host in the cluster and return it. If
	 * necessary, it will also create the rack of this host.
	 * 
	 * The full host name is of the form: /rackName/hostName. Example:
	 * /default-rack/ip-10-196-215-0.ec2.internal
	 * 
	 * @param fullHostName
	 *            the full host name of the host to add
	 * @return the (new) host
	 */
	public SlaveHostInfo addFindSlaveHostInfo(String fullHostName) {

		// Parse the rack and host name
		String rackName = null;
		String hostName = null;
		String[] pieces = fullHostName.split("/");
		if (pieces.length == 3) {
			rackName = pieces[1];
			hostName = pieces[2];
		} else {
			// Last attempt to find the host
			if (slaveHosts.containsKey(fullHostName)
					&& fullHostName.equals(slaveHosts.get(fullHostName)
							.getName())) {
				rackName = slaveHosts.get(fullHostName).getRackName();
				hostName = slaveHosts.get(fullHostName).getName();
			} else {
				return null;
			}
		}

		SlaveHostInfo host = null;
		RackInfo rack = addFindRackInfo(rackName);
		if (rack.getSlaveHost(hostName) != null) {
			// The slave host is already in the cluster
			host = rack.getSlaveHost(hostName);

		} else {
			// Create a new slave host
			host = new SlaveHostInfo();
			host.setName(hostName);
			host.setRackName(rackName);
		}

		addSlaveHostInfo(host);
		return host;
	}

	/**
	 * If the job tracker is already in the cluster, it will simply return it.
	 * Otherwise, it will create a new job tracker in the cluster and return it.
	 * If necessary, it will create and add the host and rack for this job
	 * tracker.
	 * 
	 * The full host name is of the form: /rackName/hostName. Example:
	 * /default-rack/ip-10-196-215-0.ec2.internal
	 * 
	 * @param trackerName
	 *            the job tracker to add
	 * @param fullHostName
	 *            full host name
	 * @return the (new) job tracker
	 */
	public JobTrackerInfo addFindJobTrackerInfo(String trackerName,
			String fullHostName) {

		JobTrackerInfo tracker = null;
		MasterHostInfo masterHost = addFindMasterHostInfo(fullHostName);
		if (masterHost == null)
			return null;
		if (masterHost.getJobTracker() != null
				&& trackerName.equals(masterHost.getJobTracker().getName())) {
			// The job tracker exists
			tracker = masterHost.getJobTracker();

		} else {
			// Create a new job tracker
			tracker = new JobTrackerInfo();
			tracker.setName(trackerName);
			tracker.setHostName(masterHost.getName());
		}

		addJobTrackerInfo(tracker);
		return tracker;
	}

	/**
	 * If the task tracker is already in the cluster, it will simply return it.
	 * Otherwise, it will create a new task tracker in the cluster and return
	 * it. If necessary, it will create and add the host and rack for this task
	 * tracker.
	 * 
	 * The full host name is of the form: /rackName/hostName. Example:
	 * /default-rack/ip-10-196-215-0.ec2.internal
	 * 
	 * @param trackerName
	 *            the name of the tracker
	 * @param fullHostName
	 *            the full host name (includes rack name)
	 * @return the (new) task tracker
	 */
	public TaskTrackerInfo addFindTaskTrackerInfo(String trackerName,
			String fullHostName) {

		TaskTrackerInfo tracker = null;
		SlaveHostInfo host = addFindSlaveHostInfo(fullHostName);
		if (host == null)
			return null;
		if (host.getTaskTracker() != null
				&& trackerName.equals(host.getTaskTracker().getName())) {
			// The task tracker exists
			tracker = host.getTaskTracker();

		} else {
			// Create a new task tracker
			tracker = new TaskTrackerInfo();
			tracker.setName(trackerName);
			tracker.setHostName(host.getName());
		}

		addTaskTrackerInfo(tracker);
		return tracker;
	}

	/**
	 * @return all the racks in the cluster
	 */
	public Collection<RackInfo> getAllRackInfos() {
		return racks.values();
	}

	/**
	 * 
	 * @return all the hosts in the cluster
	 */
	public Collection<SlaveHostInfo> getAllSlaveHostInfos() {
		return slaveHosts.values();
	}

	/**
	 * 
	 * @return all the task trackers in the cluster
	 */
	public Collection<TaskTrackerInfo> getAllTaskTrackersInfos() {
		return taskTrackers.values();
	}

	/**
	 * Get the rack info with the specified name
	 * 
	 * @param name
	 *            the name of the rack
	 * @return the rack info or null
	 */
	public RackInfo getRackInfo(String name) {
		return racks.get(name);
	}

	/**
	 * Get the master host info
	 * 
	 * @return the master host info
	 */
	public MasterHostInfo getMasterHostInfo() {
		return masterHost;
	}

	/**
	 * Get the slave host info with the specified name
	 * 
	 * @param name
	 *            the name of the host
	 * @return the host info or null
	 */
	public HostInfo getSlaveHostInfo(String name) {
		return slaveHosts.get(name);
	}

	/**
	 * Get the job tracker info
	 * 
	 * @return the job tracker info or null
	 */
	public JobTrackerInfo getJobTrackerInfo() {
		return jobTracker;
	}

	/**
	 * Get the task tracker info with the specified name
	 * 
	 * @param name
	 *            the name of the task tracker
	 * @return the task tracker info or null
	 */
	public TaskTrackerInfo getTaskTrackerInfo(String name) {
		return taskTrackers.get(name);
	}

	/**
	 * Get the cluster's name (could be null)
	 * 
	 * @return the cluster's name
	 */
	public String getClusterName() {
		return name;
	}

	/**
	 * Get the total number of hosts in the cluster, including the master host
	 * 
	 * @return the number of hosts
	 */
	public int getNumberOfHosts() {
		return taskTrackers.size() + (masterHost != null ? 1 : 0);
	}

	/**
	 * Get the average number of map slots per host
	 * 
	 * @return the average number of map slots per host
	 */
	public int getAvgMapSlotsPerHost() {
		return Math.round(getTotalMapSlots() / (float) taskTrackers.size());
	}

	/**
	 * Get the average number of reduce slots per host
	 * 
	 * @return the average number of reduce slots per host
	 */
	public int getAvgReduceSlotsPerHost() {
		return Math.round(getTotalReduceSlots() / (float) taskTrackers.size());
	}

	/**
	 * Get the total number of map slots available in the cluster
	 * 
	 * @return the total number of map slots
	 */
	public int getTotalMapSlots() {
		int numSlots = 0;
		for (TaskTrackerInfo taskTracker : taskTrackers.values()) {
			numSlots += taskTracker.getNumMapSlots();
		}
		return numSlots;
	}

	/**
	 * Get the total number of reduce slots available in the cluster
	 * 
	 * @return the total number of reduce slots
	 */
	public int getTotalReduceSlots() {
		int numSlots = 0;
		for (TaskTrackerInfo taskTracker : taskTrackers.values()) {
			numSlots += taskTracker.getNumReduceSlots();
		}
		return numSlots;
	}

	/**
	 * Returns the (average) maximum memory available (in bytes) for the
	 * execution of a single task
	 * 
	 * @return the maximum task memory in bytes
	 */
	public long getMaxTaskMemory() {
		long maxMem = 0;
		for (TaskTrackerInfo taskTracker : taskTrackers.values()) {
			maxMem += taskTracker.getMaxTaskMemory();
		}

		return Math.round(maxMem / (double) taskTrackers.size());
	}

	/**
	 * Set the cluster's name
	 * 
	 * @param name
	 *            the cluster's name
	 */
	public void setClusterName(String name) {
		this.name = name;
	}

	/**
	 * Prints out the information about the cluster, including information for
	 * the racks and the hosts.
	 * 
	 * @param out
	 *            The print stream to print at
	 */
	public void printClusterConfiguration(PrintStream out) {

		// Print out the cluster name, if any
		if (name != null) {
			out.println("Cluster: " + name);
			out.println();
		}

		// Print out information for the racks and hosts
		for (RackInfo rack : racks.values()) {
			out.println("Rack: " + rack.getName());
			if (rack.getMasterHost() != null) {
				out.println("\tMaster Host: " + rack.getMasterHost().getName());
			}

			for (SlaveHostInfo host : rack.getSlaveHosts()) {
				out.println("\tSlave Host: " + host.getName());
			}
		}

		// Print out information about the job tracker
		out.println();
		out.println("Job Trackers:");
		out.println("Name\tHost\tPort");
		if (jobTracker != null) {
			out.print(jobTracker.getName());
			out.print(TAB);
			out.print(jobTracker.getHostName());
			out.print(TAB);
			out.println(jobTracker.getPort());
		}

		// Print out information about the task trackers
		out.println();
		out.println("Task Trackers:");
		out.println("Name\tHost\tPort\tMap_Slots\tReduce_Slots");
		for (TaskTrackerInfo taskTracker : taskTrackers.values()) {
			out.print(taskTracker.getName());
			out.print(TAB);
			out.print(taskTracker.getHostName());
			out.print(TAB);
			out.print(taskTracker.getPort());
			out.print(TAB);
			out.print(taskTracker.getNumMapSlots());
			out.print(TAB);
			out.println(taskTracker.getNumReduceSlots());
		}
	}

	/* ***************************************************************
	 * PUBLIC STATIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Creates and returns a virtual cluster configuration. The cluster will
	 * contain 'numRacks' racks, each with 'numHostsPerRack' slave hosts. One
	 * task tracker with the specified numMapSlots and numReduceSlots will be
	 * created for each slave host. A master host will be included in the first
	 * rack, which will also host the job tracker.
	 * 
	 * @param name
	 *            the cluster name
	 * @param numRacks
	 *            the number of racks of the cluster
	 * @param numHostsPerRack
	 *            the number of hosts per rack
	 * @param numMapSlots
	 *            the number of map reduce slots per task
	 * @param numReduceSlots
	 *            the number of reduce reduce slots per task
	 * @param maxTaskMemory
	 *            the max memory per task in bytes
	 * @return a cluster configuration
	 */
	public static ClusterConfiguration createClusterConfiguration(String name,
			int numRacks, int numHostsPerRack, int numMapSlots,
			int numReduceSlots, long maxTaskMemory) {

		// Set properties for number format
		NumberFormat nf = NumberFormat.getIntegerInstance();
		nf.setMinimumIntegerDigits(3);

		// Create the cluster configuration
		ClusterConfiguration cluster = new ClusterConfiguration();
		cluster.setClusterName(name);

		for (int rackId = 1; rackId <= numRacks; ++rackId) {
			// Create the rack and add it to the cluster
			RackInfo rack = new RackInfo(rackId, "rack_" + nf.format(rackId));
			cluster.addRackInfo(rack);

			// Create the master host and job tracker on the first rack
			if (rackId == 1) {
				MasterHostInfo host = new MasterHostInfo(0, "master_host", "",
						rack.getName());
				JobTrackerInfo tracker = new JobTrackerInfo(0, "job_tracker_"
						+ host.getName(), host.getName(), 50060);

				// Add them to the cluster
				cluster.addMasterHostInfo(host);
				cluster.addJobTrackerInfo(tracker);
			}

			// Generate the hosts for each rack
			for (int hostId = 1; hostId <= numHostsPerRack; ++hostId) {

				int id = (rackId - 1) * numHostsPerRack + hostId;

				// Create one host and task tracker
				SlaveHostInfo host = new SlaveHostInfo(id, "rack_"
						+ nf.format(rackId) + "_host_" + nf.format(hostId), "",
						rack.getName());
				TaskTrackerInfo tracker = new TaskTrackerInfo(id,
						"task_tracker_" + host.getName(), host.getName(),
						50060, numMapSlots, numReduceSlots, maxTaskMemory);

				// Add them to the cluster
				cluster.addSlaveHostInfo(host);
				cluster.addTaskTrackerInfo(tracker);
			}
		}

		return cluster;
	}

}
