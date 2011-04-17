package edu.duke.starfish.profile.profileinfo.metrics;

/**
 * Enumerates the possible metric types
 * 
 * @author hero
 */
public enum MetricType {

	CPU, // CPU utilization
	MEMORY, // Memory utilization
	DISK_READS, // Amount of disk reads
	DISK_WRITES, // Amount of disk writes
	NET_IN, // Amount of incoming network traffic
	NET_OUT, // Amount of outgoing network traffic
}
