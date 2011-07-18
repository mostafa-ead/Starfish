package edu.duke.starfish.profile.utils;

import java.io.PrintStream;
import java.util.Date;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;

/**
 * Constructs and prints out a timeline with the task execution of a job. The
 * {@link TimelineCalc#printTimeline(PrintStream)} produces tabular data of the
 * form "Time\tMaps\tShuffle\tMerge\tReduce\tWaste"
 * 
 * @author hero
 */
public class TimelineCalc {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private int[] mappers;
	private int[] shuffling;
	private int[] sorting;
	private int[] reducing;
	private int[] waste;

	private long grandStart;
	private int grandDuration;

	/**
	 * Constructor
	 * 
	 * @param start
	 *            start time of timeline
	 * @param end
	 *            end time of timeline
	 */
	public TimelineCalc(Date start, Date end) {

		// Initializations
		grandStart = start.getTime() / 1000;
		grandDuration = (int) Math
				.ceil((end.getTime() - start.getTime()) / 1000d);

		// Create and initialize the counter arrays
		mappers = new int[grandDuration];
		shuffling = new int[grandDuration];
		sorting = new int[grandDuration];
		reducing = new int[grandDuration];
		waste = new int[grandDuration];

		for (int i = 0; i < grandDuration; ++i) {
			mappers[i] = shuffling[i] = sorting[i] = reducing[i] = waste[i] = 0;
		}
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * Add a job in the timeline, that is, include the execution of the job's
	 * tasks in the timeline.
	 * 
	 * @param mrJob
	 *            the MR job
	 */
	public void addJob(MRJobInfo mrJob) {

		// Error checking
		if (mrJob.getStartTime().getTime() / 1000 < grandStart)
			throw new RuntimeException(
					"The job's start time cannot be before the grand start time");
		if (mrJob.getEndTime().getTime() / 1000 > grandStart + grandDuration)
			throw new RuntimeException(
					"The job's end time cannot be after the grand end time");

		boolean success = true;
		int start = 0;
		int shuffle = 0;
		int sort = 0;
		int end = 0;
		int time = 0;

		// Count the map tasks
		for (MRMapInfo mrMap : mrJob.getMapTasks()) {
			for (MRMapAttemptInfo mrMapAttempt : mrMap.getAttempts()) {
				success = mrMapAttempt.getStatus() == MRExecutionStatus.SUCCESS;
				start = (int) (mrMapAttempt.getStartTime().getTime() / 1000 - grandStart);
				end = (int) (mrMapAttempt.getEndTime().getTime() / 1000 - grandStart);
				if (success) {
					for (time = start; time < end; ++time)
						++mappers[time];
				} else {
					for (time = start; time < end; ++time)
						++waste[time];
				}
			}
		}

		// Count the reduce tasks
		for (MRReduceInfo mrRed : mrJob.getReduceTasks()) {
			for (MRReduceAttemptInfo mrRedAttempt : mrRed.getAttempts()) {
				success = mrRedAttempt.getStatus() == MRExecutionStatus.SUCCESS;
				start = (int) (mrRedAttempt.getStartTime().getTime() / 1000 - grandStart);
				end = (int) (mrRedAttempt.getEndTime().getTime() / 1000 - grandStart);

				if (success) {
					shuffle = (int) (mrRedAttempt.getShuffleEndTime().getTime() / 1000 - grandStart);
					sort = (int) (mrRedAttempt.getSortEndTime().getTime() / 1000 - grandStart);

					for (time = start; time < shuffle; ++time)
						++shuffling[time];
					for (time = shuffle; time < sort; ++time)
						++sorting[time];
					for (time = sort; time < end; ++time)
						++reducing[time];
				} else {
					for (time = start; time < end; ++time)
						++waste[time];
				}
			}
		}

	}

	/**
	 * Print out the timeline as tabular data of the form
	 * "Time\tMaps\tShuffle\tMerge\tReduce\tWaste"
	 * 
	 * @param ps
	 *            the print stream to write to
	 */
	public void printTimeline(PrintStream ps) {

		// Print out the timeline
		StringBuffer sb = new StringBuffer();
		ps.println("Time\tMaps\tShuffle\tMerge\tReduce\tWaste");
		for (int t = 0; t < grandDuration; ++t) {
			sb.append(t);
			sb.append("\t");
			sb.append(mappers[t]);
			sb.append("\t");
			sb.append(shuffling[t]);
			sb.append("\t");
			sb.append(sorting[t]);
			sb.append("\t");
			sb.append(reducing[t]);
			sb.append("\t");
			sb.append(waste[t]);

			ps.println(sb.toString());
			sb.delete(0, sb.length());
		}
	}
}
