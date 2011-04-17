package edu.duke.starfish.visualizer.model.timeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRCleanupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRSetupAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRTaskInfo;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;

/**
 * This class forms the Model for the Timeline View. It is responsible for
 * scheduling the tasks for execution in the task trackers. The View will query
 * this Model for the data it needs to populate itself.
 * 
 * @author hero
 */
public class TimelineModel {

	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private MRJobInfo job;
	private Map<String, TimelineTaskTracker> trackersMap;
	private List<TimelineTaskTracker> trackersList;

	/**
	 * @param job
	 */
	public TimelineModel(MRJobInfo job) {
		this.job = job;
		this.trackersMap = new HashMap<String, TimelineTaskTracker>();
		this.trackersList = new ArrayList<TimelineTaskTracker>();

		scheduleJob();
		updateTaskTrackers();
	}

	/* ***************************************************************
	 * PUBLIC METHODS
	 * ***************************************************************
	 */

	/**
	 * @return the task trackers
	 */
	public List<TimelineTaskTracker> getTimelineTaskTrackers() {
		return trackersList;
	}

	/**
	 * @return the number of task trackers
	 */
	public int getNumberTaskTrackers() {
		return trackersList.size();
	}

	/**
	 * Assumes all task trackers have the same number of slots
	 * 
	 * @return the number of task slots per task trackers
	 */
	public int getNumberTaskSlotsPerTaskTracker() {
		return trackersList.get(0).getNumMapSlots()
				+ trackersList.get(0).getNumReduceSlots();
	}

	/* ***************************************************************
	 * PRIVATE METHODS
	 * ***************************************************************
	 */

	/**
	 * Schedule all the task attempts in the task trackers.
	 */
	private void scheduleJob() {

		// Schedule the setup attempts
		List<MRTaskAttemptInfo> setupAttempts = getSortedTaskAttempts(job
				.getSetupTasks());
		for (MRTaskAttemptInfo attempt : setupAttempts) {
			findTimelineTaskTracker(attempt.getTaskTracker())
					.scheduleSetupTask((MRSetupAttemptInfo) attempt);
		}

		// Schedule the map attempts
		List<MRTaskAttemptInfo> mapAttempts = getSortedTaskAttempts(job
				.getMapTasks());
		for (MRTaskAttemptInfo attempt : mapAttempts) {
			findTimelineTaskTracker(attempt.getTaskTracker()).scheduleMapTask(
					(MRMapAttemptInfo) attempt);
		}

		// Schedule the reduce attempts
		List<MRTaskAttemptInfo> redAttempts = getSortedTaskAttempts(job
				.getReduceTasks());
		for (MRTaskAttemptInfo attempt : redAttempts) {
			findTimelineTaskTracker(attempt.getTaskTracker())
					.scheduleReduceTask((MRReduceAttemptInfo) attempt);
		}

		// Schedule the cleanup attempts
		List<MRTaskAttemptInfo> cleanupAttempts = getSortedTaskAttempts(job
				.getCleanupTasks());
		for (MRTaskAttemptInfo attempt : cleanupAttempts) {
			findTimelineTaskTracker(attempt.getTaskTracker())
					.scheduleCleanupTask((MRCleanupAttemptInfo) attempt);
		}

	}

	/**
	 * Update the task tracker ids to match their sort order
	 */
	private void updateTaskTrackers() {

		// Sort the trackers on host name
		trackersList.addAll(trackersMap.values());
		Collections.sort(trackersList, new Comparator<TimelineTaskTracker>() {
			@Override
			public int compare(TimelineTaskTracker o1, TimelineTaskTracker o2) {
				return o1.getHostName().compareTo(o2.getHostName());
			}
		});

		// Update the tracker ids to match the sort order
		int hostId = 0;
		for (TimelineTaskTracker tracker : trackersList) {
			tracker.setHostId(hostId);
			++hostId;
		}
	}

	/**
	 * Find the timeline task tracker based on the name of the input tracker. If
	 * it doesn't exist, it will be created
	 * 
	 * @param tracker
	 *            the task tracker
	 * @return the timeline task tracker
	 */
	private TimelineTaskTracker findTimelineTaskTracker(TaskTrackerInfo tracker) {

		String hostName = tracker.getHostName();
		if (!trackersMap.containsKey(hostName)) {
			trackersMap.put(hostName, new TimelineTaskTracker(hostName, 0,
					tracker.getNumMapSlots(), tracker.getNumReduceSlots()));
		}

		return trackersMap.get(hostName);
	}

	/**
	 * Sorts the task attempts based on start time
	 * 
	 * @param attempts
	 *            the attempts
	 */
	private List<MRTaskAttemptInfo> getSortedTaskAttempts(
			List<? extends MRTaskInfo> tasks) {

		List<MRTaskAttemptInfo> attempts = new ArrayList<MRTaskAttemptInfo>();
		for (MRTaskInfo task : tasks) {
			attempts.addAll(task.getAttempts());
		}

		Collections.sort(attempts, new Comparator<MRTaskAttemptInfo>() {
			@Override
			public int compare(MRTaskAttemptInfo arg0, MRTaskAttemptInfo arg1) {
				return arg0.getStartTime().compareTo(arg1.getStartTime());
			}
		});

		return attempts;
	}
}
