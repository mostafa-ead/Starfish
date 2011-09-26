import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.Kind;
import com.sun.btrace.annotations.Location;
import com.sun.btrace.annotations.OnMethod;
import com.sun.btrace.annotations.Self;

import edu.duke.starfish.jobopt.optimizer.JobOptimizer;
import edu.duke.starfish.profile.profiler.Profiler;

/**
 * A BTrace script that dynamically finds the best configuration settings for a
 * MapReduce job.
 * 
 * In order to optimize the job, the job must have submitted the job using the
 * using the job's methods 'submit' or 'waitForCompletion' or via JobControl.
 * 
 * @author hero
 */
@BTrace
public class BTraceJobOptimizer {

	/**
	 * Probe for performing the job optimization right before the job is
	 * submitted to the cluster.
	 */
	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Job", 
			method = "submit", 
			location = @Location(value = Kind.ENTRY))
	public static void onJob_submit_entry(@Self Job job) {
		
		// Set the user-specified configuration options
		Configuration conf = job.getConfiguration();
		JobOptimizer.loadOptimizationSystemProperties(conf);
		
		// Find the optimal configuration
		String mode = System.getProperty(JobOptimizer.JOB_OPT_MODE);
		String profileId = System.getProperty(JobOptimizer.JOB_OPT_PROFILE_ID);

		if (mode.equalsIgnoreCase(JobOptimizer.JOB_OPT_RUN)) {
			// Optimize the job
			if (!JobOptimizer.processJobOptimizationRequest(job, profileId)) {
				System.exit(0);
			}

		} else if (mode.equalsIgnoreCase(JobOptimizer.JOB_OPT_RECOMMEND)) {
			// Recommend a configuration
			JobOptimizer.processJobRecommendationRequest(job, profileId);
			System.exit(0);

		} else {
			System.exit(0);
		}
	}

	/**
	 * Probe for collecting the job execution files after the job completes.
	 * Used when the user submitted the job via waitForCompletion().
	 */
	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Job", 
			method = "waitForCompletion", 
			location = @Location(value = Kind.RETURN))
	public static void onJob_waitForCompletion_return(@Self Job job) {
		
		// Gather the execution files
		Profiler.gatherJobExecutionFiles(job.getConfiguration());
	}
	
	/**
	 * Probe for collecting the job execution files after the job completes.
	 * Used when the user submitted the job via JobControl.
	 */
	@OnMethod(clazz = "org.apache.hadoop.mapred.jobcontrol.JobControl", 
			method = "addToQueue", 
			location = @Location(value = Kind.ENTRY))
	public static void onJobControl_addToQueue_entry(org.apache.hadoop.mapred.jobcontrol.Job job) {

		if (job.getState() == org.apache.hadoop.mapred.jobcontrol.Job.SUCCESS) {
			// Gather the execution files
			Profiler.gatherJobExecutionFiles(job.getJobConf());
		}
	}

}
