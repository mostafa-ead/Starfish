import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

import com.sun.btrace.AnyType;
import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.Kind;
import com.sun.btrace.annotations.Location;
import com.sun.btrace.annotations.OnMethod;
import com.sun.btrace.annotations.Return;
import com.sun.btrace.annotations.Self;
import com.sun.btrace.annotations.Where;

import edu.duke.starfish.profile.profiler.Profiler;
import edu.duke.starfish.profile.sampling.ProfileSampler;

/**
 * A BTrace script that dynamically collects the execution files upon the
 * completion of a MapReduce job.
 * 
 * In order to collect the files at the end, the job must have submitted the job
 * using the job's method 'waitForCompletion' or via JobControl.
 * 
 * @author hero
 */
@BTrace
public class BTraceJobExecuter {

	/**
	 * Probe for submitting the job
	 */
	@OnMethod(clazz = "org.apache.hadoop.mapred.JobClient", 
			method = "submitJobInternal", 
			location = @Location(value = Kind.ENTRY))
	public static void onJobClient_submitJobInternal_entry(AnyType input) {

		// Load the system properties in the configuration
		Configuration conf = (Configuration) input;
		Profiler.loadProfilingSystemProperties(conf);
	}

	/**
	 * Probe for performing task sampling
	 */
	@OnMethod(clazz = "org.apache.hadoop.mapred.JobClient", 
			method = "writeNewSplits", 
			location = @Location(where = Where.AFTER, value = Kind.CALL, clazz = "/.*/", method = "getSplits"))
	public static void onJobClient_writeNewSplits_getSplits(AnyType job,
			@Return List<InputSplit> splits) {

		JobContext context = (JobContext) job;
		Configuration conf = context.getConfiguration();
		if (conf.get(Profiler.PROFILER_SAMPLING_MODE, "off").equals("tasks")) {
			// Only execute a sample of the map tasks
			ProfileSampler.sampleInputSplits(context, splits);
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
	public static void onJobControl_addToQueue_entry(
			org.apache.hadoop.mapred.jobcontrol.Job job) {

		if (job.getState() == org.apache.hadoop.mapred.jobcontrol.Job.SUCCESS) {
			// Gather the execution files
			Profiler.gatherJobExecutionFiles(job.getJobConf());
		}
	}

}
