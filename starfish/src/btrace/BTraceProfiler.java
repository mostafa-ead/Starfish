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

import edu.duke.starfish.profile.profileinfo.utils.Constants;
import edu.duke.starfish.profile.profiler.Profiler;
import edu.duke.starfish.profile.sampling.ProfileSampler;

/**
 * A BTrace script that dynamically enables profiling for a MapReduce job.
 * 
 * In order to enable profiling, the job must implement the new API.
 * 
 * In order to compute and profile at the end, the job must have submitted the
 * job using the job's method 'waitForCompletion'.
 * 
 * @author hero
 */
@BTrace
public class BTraceProfiler {
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.JobClient", 
			method = "submitJobInternal", 
			location = @Location(value = Kind.ENTRY))
	public static void onJobClient_submitJobInternal_entry(AnyType input) {

		// Set the system property as a configuration setting
		Configuration conf = (Configuration) input;
		if (conf.get(Profiler.BTRACE_PROFILE_DIR) == null)
			conf.set(Profiler.BTRACE_PROFILE_DIR, 
					System.getProperty(Profiler.BTRACE_PROFILE_DIR));

		// The cluster name
		if (conf.get(Profiler.PROFILER_CLUSTER_NAME) == null)
			conf.set(Profiler.PROFILER_CLUSTER_NAME, 
					System.getProperty(Profiler.PROFILER_CLUSTER_NAME));

		// The sampling mode (off, profiles, or tasks)
		if (conf.get(Profiler.PROFILER_SAMPLING_MODE) == null)
			conf.set(Profiler.PROFILER_SAMPLING_MODE,
					System.getProperty(Profiler.PROFILER_SAMPLING_MODE));

		// The sampling fraction
		if (conf.get(Profiler.PROFILER_SAMPLING_FRACTION) == null)
			conf.set(Profiler.PROFILER_SAMPLING_FRACTION,
					System.getProperty(Profiler.PROFILER_SAMPLING_FRACTION));
		
		// Enable profiling
		if (Profiler.enableExecutionProfiling(conf)) {
			
			// Profile only a fraction of the tasks if requested
			if (conf.get(Profiler.PROFILER_SAMPLING_MODE, "off").equals("profiles")) {
				
				if (!ProfileSampler.sampleTasksToProfile(conf)) {
					// Disable profiling
					conf.setBoolean(Constants.MR_TASK_PROFILE, false);
				}
			}
		}
	}

	
	@OnMethod(clazz = "org.apache.hadoop.mapred.JobClient", 
			method = "writeNewSplits", 
		    location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="getSplits"))
	public static void onJobClinet_writeNewSplits_getSplits(AnyType job, @Return List<InputSplit> splits) {

		JobContext context = (JobContext) job;
		Configuration conf = context.getConfiguration();
		if (conf.getBoolean(Constants.MR_TASK_PROFILE, false) &&
				conf.get(Profiler.PROFILER_SAMPLING_MODE, "off").equals("tasks")) {
			// Only execute a sample of the map tasks
			ProfileSampler.sampleInputSplits(context, splits);
		}
	}


	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Job", 
			method = "waitForCompletion", 
			location = @Location(value = Kind.RETURN))
	public static void onJob_waitForCompletion_return(@Self Job job) {

		// Get the output directory
		Configuration conf = job.getConfiguration();
		String outputDir = conf.get(Profiler.PROFILER_OUTPUT_DIR);
		if (outputDir == null)
			outputDir = System.getProperty(Profiler.PROFILER_OUTPUT_DIR);

		// Flag to retain the task profiles
		if (conf.get(Profiler.PROFILER_RETAIN_TASK_PROFS) == null)
			conf.set(Profiler.PROFILER_RETAIN_TASK_PROFS,
					System.getProperty(Profiler.PROFILER_RETAIN_TASK_PROFS));

		// Flag to collect the data transfers
		if (conf.get(Profiler.PROFILER_COLLECT_TRANSFERS) == null)
			conf.set(Profiler.PROFILER_COLLECT_TRANSFERS,
					System.getProperty(Profiler.PROFILER_COLLECT_TRANSFERS));

		// Gather the execution files
		Profiler.gatherJobExecutionFiles(job, outputDir);
	}

}
