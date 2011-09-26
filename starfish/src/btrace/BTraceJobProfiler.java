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
import edu.duke.starfish.profile.utils.Constants;

/**
 * A BTrace script that dynamically enables profiling for a MapReduce job.
 * 
 * In order to enable profiling, the job must implement the new API.
 * 
 * In order to compute and profile at the end, the job must have submitted the
 * job using the job's method 'waitForCompletion' or via JobControl.
 * 
 * @author hero
 */
@BTrace
public class BTraceJobProfiler {
		
		/**
		 * Initial probe to enable profiling
		 */
		@OnMethod(clazz = "org.apache.hadoop.mapred.JobClient", 
				method = "submitJobInternal", 
				location = @Location(value = Kind.ENTRY))
		public static void onJobClient_submitJobInternal_entry(AnyType input) {

			// Load the system properties in the configuration
			Configuration conf = (Configuration) input;
			Profiler.loadProfilingSystemProperties(conf);

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

		
		/**
		 * Probe for performing task sampling
		 */
		@OnMethod(clazz = "org.apache.hadoop.mapred.JobClient", 
				method = "writeNewSplits", 
			    location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="getSplits"))
		public static void onJobClient_writeNewSplits_getSplits(AnyType job, @Return List<InputSplit> splits) {

			JobContext context = (JobContext) job;
			Configuration conf = context.getConfiguration();
			if (conf.getBoolean(Constants.MR_TASK_PROFILE, false) &&
					conf.get(Profiler.PROFILER_SAMPLING_MODE, "off").equals("tasks")) {
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
			
			// Get the output directory
			Configuration conf = job.getConfiguration();
			String outputDir = conf.get(Profiler.PROFILER_OUTPUT_DIR);

			// Gather the execution files
			Profiler.gatherJobExecutionFiles(conf, outputDir);
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
				// Get the output directory
				Configuration conf = job.getJobConf();
				String outputDir = conf.get(Profiler.PROFILER_OUTPUT_DIR);

				// Gather the execution files
				Profiler.gatherJobExecutionFiles(conf, outputDir);
			}
		}

	}
