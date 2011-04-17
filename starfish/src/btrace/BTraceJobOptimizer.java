import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.Kind;
import com.sun.btrace.annotations.Location;
import com.sun.btrace.annotations.OnMethod;
import com.sun.btrace.annotations.Self;

import edu.duke.starfish.jobopt.optimizer.JobOptimizer;

/**
 * A BTrace script that dynamically finds the best configuration settings for a
 * MapReduce job.
 * 
 * In order to optimize the job, the job must have submitted the job using the
 * job's method 'waitForCompletion'.
 * 
 * @author hero
 */
@BTrace
public class BTraceJobOptimizer {
	
	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Job", 
			method = "waitForCompletion", 
			location = @Location(value = Kind.ENTRY))
	public static void onJob_waitForCompletion_entry(@Self Job job) {
		
		// Set the user-specified configuration options
		Configuration conf = job.getConfiguration();

		// Set the optimizer type
		if (conf.get("starfish.job.optimizer.type") == null)
			conf.set("starfish.job.optimizer.type",
					System.getProperty("starfish.job.optimizer.type"));

		// Set the task scheduler
		if (conf.get("starfish.whatif.task.scheduler") == null)
			conf.set("starfish.whatif.task.scheduler",
					System.getProperty("starfish.whatif.task.scheduler"));

		// Set the excluded parameters
		if (conf.get("starfish.job.optimizer.exclude.parameters") == null)
			conf.set("starfish.job.optimizer.exclude.parameters",
					System.getProperty("starfish.job.optimizer.exclude.parameters"));

		// Set the output location
		if (conf.get("starfish.job.optimizer.output") == null)
			conf.set("starfish.job.optimizer.outputs",
					System.getProperty("starfish.job.optimizer.output"));		
		
		// Set the number of values per parameter (for full and smart_full optimizers)
		if (conf.get("starfish.job.optimizer.num.values.per.param") == null)
			conf.set("starfish.job.optimizer.num.values.per.param",
					System.getProperty("starfish.job.optimizer.num.values.per.param"));

		// Set the flag for using random values (for full and smart_full optimizers)
		if (conf.get("starfish.job.optimizer.use.random.values") == null)
			conf.set("starfish.job.optimizer.use.random.values",
					System.getProperty("starfish.job.optimizer.use.random.values"));

		// Find the optimal configuration
		String mode = System.getProperty("starfish.job.optimizer.mode");
		String profileFile = System
				.getProperty("starfish.job.optimizer.profile.file");

		if (mode.equalsIgnoreCase("run")) {
			// Optimize the job
			if (!JobOptimizer.processJobOptimizationRequest(job, profileFile)) {
				System.exit(0);
			}

		} else if (mode.equalsIgnoreCase("recommend")) {
			// Recommend a configuration
			JobOptimizer.processJobRecommendationRequest(job, profileFile);
			System.exit(0);

		} else {
			System.exit(0);
		}
	}

}
