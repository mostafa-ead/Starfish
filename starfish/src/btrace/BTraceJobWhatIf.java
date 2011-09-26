import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.Kind;
import com.sun.btrace.annotations.Location;
import com.sun.btrace.annotations.OnMethod;
import com.sun.btrace.annotations.Self;

import edu.duke.starfish.profile.profiler.Profiler;
import edu.duke.starfish.whatif.WhatIfEngine;

/**
 * A BTrace script that dynamically answers what-if questions for a MapReduce
 * job.
 * 
 * In order to answer the what-if question, the job must have submitted the job
 * using the job's methods 'submit' or 'waitForCompletion'.
 * 
 * @author hero
 */
@BTrace
public class BTraceJobWhatIf {

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Job", 
			method = "submit", 
			location = @Location(value = Kind.ENTRY))
	public static void onJob_submit_entry(@Self Job job) {

		// Load the system properties
		Configuration conf = job.getConfiguration();
		Profiler.loadCommonSystemProperties(conf);

		// Process the what-if request
		String mode = System.getProperty("starfish.whatif.mode");
		String profileId = System.getProperty("starfish.whatif.profile.id");
		WhatIfEngine.processJobWhatIfRequest(mode, profileId, conf);
		
		System.exit(0);
	}

}
