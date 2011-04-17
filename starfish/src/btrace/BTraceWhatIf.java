import org.apache.hadoop.mapreduce.Job;

import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.Kind;
import com.sun.btrace.annotations.Location;
import com.sun.btrace.annotations.OnMethod;
import com.sun.btrace.annotations.Self;

import edu.duke.starfish.whatif.WhatIfEngine;

/**
 * A BTrace script that dynamically answers what-if questions for a MapReduce
 * job.
 * 
 * In order to answer the what-if question, the job must have submitted the job
 * using the job's method 'waitForCompletion'.
 * 
 * @author hero
 */
@BTrace
public class BTraceWhatIf {
	
	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Job", 
			method = "waitForCompletion", 
			location = @Location(value = Kind.ENTRY))
	public static void onJob_waitForCompletion_entry(@Self Job job) {
		
		String mode = System.getProperty("starfish.whatif.mode");
		String profileFile = System.getProperty("starfish.whatif.profile.file");

		WhatIfEngine.processJobWhatIfRequest(mode, profileFile, job
				.getConfiguration());
		
		System.exit(0);
	}

}
