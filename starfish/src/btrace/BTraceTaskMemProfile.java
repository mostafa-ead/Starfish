import static com.sun.btrace.BTraceUtils.*;

import com.sun.btrace.AnyType;
import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.Duration;
import com.sun.btrace.annotations.Kind;
import com.sun.btrace.annotations.Location;
import com.sun.btrace.annotations.OnMethod;
import com.sun.btrace.annotations.TLS;
import com.sun.btrace.annotations.Where;

/**
 * This BTrace scripts prints out a memory trace for a Map or Reduce tasks. It
 * prints out the initial used heap memory in setup, the used memory in
 * map/reduce every 1000 records, and the final memory in cleanup.
 * 
 * @author hero
 */
@BTrace
public class BTraceTaskMemProfile {

	/*************************************************************************/
	/********************************* MAP ***********************************/
	/*************************************************************************/
	@TLS private static int numMapRecords = 0;

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(where = Where.BEFORE, value = Kind.CALL, clazz = "/.*/", method = "setup"))
	public static void onMapper_run_Before_Call_setup() {
		println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(where = Where.AFTER, value = Kind.CALL, clazz = "/.*/", method = "setup"))
	public static void onMapper_run_After_Call_setup() {
		println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(where = Where.AFTER, value = Kind.CALL, clazz = "/.*/", method = "map"))
	public static void onMapper_run_After_Call_map(AnyType k, AnyType v,
			AnyType c) {
		++numMapRecords;
		if (numMapRecords == 1000) {
			println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
			numMapRecords = 0;
		}
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(where = Where.BEFORE, value = Kind.CALL, clazz = "/.*/", method = "cleanup"))
	public static void onMapper_run_Before_Call_cleanup() {
		println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(where = Where.AFTER, value = Kind.CALL, clazz = "/.*/", method = "cleanup"))
	public static void onMapper_run_After_Call_cleanup() {
		println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
	}

	/*************************************************************************/
	/******************************* REDUCE **********************************/
	/*************************************************************************/
	@TLS private static int numReduceRecords = 0;

	@TLS private static boolean onReducer = false;

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier", 
			method = "fetchOutputs", 
			location = @Location(value = Kind.RETURN))
	public static void onReduceCopier_fetchOutputs_return(
			@Duration long duration) {
		onReducer = true;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Reducer", 
			method = "run", 
			location = @Location(where = Where.BEFORE, value = Kind.CALL, clazz = "/.*/", method = "setup"))
	public static void onReducer_run_Before_Call_setup() {
		if (onReducer) {
			println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
		}
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Reducer", 
			method = "run", 
			location = @Location(where = Where.AFTER, value = Kind.CALL, clazz = "/.*/", method = "setup"))
	public static void onReducer_run_After_Call_setup() {
		if (onReducer) {
			println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
		}
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Reducer", 
			method = "run", 
			location = @Location(where = Where.AFTER, value = Kind.CALL, clazz = "/.*/", method = "reduce"))
	public static void onReducer_run_After_Call_reduce() {
		if (onReducer) {
			++numReduceRecords;
			if (numReduceRecords == 1000) {
				println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
				numReduceRecords = 0;
			}
		}
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Reducer", 
			method = "run", 
			location = @Location(where = Where.BEFORE, value = Kind.CALL, clazz = "/.*/", method = "cleanup"))
	public static void onReducer_run_Before_Call_cleanup() {
		if (onReducer) {
			println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
		}
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Reducer", 
			method = "run", 
			location = @Location(where = Where.AFTER, value = Kind.CALL, clazz = "/.*/", method = "cleanup"))
	public static void onReducer_run_After_Call_cleanup() {
		if (onReducer) {
			println("MEMORY\t" + timeMillis() + "\t" + used(heapUsage()));
		}
	}

}
