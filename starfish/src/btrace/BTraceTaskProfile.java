import static com.sun.btrace.BTraceUtils.println;
import static com.sun.btrace.BTraceUtils.str;
import static com.sun.btrace.BTraceUtils.strcat;
import static com.sun.btrace.BTraceUtils.timeNanos;
import static com.sun.btrace.BTraceUtils.used;
import static com.sun.btrace.BTraceUtils.heapUsage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.btrace.AnyType;
import com.sun.btrace.annotations.BTrace;
import com.sun.btrace.annotations.Duration;
import com.sun.btrace.annotations.Kind;
import com.sun.btrace.annotations.Location;
import com.sun.btrace.annotations.OnMethod;
import com.sun.btrace.annotations.Return;
import com.sun.btrace.annotations.TLS;
import com.sun.btrace.annotations.Where;

/**
 * Profile the execution of a Map-Reduce job on a hadoop cluster.
 * 
 * This class is logically split into three sections:
 * (a) Profile common task parts
 * (b) Profile map-related parts
 * (c) Profile reduce-related parts
 * 
 * It's not possible to split this class into multiple class because the
 * class cannot communicate with each other (i.e. common profiling cannot
 * be used by the map- or reduce-related parts). Also, writing to the same
 * file from multiple classes is not thread-safe.
 * 
 * Supported Hadoop versions: v0.20.2 and v0.20.203.0
 * Unless otherwise noted, the methods instrument both versions
 * 
 * @author hero
 */
@BTrace
public class BTraceTaskProfile {
	
    /*************************************************************************/
	/***************************** TASK COMMON *******************************/
	/*************************************************************************/

	/* ***********************************************************
	 * TASK EXECUTION
	 * **********************************************************/
	@TLS private static long taskRunStartTime = 0l;

	// Hadoop Version: v0.20.2
	@OnMethod(clazz = "org.apache.hadoop.mapred.Child", 
			method = "main", 
			location = @Location(where=Where.BEFORE, value = Kind.CALL, clazz="/.*/", method="run"))
	public static void onChild_main_Before_Call_run() {
		taskRunStartTime = timeNanos();
	}

	// Hadoop Version: v0.20.2
	@OnMethod(clazz = "org.apache.hadoop.mapred.Child", 
			method = "main", 
			location = @Location(where=Where.AFTER, value = Kind.CALL, clazz="/.*/", method="run"))
	public static void onChild_main_After_Call_run() {
		println(strcat("TASK\tTOTAL_RUN\t", str(timeNanos() - taskRunStartTime)));
	}

	// Hadoop Version: v0.20.203.0
	@OnMethod(clazz = "org.apache.hadoop.security.UserGroupInformation", 
			method = "doAs", 
			location = @Location(value = Kind.RETURN))
	public static void onUserGroupInformation_doAs_return(@Duration long duration) {
		println(strcat("TASK\tTOTAL_RUN\t", str(duration)));
	}

	/* ***********************************************************
	 * HANDLE COMPRESSION
	 * **********************************************************/
//	@TLS private static long uncompressStartTime = 0l;
//	@TLS private static long compressStartTime = 0l;
	@TLS private static long uncompressDuration = 0l;
	@TLS private static long compressDuration = 0l;

//	@OnMethod(clazz = "org.apache.hadoop.io.compress.DecompressorStream", 
//			method = "decompress", 
//			location = @Location(where=Where.BEFORE, value=Kind.CALL, clazz="/.*/", method="decompress"))
//	public static void onDecompressorStream_Before_Call_uncompress() {
//		uncompressStartTime = timeNanos();
//	}
//
//	@OnMethod(clazz = "org.apache.hadoop.io.compress.DecompressorStream", 
//			method = "decompress", 
//			location = @Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="decompress"))
//	public static void onDecompressorStream_After_Call_uncompress() {
//		uncompressDuration += timeNanos() - uncompressStartTime;
//	}
//
//	@OnMethod(clazz = "org.apache.hadoop.io.compress.CompressorStream", 
//			method = "compress", 
//			location = @Location(where=Where.BEFORE, value=Kind.CALL, clazz="/.*/", method="compress"))
//	public static void onCompressorStream_Before_Call_uncompress() {
//		compressStartTime = timeNanos();
//	}
//
//	@OnMethod(clazz = "org.apache.hadoop.io.compress.CompressorStream", 
//			method = "compress", 
//			location = @Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="compress"))
//	public static void onCompressorStream_After_Call_uncompress() {
//		compressDuration += timeNanos() - compressStartTime;
//	}

	
	/* ***********************************************************
	 * HANDLE MERGING
	 * **********************************************************/
	@TLS private static long mergerWriteFileDuration = 0l;
	@TLS private static int mergerWriteFileCount = 0;
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.Merger", 
			method = "writeFile",
			location = @Location(value = Kind.RETURN))
	public static void onMerger_writeFile_return(@Duration long duration) {
		mergerWriteFileDuration += duration;
		++mergerWriteFileCount;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$MapOutputBuffer", 
			method = "mergeParts", 
			location = @Location(value = Kind.ENTRY))
	public static void onMapOutputBuffer_mergeParts_entry() {
		mergerWriteFileDuration = 0l;
		mergerWriteFileCount = 0;
	}

	
	/* ***********************************************************
	 * PERFORM COMBINER
	 * **********************************************************/
	@TLS private static long combinerTotalDuration = 0l;
	@TLS private static long combinerWriteDuration = 0l;
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.Task$CombineOutputCollector", 
			method = "collect", 
			location = @Location(value = Kind.RETURN))
	public static void onCombineOutputCollector_collect_return(@Duration long duration) {
		combinerWriteDuration += duration;
	}	

	@OnMethod(clazz = "org.apache.hadoop.mapred.Task$NewCombinerRunner", 
			method = "combine", 
			location = @Location(value = Kind.RETURN))
	public static void onNewCombinerRunner_combine_return(@Duration long duration) {
		combinerTotalDuration += duration;
	}	
	
    
	/*************************************************************************/
	/******************************* MAPPER **********************************/
	/*************************************************************************/

	
	/* ***********************************************************
	 * PERFORM MAPPER SETUP
	 * **********************************************************/
	@TLS private static long mapperSetupStartTime = 0l;

	@OnMethod(clazz="org.apache.hadoop.mapreduce.Mapper", 
			  method="run", 
			  location=@Location(where=Where.BEFORE, value=Kind.CALL, clazz="/.*/", method="setup"))
	public static void onMapper_run_Before_Call_setup() {
		mapperSetupStartTime = timeNanos();
		println(strcat("MAP\tSTARTUP_MEM\t", str(used(heapUsage()))));
	}

	@OnMethod(clazz="org.apache.hadoop.mapreduce.Mapper", 
			  method="run", 
			  location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="setup"))
	public static void onMapper_run_After_Call_setup() {
		println(strcat("MAP\tSETUP\t", str(timeNanos() - mapperSetupStartTime)));
		println(strcat("MAP\tSETUP_MEM\t", str(used(heapUsage()))));
	}

	
	/* ***********************************************************
	 * READ MAP INPUT
	 * **********************************************************/
	@TLS private static long mapInputDuration = 0l;

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(value = Kind.ENTRY))
	public static void onMapper_run_Entry(Mapper<?,?,?,?>.Context context) {
		InputSplit split = context.getInputSplit();
		if (split instanceof FileSplit) {
			println(strcat(strcat("MAP\t", ((FileSplit) split).getPath().toString()), "\t0"));
		} else {
			println("MAP\tNOT_FILE_SPLIT\t0");
		}
	}
		
	@OnMethod(clazz = "org.apache.hadoop.mapreduce.MapContext", 
			method = "nextKeyValue", 
			location = @Location(value = Kind.RETURN))
	public static void onMapContext_nextKeyValue_return(@Duration long duration) {
		mapInputDuration += duration;
	}
	
	@OnMethod(clazz = "org.apache.hadoop.mapreduce.MapContext", 
			method = "getCurrentKey", 
			location = @Location(value = Kind.RETURN))
	public static void onMapContext_getCurrentKey_return(@Duration long duration) {
		mapInputDuration += duration;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.MapContext", 
			method = "getCurrentValue", 
			location = @Location(value = Kind.RETURN))
	public static void onMapContext_getCurrentValue_return(@Duration long duration) {
		mapInputDuration += duration;
	}

	
	/* ***********************************************************
	 * PERFORM MAP PROCESSING
	 * **********************************************************/
	@TLS private static long mapProcessingDuration = 0l;
	@TLS private static long mapProcessingStartTime = 0l;
	@TLS private static long mapInputKByteCount = 0l;
	@TLS private static long mapInputVByteCount = 0l;

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(where=Where.BEFORE, value = Kind.CALL, clazz="/.*/", method="map"))
	public static void onMapper_run_Before_Call_map() {
		mapProcessingStartTime = timeNanos();
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(where=Where.AFTER, value = Kind.CALL, clazz="/.*/", method="map"))
	public static void onMapper_run_After_Call_map(AnyType k, AnyType v, AnyType c) {
		try {
			if (k != null)
				mapInputKByteCount += k.toString().getBytes("UTF-8").length;
			if (v != null)
				mapInputVByteCount += v.toString().getBytes("UTF-8").length;
		} catch (Exception e) {}
		
		mapProcessingDuration += timeNanos() - mapProcessingStartTime;
	}

	
	/* ***********************************************************
	 * WRITE INTERMEDIATE MAP OUTPUT
	 * **********************************************************/
	@TLS private static long mapCollectorWriteDuration = 0l;
	@TLS private static long mapBufferCollectStartTime = 0l;
	@TLS private static long mapBufferCollectDuration = 0l;
	@TLS private static long mapPartitionStartTime = 0l;
	@TLS private static long mapPartitionDuration = 0l;
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$NewOutputCollector", 
			method = "write", 
			location = @Location(value = Kind.RETURN))
	public static void onNewOutputCollector_write_return(@Duration long duration) {
		mapCollectorWriteDuration += duration;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$NewOutputCollector", 
			method = "write", 
			location = @Location(where=Where.BEFORE, value = Kind.CALL, clazz="/.*/", method="getPartition"))
	public static void onNewOutputCollector_write_Before_Call_getPartition() {
		mapPartitionStartTime = timeNanos();
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$NewOutputCollector", 
			method = "write", 
			location = @Location(where=Where.AFTER, value = Kind.CALL, clazz="/.*/", method="getPartition"))
	public static void onNewOutputCollector_write_After_Call_getPartition() {
		mapPartitionDuration += timeNanos() - mapPartitionStartTime;
	}
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$NewOutputCollector", 
			method = "close",
			location = @Location(value = Kind.RETURN))
	public static void onNewOutputCollector_close_return() {
		// These should all be zero but output to be consistent
		// with output from map-only jobs
		println(strcat("MAP\tWRITE\t", str(mapDirectOutputDuration)));
		println(strcat("MAP\tCOMPRESS\t", str(compressDuration)));
		println(strcat("MAP\tKEY_BYTE_COUNT\t", str(mapOutputKByteCount)));
		println(strcat("MAP\tVALUE_BYTE_COUNT\t", str(mapOutputVByteCount)));
	}


	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$MapOutputBuffer", 
			method = "collect", 
			location = @Location(where=Where.AFTER, value = Kind.CALL, clazz="/.*/", method="unlock"))
	public static void onMapOutputBuffer_collect_after_await() {
		mapBufferCollectStartTime = timeNanos();
	}
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$MapOutputBuffer", 
			method = "collect", 
			location = @Location(value = Kind.RETURN))
	public static void onMapOutputBuffer_collect_return() {
		mapBufferCollectDuration += timeNanos() - mapBufferCollectStartTime;
	}

	
	/* ***********************************************************
	 * WRITE DIRECT MAP OUTPUT
	 * **********************************************************/
	@TLS private static long mapDirectOutputDuration = 0l;
	@TLS private static long mapOutputKByteCount = 0l;
	@TLS private static long mapOutputVByteCount = 0l;

	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$NewDirectOutputCollector", 
			method = "write", 
			location = @Location(value = Kind.RETURN))
	public static void onNewDirectOutputCollector_write_return(@Duration long duration, AnyType k, AnyType v) {
		mapCollectorWriteDuration += duration;
		try {
			if (k != null)
				mapOutputKByteCount += k.toString().getBytes("UTF-8").length;
			if (v != null)
				mapOutputVByteCount += v.toString().getBytes("UTF-8").length;
		} catch (Exception e) {}
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$NewDirectOutputCollector", 
			method = "close", 
			location = @Location(value = Kind.RETURN))
	public static void onNewDirectOutputCollector_close_return(@Duration long duration) {
		mapDirectOutputDuration = duration;
		
		// Print direct output info
		println(strcat("MAP\tWRITE\t", str(mapDirectOutputDuration)));
		println(strcat("MAP\tCOMPRESS\t", str(compressDuration)));
		println(strcat("MAP\tKEY_BYTE_COUNT\t", str(mapOutputKByteCount)));
		println(strcat("MAP\tVALUE_BYTE_COUNT\t", str(mapOutputVByteCount)));
	}
	
	/* ***********************************************************
	 * PERFORM MAPPER CLEANUP
	 * **********************************************************/
	@TLS private static long mapCleanupStartTime = 0l;

	@OnMethod(clazz="org.apache.hadoop.mapreduce.Mapper", 
			  method="run", 
			  location=@Location(where=Where.BEFORE, value=Kind.CALL, clazz="/.*/", method="cleanup"))
	public static void onMapper_run_Before_Call_cleanup() {
		mapCleanupStartTime = timeNanos();
	}

	@OnMethod(clazz="org.apache.hadoop.mapreduce.Mapper", 
			  method="run", 
			  location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="cleanup"))
	public static void onMapper_run_After_Call_cleanup() {
		println(strcat("MAP\tCLEANUP\t", str(timeNanos() - mapCleanupStartTime)));
		println(strcat("MAP\tCLEANUP_MEM\t", str(used(heapUsage()))));
	}

	
	/* ***********************************************************
	 * PERFORM MAP SPILL
	 * **********************************************************/
	@TLS private static long sortDuration = 0l;
	@TLS private static int sortNumRecs = 0;
	@TLS private static long spillRawByteCount = 0l;
	@TLS private static long spillCompressedByteCount = 0l;

	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$MapOutputBuffer", 
			method = "sortAndSpill", 
			location = @Location(value = Kind.ENTRY))
	public static void onMapOutputBuffer_sortAndSpill_entry() {
		combinerTotalDuration = 0l;
		combinerWriteDuration = 0l;
		compressDuration = 0l;
		spillRawByteCount = 0l;
		spillCompressedByteCount = 0l;
	}
	
	@OnMethod(clazz = "org.apache.hadoop.util.QuickSort", 
			method = "sort", 
			location = @Location(value = Kind.RETURN))
	public static void onQuickSort_sort_return(@Duration long duration, 
			AnyType s, int l, int r, AnyType rep) {
		sortDuration = duration;
		sortNumRecs = r-l;
	}
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$MapOutputBuffer", 
			method = "sortAndSpill", 
		    location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="getRawLength"))
	public static void onMapOutputBuffer_sortAndSpill_getRawLength(@Return long length) {
		spillRawByteCount += length;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$MapOutputBuffer", 
			method = "sortAndSpill", 
		    location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="getCompressedLength"))
	public static void onMapOutputBuffer_sortAndSpill_getCompressedLength(@Return long length) {
		spillCompressedByteCount += length;
	}
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$MapOutputBuffer", 
			method = "sortAndSpill", 
			location = @Location(value = Kind.RETURN))
	public static void onMapOutputBuffer_sortAndSpill_return(@Duration long duration) {
		println(strcat("SPILL\tSORT_AND_SPILL\t", str(duration)));
		println(strcat("SPILL\tQUICK_SORT\t", str(sortDuration)));
		println(strcat("SPILL\tSORT_COUNT\t", str(sortNumRecs)));
		println(strcat("SPILL\tCOMBINE\t", str(combinerTotalDuration)));
		if (combinerTotalDuration == 0)
			println(strcat("SPILL\tWRITE\t", str(duration - sortDuration)));
		else
			println(strcat("SPILL\tWRITE\t", str(combinerWriteDuration)));
		println(strcat("SPILL\tCOMPRESS\t", str(compressDuration)));
		println(strcat("SPILL\tUNCOMPRESS_BYTE_COUNT\t", str(spillRawByteCount)));
		println(strcat("SPILL\tCOMPRESS_BYTE_COUNT\t", str(spillCompressedByteCount)));

		combinerTotalDuration = 0l;
		combinerWriteDuration = 0l;
		compressDuration = 0l;
	}	

	
	/* ***********************************************************
	 * PERFORM MERGE OF INTERMEDIATE OUTPUT
	 * **********************************************************/

	@OnMethod(clazz = "org.apache.hadoop.mapred.MapTask$MapOutputBuffer", 
			method = "mergeParts", 
			location = @Location(value = Kind.RETURN))
	public static void onMapOutputBuffer_mergeParts_return(@Duration long duration) {
		println(strcat("MERGE\tTOTAL_MERGE\t", str(duration)));
		println(strcat("MERGE\tREAD_WRITE\t", str(mergerWriteFileDuration)));
		println(strcat("MERGE\tREAD_WRITE_COUNT\t", str(mergerWriteFileCount)));
		println(strcat("MERGE\tUNCOMPRESS\t", str(uncompressDuration)));
		println(strcat("MERGE\tCOMPRESS\t", str(compressDuration)));

		uncompressDuration = 0l;
		compressDuration = 0l;
	}


	/* ***********************************************************
	 * DONE WITH MAPPER EXECUTION
	 * **********************************************************/

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Mapper", 
			method = "run", 
			location = @Location(value = Kind.RETURN))
	public static void onMapper_run_return(@Duration long duration) {
		// Print out the map statistics
		println(strcat("MAP\tTOTAL_RUN\t", str(duration)));
		println(strcat("MAP\tREAD\t", str(mapInputDuration)));
		println(strcat("MAP\tUNCOMPRESS\t", str(uncompressDuration)));
		println(strcat("MAP\tKEY_BYTE_COUNT\t", str(mapInputKByteCount)));
		println(strcat("MAP\tVALUE_BYTE_COUNT\t", str(mapInputVByteCount)));
		println(strcat("MAP\tMAP\t", str(mapProcessingDuration)));
		println(strcat("MAP\tWRITE\t", str(mapCollectorWriteDuration)));
		println(strcat("MAP\tCOMPRESS\t", str(compressDuration)));
		println(strcat("MAP\tPARTITION_OUTPUT\t", str(mapPartitionDuration)));
		println(strcat("MAP\tSERIALIZE_OUTPUT\t", str(mapBufferCollectDuration)));
		println(strcat("MAP\tMAP_MEM\t", str(used(heapUsage()))));

		uncompressDuration = 0l;
		compressDuration = 0l;
	}
	

	/*************************************************************************/
	/******************************* REDUCER *********************************/
	/*************************************************************************/
	
	
	/* ***********************************************************
	 * SHUFFLE MAP OUTPUT TO REDUCER
	 * **********************************************************/
	@TLS private static boolean onReducer = false;
	@TLS private static int toggleByteCount = 0;
	@TLS private static String shuffleUncomprByteCount = "";
	@TLS private static String shuffleComprByteCount = "";

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier", 
			method = "fetchOutputs", 
			location = @Location(value = Kind.RETURN))
	public static void onReduceCopier_fetchOutputs_return(@Duration long duration) {
		onReducer = true;
		uncompressDuration = 0l;
		compressDuration = 0l;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$MapOutputCopier", 
			  method="getMapOutput", 
			  location=@Location(where=Where.BEFORE, value=Kind.CALL, clazz="java.lang.Long", method="parseLong"))
	public static void onReducerCopier_getMapOutput_Before_Call_parseLong(String s) {
		if (toggleByteCount == 0)
			shuffleUncomprByteCount = s;
		else
			shuffleComprByteCount = s;
		toggleByteCount = 1 - toggleByteCount;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$MapOutputCopier", 
			  method="getMapOutput", 
			  location=@Location(value = Kind.RETURN))
	public static void onReducerCopier_getMapOutput_return(@Duration long duration) {
		String out = strcat("SHUFFLE\tUNCOMPRESS_BYTE_COUNT\t", shuffleUncomprByteCount);
		out += strcat("\nSHUFFLE\tCOMPRESS_BYTE_COUNT\t", shuffleComprByteCount);
		out += strcat("\nSHUFFLE\tCOPY_MAP_DATA\t", str(duration));
		out += strcat("\nSHUFFLE\tUNCOMPRESS\t", str(uncompressDuration));
		println(out);
		
		uncompressDuration = 0l;
	}

	
	/* ***********************************************************
	 * ENABLE COLLECTION OF DATA TRANSFERS (Hadoop v0.20.203.0)
	 * **********************************************************/

	@TLS private static int toggleDebugShuffling = 0;
	@TLS private static int toggleDebugRead = 1;
	private static boolean collectTransfers = false;
	
	// Hadoop v0.20.203.0
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier", 
			method = "configureClasspath", 
			location = @Location(value = Kind.ENTRY))
	public static void onReduceCopier_configureClasspath_return(AnyType conf) {
		collectTransfers = ((Configuration) conf).getBoolean("starfish.profiler.collect.data.transfers", false);
	}
	
	// Hadoop v0.20.203.0
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$MapOutputCopier", 
			  method="getMapOutput", 
			  location=@Location(where=Where.BEFORE, value=Kind.CALL, 
					  			 clazz="org.apache.commons.logging.Log", method="isDebugEnabled"))
	public static void onReducerCopier_getMapOutput_Before_Call_isDebugEnabled() {
		if (collectTransfers) {
			if (toggleDebugShuffling == 1) {
				Logger.getLogger("org.apache.hadoop.mapred.ReduceTask").setLevel(Level.DEBUG);
			}
			toggleDebugShuffling = 1 - toggleDebugShuffling;
		}
	}

	// Hadoop v0.20.203.0
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$MapOutputCopier", 
			  method="getMapOutput", 
			  location=@Location(where=Where.AFTER, value=Kind.CALL, 
					  			 clazz="org.apache.commons.logging.Log", method="debug"))
	public static void onReducerCopier_getMapOutput_After_Call_debug() {
		if (collectTransfers) {
			Logger.getLogger("org.apache.hadoop.mapred.ReduceTask").setLevel(Level.INFO);
		}
	}

	// Hadoop v0.20.203.0
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$MapOutputCopier", 
			  method="shuffleInMemory", 
			  location=@Location(where=Where.BEFORE, value=Kind.CALL, 
					  			 clazz="org.apache.commons.logging.Log", method="isDebugEnabled"))
	public static void onReducerCopier_shuffleInMemory_Before_Call_isDebugEnabled() {
		if (collectTransfers) {
			if (toggleDebugRead == 1) {
				Logger.getLogger("org.apache.hadoop.mapred.ReduceTask").setLevel(Level.DEBUG);
			}
			toggleDebugRead = 1 - toggleDebugRead;
		}
	}

	// Hadoop v0.20.203.0
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$MapOutputCopier", 
			  method="shuffleInMemory", 
			  location=@Location(where=Where.AFTER, value=Kind.CALL, 
					  			 clazz="org.apache.commons.logging.Log", method="debug"))
	public static void onReducerCopier_shuffleInMemory_After_Call_debug() {
		if (collectTransfers) {
			Logger.getLogger("org.apache.hadoop.mapred.ReduceTask").setLevel(Level.INFO);
		}
	}
	
	
	/* ***********************************************************
	 * SORT/MERGE DURING SHUFFLING
	 * **********************************************************/
	@TLS private static long doInMemMergeDuration = 0l;
	@TLS private static long doOnDiskMergeDuration = 0l;
	@TLS private static long doOnDiskMergeStartTime = 0l;

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$InMemFSMergeThread", 
			method = "doInMemMerge", 
			location = @Location(value = Kind.RETURN))
	public static void onInMemFSMergeThread_doInMemMerge_return(@Duration long duration) {
		doInMemMergeDuration += duration;
	}
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$InMemFSMergeThread", 
			method = "run", 
			location = @Location(value = Kind.RETURN))
	public static void onInMemFSMergeThread_run_return() {
		if (doInMemMergeDuration != 0l) {
			println(strcat("MERGE\tMERGE_IN_MEMORY\t", str(doInMemMergeDuration)));
			println(strcat("MERGE\tREAD_WRITE\t", str(mergerWriteFileDuration)));
			println(strcat("MERGE\tREAD_WRITE_COUNT\t", str(mergerWriteFileCount)));
			println(strcat("MERGE\tCOMBINE\t", str(combinerTotalDuration)));
			println(strcat("MERGE\tWRITE\t", str(combinerWriteDuration)));
			println(strcat("MERGE\tUNCOMPRESS\t", str(uncompressDuration)));
			println(strcat("MERGE\tCOMPRESS\t", str(compressDuration)));
		}
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$LocalFSMerger", 
			method = "run", 
			  location=@Location(where=Where.BEFORE, value=Kind.CALL, clazz="/.*/", method="getLocalPathForWrite"))
	public static void onLocalFSMerger_run_Before_Merge() {
		doOnDiskMergeStartTime = timeNanos();
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$LocalFSMerger", 
			method = "run", 
			  location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="addToMapOutputFilesOnDisk"))
	public static void onLocalFSMerger_run_After_Merge() {
		doOnDiskMergeDuration += timeNanos() - doOnDiskMergeStartTime;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier$LocalFSMerger", 
			method = "run", 
			location = @Location(value = Kind.RETURN))
	public static void onLocalFSMerger_run_return() {
		if (doOnDiskMergeDuration != 0l) {
			println(strcat("MERGE\tMERGE_TO_DISK\t", str(doOnDiskMergeDuration)));
			println(strcat("MERGE\tREAD_WRITE\t", str(mergerWriteFileDuration)));
			println(strcat("MERGE\tREAD_WRITE_COUNT\t", str(mergerWriteFileCount)));
			println(strcat("MERGE\tCOMBINE\t", str(combinerTotalDuration)));
			println(strcat("MERGE\tWRITE\t", str(combinerWriteDuration)));
			println(strcat("MERGE\tUNCOMPRESS\t", str(uncompressDuration)));
			println(strcat("MERGE\tCOMPRESS\t", str(compressDuration)));
		}
	}

	
	/* ***********************************************************
	 * SORT/MERGE MAP OUTPUT DATA
	 * **********************************************************/

	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier", 
			method = "createKVIterator", 
			location = @Location(value = Kind.ENTRY))
	public static void onReduceCopier_createKVIterator_entry() {
		if (onReducer) {
			mergerWriteFileCount = 0;
			mergerWriteFileDuration = 0;
		}
	}
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$ReduceCopier", 
			method = "createKVIterator", 
			location = @Location(value = Kind.RETURN))
	public static void onReduceCopier_createKVIterator_return(@Duration long duration) {
		if (onReducer) {
			println(strcat("SORT\tMERGE_MAP_DATA\t", str(duration)));
			println(strcat("SORT\tREAD_WRITE\t", str(mergerWriteFileDuration)));
			println(strcat("SORT\tREAD_WRITE_COUNT\t", str(mergerWriteFileCount)));
			println(strcat("SORT\tUNCOMPRESS\t", str(uncompressDuration)));
			println(strcat("SORT\tCOMPRESS\t", str(compressDuration)));

			uncompressDuration = 0l;
			compressDuration = 0l;
		}
	}
	
	
	/* ***********************************************************
	 * PERFORM REDUCER SETUP
	 * **********************************************************/
	@TLS private static long reducerSetupStartTime = 0l;

	@OnMethod(clazz="org.apache.hadoop.mapreduce.Reducer", 
			  method="run", 
			  location=@Location(where=Where.BEFORE, value=Kind.CALL, clazz="/.*/", method="setup"))
	public static void onReducer_run_Before_Call_setup() {
		if (onReducer) {
			reducerSetupStartTime = timeNanos();
			println(strcat("REDUCE\tSTARTUP_MEM\t", str(used(heapUsage()))));
		}
	}

	@OnMethod(clazz="org.apache.hadoop.mapreduce.Reducer", 
			  method="run", 
			  location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="setup"))
	public static void onReducer_run_After_Call_setup() {
		if (onReducer) {
			println(strcat("REDUCE\tSETUP\t", 
					str(timeNanos() - reducerSetupStartTime)));
			println(strcat("REDUCE\tSETUP_MEM\t", str(used(heapUsage()))));
		}
	}
	
	
	/* ***********************************************************
	 * READ REDUCER INPUT
	 * **********************************************************/
	@TLS private static long reduceInputDuration = 0l;

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.ReduceContext", 
			method = "nextKey", 
			location = @Location(value = Kind.RETURN))
	public static void onReduceContext_nextKey_return(@Duration long duration) {
		if (onReducer)
			reduceInputDuration += duration;
	}
	
	@OnMethod(clazz = "org.apache.hadoop.mapreduce.ReduceContext", 
			method = "getCurrentKey", 
			location = @Location(value = Kind.RETURN))
	public static void onReduceContext_getCurrentKey_return(@Duration long duration) {
		if (onReducer)
			reduceInputDuration += duration;
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.ReduceContext", 
			method = "getValues", 
			location = @Location(value = Kind.RETURN))
	public static void onReduceContext_getValues_return(@Duration long duration) {
		if (onReducer)
			reduceInputDuration += duration;
	}

	
	/* ***********************************************************
	 * PERFORM REDUCE PROCESSING
	 * **********************************************************/
	@TLS private static long reduceProcessingDuration = 0l;
	@TLS private static long reduceProcessingStartTime = 0l;

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Reducer", 
			method = "run", 
			location = @Location(where=Where.BEFORE, value = Kind.CALL, clazz="/.*/", method="reduce"))
	public static void onReducer_run_Before_Call_reduce() {
		if (onReducer)
			reduceProcessingStartTime = timeNanos();
	}

	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Reducer", 
			method = "run", 
			location = @Location(where=Where.AFTER, value = Kind.CALL, clazz="/.*/", method="reduce"))
	public static void onReducer_run_After_Call_reduce() {
		if (onReducer)
			reduceProcessingDuration += timeNanos() - reduceProcessingStartTime;
	}

	
	/* ***********************************************************
	 * WRITE REDUCER OUTPUT
	 * **********************************************************/
	@TLS private static long reduceWriteDuration = 0l;
	@TLS private static long reduceWriterCloseStartTime = 0l;
	@TLS private static long reduceWriteKByteCount = 0l;
	@TLS private static long reduceWriteVByteCount = 0l;
	
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$NewTrackingRecordWriter", 
			method = "write", 
			location = @Location(value = Kind.RETURN))
	public static void onNewTrackingRecordWriter_write_return(@Duration long duration, AnyType k, AnyType v) {
		if (onReducer) {
			reduceWriteDuration += duration;
			try {
				if (k != null)
					reduceWriteKByteCount += k.toString().getBytes("UTF-8").length;
				if (v != null)
					reduceWriteVByteCount += v.toString().getBytes("UTF-8").length;
			} catch (Exception e) {}
		}
	}

	// Hadoop Version: v0.20.203.0
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask$NewTrackingRecordWriter", 
			method = "close", 
			location = @Location(value = Kind.RETURN))
	public static void onNewTrackingRecordWriter_close_return(@Duration long duration) {
		if (onReducer) {
			println(strcat("REDUCE\tWRITE\t", str(duration)));
			println(strcat("REDUCE\tCOMPRESS\t", str(compressDuration)));
			compressDuration = 0l;
		}
	}

	// Hadoop Version: v0.20.2
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask", 
			method = "runNewReducer", 
			location = @Location(where=Where.BEFORE, value = Kind.CALL, clazz="/.*/", method="close"))
	public static void onReduceTask_runNewReducer_Before_Call_close() {
		if (onReducer)
			reduceWriterCloseStartTime = timeNanos();
	}

	// Hadoop Version: v0.20.2
	@OnMethod(clazz = "org.apache.hadoop.mapred.ReduceTask", 
			method = "runNewReducer", 
			location = @Location(where=Where.AFTER, value = Kind.CALL, clazz="/.*/", method="close"))
	public static void onReduceTask_runNewReducer_After_Call_close() {
		if (onReducer) {
			println(strcat("REDUCE\tWRITE\t", str(timeNanos() - reduceWriterCloseStartTime)));
			println(strcat("REDUCE\tCOMPRESS\t", str(compressDuration)));
			compressDuration = 0l;
		}
	}


	/* ***********************************************************
	 * PERFORM REDUCER CLEANUP
	 * **********************************************************/
	@TLS private static long reducerCleanupStartTime = 0l;

	@OnMethod(clazz="org.apache.hadoop.mapreduce.Reducer", 
			  method="run", 
			  location=@Location(where=Where.BEFORE, value=Kind.CALL, clazz="/.*/", method="cleanup"))
	public static void onReducer_run_Before_Call_cleanup() {
		if (onReducer)
			reducerCleanupStartTime = timeNanos();
	}

	@OnMethod(clazz="org.apache.hadoop.mapreduce.Reducer", 
			  method="run", 
			  location=@Location(where=Where.AFTER, value=Kind.CALL, clazz="/.*/", method="cleanup"))
	public static void onReducer_run_After_Call_cleanup() {
		if (onReducer) {
			println(strcat("REDUCE\tCLEANUP\t", str(timeNanos() - reducerCleanupStartTime)));
			println(strcat("REDUCE\tCLEANUP_MEM\t", str(used(heapUsage()))));
		}
	}

	
	/* ***********************************************************
	 * DONE WITH REDUCER EXECUTION
	 * **********************************************************/
	
	@OnMethod(clazz = "org.apache.hadoop.mapreduce.Reducer", 
			method = "run", 
			location = @Location(value = Kind.RETURN))
	public static void onReducer_run_return(@Duration long duration) {
		// Print out the reducer statistics
		if (onReducer) {
			println(strcat("REDUCE\tTOTAL_RUN\t", str(duration)));
			println(strcat("REDUCE\tREAD\t", str(reduceInputDuration)));
			println(strcat("REDUCE\tUNCOMPRESS\t", str(uncompressDuration)));
			println(strcat("REDUCE\tREDUCE\t", str(reduceProcessingDuration)));
			println(strcat("REDUCE\tWRITE\t", str(reduceWriteDuration)));
			println(strcat("REDUCE\tCOMPRESS\t", str(compressDuration)));
			println(strcat("REDUCE\tKEY_BYTE_COUNT\t", str(reduceWriteKByteCount)));
			println(strcat("REDUCE\tVALUE_BYTE_COUNT\t", str(reduceWriteVByteCount)));
			println(strcat("REDUCE\tREDUCE_MEM\t", str(used(heapUsage()))));

			uncompressDuration = 0l;
			compressDuration = 0l;
		}
	}

}

