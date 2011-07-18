// (c) Copyright 2009 Cloudera, Inc.
// Hadoop 0.20.1 API Updated by Marcello de Sales (marcello.desales@gmail.com)

package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * LineIndexReducer Takes a list of filename@offset entries for a single word
 * and concatenates them into a list.
 */
public class WordFrequenceInDocReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private static IntWritable COUNT = new IntWritable();

	public WordFrequenceInDocReducer() {
	}

	/**
	 * PRE-CONDITION: receive a list of <"word@filename",[1, 1, 1, ...]> pairs
	 * <"marcello@a.txt", [1, 1]>
	 * 
	 * POST-CONDITION: emit the output a single key-value where the sum of the
	 * occurrences. <"marcello@a.txt", 2>
	 * 
	 * @param key
	 *            is the key of the mapper
	 * @param values
	 *            are all the values aggregated during the mapping phase
	 * @param context
	 *            contains the context of the job run
	 * 
	 */
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		// write the key and the adjusted value
		COUNT.set(sum);
		context.write(key, COUNT);
	}
}
