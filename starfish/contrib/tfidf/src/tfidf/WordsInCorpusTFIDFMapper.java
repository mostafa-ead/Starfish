package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WordsInCorpusTFIDFMapper implements the Job 3 specification for the TF-IDF
 * algorithm
 * 
 * @author Marcello de Sales (marcello.desales@gmail.com)
 */
public class WordsInCorpusTFIDFMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public WordsInCorpusTFIDFMapper() {
	}

	// Reuse writables
	private static Text INTERM_KEY = new Text();
	private static Text INTERM_VALUE = new Text();

	/**
	 * PRE-CONDITION: marcello@book.txt \t 3/1500
	 * 
	 * POST-CONDITION: marcello, book.txt=3/1500
	 * 
	 * @param key
	 *            is the byte offset of the current line in the file;
	 * @param value
	 *            is the line from the file
	 * @param context
	 *            the context of the job
	 * 
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] wordAndCounters = value.toString().split("\t");
		String[] wordAndDoc = wordAndCounters[0].split("@");

		INTERM_KEY.set(wordAndDoc[0]);
		INTERM_VALUE.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
		context.write(INTERM_KEY, INTERM_VALUE);
	}
}
