package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * LineIndexMapper Maps each observed word in a line to a (filename@offset)
 * string. Hadoop 0.20.1 API
 * 
 * @author Marcello de Sales (marcello.desales@gmail.com)
 */
public class WordCountsForDocsMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public WordCountsForDocsMapper() {
	}

	// Reuse writables
	private static Text INTERM_KEY = new Text();
	private static Text INTERM_VALUE = new Text();

	/**
	 * PRE-CONDITION: aa@leornardo-davinci-all.txt 1 aaron@all-shakespeare 98
	 * ab@leornardo-davinci-all.txt 3
	 * 
	 * POST-CONDITION: Output <"all-shakespeare", "aaron=98"> pairs
	 * 
	 * @param key
	 *            is the byte offset of the current line in the file;
	 * @param value
	 *            is the line from the file
	 * @param context
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] wordAndDocCounter = value.toString().split("\t");
		String[] wordAndDoc = wordAndDocCounter[0].split("@");

		INTERM_KEY.set(wordAndDoc[1]);
		INTERM_VALUE.set(wordAndDoc[0] + "=" + wordAndDocCounter[1]);
		context.write(INTERM_KEY, INTERM_VALUE);
	}
}
