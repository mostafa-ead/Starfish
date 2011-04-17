// (c) Copyright 2009 Cloudera, Inc.
// Hadoop 0.20.1 API Updated by Marcello de Sales (marcello.desales@gmail.com)
package tfidf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * WordFrequenceInDocMapper implements the Job 1 specification for the TF-IDF
 * algorithm
 */
public class WordFrequenceInDocMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public WordFrequenceInDocMapper() {
	}

	/**
	 * Google's search Stopwords
	 */
	private static Set<String> googleStopwords;

	static {
		googleStopwords = new HashSet<String>();
		googleStopwords.add("I");
		googleStopwords.add("a");
		googleStopwords.add("about");
		googleStopwords.add("an");
		googleStopwords.add("are");
		googleStopwords.add("as");
		googleStopwords.add("at");
		googleStopwords.add("be");
		googleStopwords.add("by");
		googleStopwords.add("com");
		googleStopwords.add("de");
		googleStopwords.add("en");
		googleStopwords.add("for");
		googleStopwords.add("from");
		googleStopwords.add("how");
		googleStopwords.add("in");
		googleStopwords.add("is");
		googleStopwords.add("it");
		googleStopwords.add("la");
		googleStopwords.add("of");
		googleStopwords.add("on");
		googleStopwords.add("or");
		googleStopwords.add("that");
		googleStopwords.add("the");
		googleStopwords.add("this");
		googleStopwords.add("to");
		googleStopwords.add("was");
		googleStopwords.add("what");
		googleStopwords.add("when");
		googleStopwords.add("where");
		googleStopwords.add("who");
		googleStopwords.add("will");
		googleStopwords.add("with");
		googleStopwords.add("and");
		googleStopwords.add("the");
		googleStopwords.add("www");
	}

	// Compile all the words using regex
	private static Pattern pattern = Pattern.compile("\\w+");

	// Reuse writeables
	private static Text INTERM_KEY = new Text();
	private static IntWritable ONE = new IntWritable(1);
	private static StringBuilder valueBuilder = new StringBuilder();

	/**
	 * POST-CONDITION: Output <"word", "filename@offset"> pairs
	 * 
	 * @param key
	 *            is the byte offset of the current line in the file;
	 * @param value
	 *            is the line from the file
	 * @param context
	 *            contains the context of the job run
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Match all words in the line
		Matcher m = pattern.matcher(value.toString());

		// Get the name of the file from the inputsplit in the context
		String fileName = ((FileSplit) context.getInputSplit()).getPath()
				.getName();

		// build the values and write <k,v> pairs through the context
		while (m.find()) {
			String matchedKey = m.group().toLowerCase();

			// remove names starting with non letters, digits, considered
			// stopwords or containing other chars
			if (!Character.isLetter(matchedKey.charAt(0))
					|| Character.isDigit(matchedKey.charAt(0))
					|| googleStopwords.contains(matchedKey)
					|| matchedKey.contains("_")) {
				continue;
			}
			valueBuilder.delete(0, valueBuilder.length());
			valueBuilder.append(matchedKey);
			valueBuilder.append("@");
			valueBuilder.append(fileName);

			// emit the partial <k,v>
			INTERM_KEY.set(valueBuilder.toString());
			context.write(INTERM_KEY, ONE);
		}
	}
}
