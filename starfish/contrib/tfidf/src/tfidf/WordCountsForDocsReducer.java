package tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCountsForDocsReducer counts the number of documents in the Hadoop 0.20.1
 * API
 * 
 * @author Marcello de Sales (marcello.desales@gmail.com)
 */
public class WordCountsForDocsReducer extends Reducer<Text, Text, Text, Text> {

	public WordCountsForDocsReducer() {
	}

	private static Text OUT_KEY = new Text();
	private static Text OUT_VALUE = new Text();

	/**
	 * PRE-CONDITION: receive a list of <document, ["word=n", "word-b=x"]> pairs
	 * <"a.txt", ["word1=3", "word2=5", "word3=5"]>
	 * 
	 * POST-CONDITION: <"word1@a.txt, 3/13">, <"word2@a.txt, 5/13">
	 * 
	 * @param key
	 *            is the key of the mapper
	 * @param values
	 *            are all the values aggregated during the mapping phase
	 * @param context
	 *            contains the context of the job run
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sumOfWordsInDocument = 0;
		Map<String, Integer> tempCounter = new HashMap<String, Integer>();

		for (Text val : values) {
			String[] wordCounter = val.toString().split("=");
			tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
			sumOfWordsInDocument += Integer.parseInt(wordCounter[1]);
		}

		for (Entry<String, Integer> entry : tempCounter.entrySet()) {
			OUT_KEY.set(entry.getKey() + "@" + key.toString());
			OUT_VALUE.set(entry.getValue() + "/" + sumOfWordsInDocument);
			context.write(OUT_KEY, OUT_VALUE);
		}
	}
}
