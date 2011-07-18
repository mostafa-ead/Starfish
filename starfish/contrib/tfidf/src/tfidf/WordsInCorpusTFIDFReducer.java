package tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordsInCorpusTFIDFReducer calculates the number of documents in corpus that a
 * given key occurs and the TF-IDF computation. The total number of D is
 * acquired from the job name <img src=
 * "http://s2.wp.com/wp-includes/images/smilies/icon_smile.gif?m=1245902109g"
 * alt=":)" class="wp-smiley"> It is a dirty hack, but the only way I could
 * communicate the number from the driver.
 * 
 * @author Marcello de Sales (marcello.desales@gmail.com)
 */
public class WordsInCorpusTFIDFReducer extends Reducer<Text, Text, Text, Text> {

	private static final DecimalFormat DF = new DecimalFormat("###.########");
	private static Text OUT_KEY = new Text();
	private static Text OUT_VALUE = new Text();

	public WordsInCorpusTFIDFReducer() {
	}

	/**
	 * PRECONDITION: receive a list of <word, ["doc1=n1/N1", "doc2=n2/N2"]>
	 * 
	 * POSTCONDITION: <"word@doc1,  [d/D, n/N, TF-IDF]">
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

		// get the number of documents from the configuration
		int numberOfDocsInCorpus = context.getConfiguration().getInt(
				"docsInCorpus", 1);

		// total frequency of this word
		int numberOfDocsInCorpusWithKey = 0;
		Map<String, String> tempFrequencies = new HashMap<String, String>();
		for (Text val : values) {
			String[] documentAndFrequencies = val.toString().split("=");
			numberOfDocsInCorpusWithKey++;
			tempFrequencies.put(documentAndFrequencies[0],
					documentAndFrequencies[1]);
		}

		for (Entry<String, String> entry : tempFrequencies.entrySet()) {
			String[] wordFrequenceAndTotalWords = entry.getValue().split("/");

			// Term frequency is the quotient of the number of terms in document
			// and the total number of terms in doc
			double tf = Double.parseDouble(wordFrequenceAndTotalWords[0])
					/ Double.parseDouble(wordFrequenceAndTotalWords[1]);

			// inverse document frequency quotient between the number of docs in
			// corpus and number of docs the term appears
			double idf = (double) numberOfDocsInCorpus
					/ (double) numberOfDocsInCorpusWithKey;

			// given that log(10) = 0, just consider the term frequency in
			// documents
			double tfIdf = (numberOfDocsInCorpus == numberOfDocsInCorpusWithKey) ? tf
					: tf * Math.log10(idf);

			OUT_KEY.set(key + "@" + entry.getKey());
			OUT_VALUE.set("[" + numberOfDocsInCorpusWithKey + "/"
					+ numberOfDocsInCorpus + " , "
					+ wordFrequenceAndTotalWords[0] + "/"
					+ wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf)
					+ "]");
			context.write(OUT_KEY, OUT_VALUE);
		}
	}
}
