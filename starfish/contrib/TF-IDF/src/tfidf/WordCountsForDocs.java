package tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCountsForDocs counts the total number of words in each document and
 * produces data with the relative and total number of words for each document.
 * (Hadoop 0.20.2 API)
 * 
 * @author Marcello de Sales (marcello.desales@gmail.com)
 */
public class WordCountsForDocs extends Configured implements Tool {

	/**
	 * Setup the MR job
	 * 
	 * @param args
	 * @throws Exception
	 */

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: tf-idf-2 <tf-idf-1-output> <tf-idf-2-output>");
			System.exit(-1);
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "Words Counts for Docs");

		job.setJarByClass(WordCountsForDocs.class);
		job.setMapperClass(WordCountsForDocsMapper.class);
		job.setReducerClass(WordCountsForDocsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Main driver
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountsForDocs(),
				args);
		System.exit(res);
	}
}
