package tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordsInCorpusTFIDF Creates the index of the words in documents, mapping each
 * of them to their frequency. (Hadoop 0.20.2 API)
 * 
 * @author Marcello de Sales (marcello.desales@gmail.com)
 */
public class WordsInCorpusTFIDF extends Configured implements Tool {

	/**
	 * Setup the MR job
	 * 
	 * @param args
	 * @throws Exception
	 */

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out
					.println("Usage: tf-idf-3 <doc-input> <tf-idf-2-output> <output>");
			System.exit(-1);
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "Docs In Corpus and TF-IDF");
		conf = job.getConfiguration();

		job.setJarByClass(WordsInCorpusTFIDF.class);
		job.setMapperClass(WordsInCorpusTFIDFMapper.class);
		job.setReducerClass(WordsInCorpusTFIDFReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// Getting the number of documents from the original input directory.
		Path inputPath = new Path(args[0]);
		FileSystem fs = inputPath.getFileSystem(conf);
		FileStatus[] matches = fs.globStatus(inputPath);
		if (matches == null || matches.length == 0)
			throw new IOException("Input path does not exist: " + inputPath);

		int docsInCoprus = 0;
		for (FileStatus globStat : matches) {
			if (globStat.isDir()) {
				docsInCoprus += fs.listStatus(globStat.getPath()).length;
			} else {
				docsInCoprus += 1;
			}

		}
		conf.setInt("docsInCorpus", docsInCoprus);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Main driver
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordsInCorpusTFIDF(),
				args);
		System.exit(res);
	}
}
