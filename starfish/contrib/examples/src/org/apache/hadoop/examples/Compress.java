package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map-only job that compressed the input
 * 
 * @author hero
 */
public class Compress extends Configured implements Tool {

	/**
	 * CompressMapper - simply output each record
	 */
	public static class CompressMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		NullWritable NULL = NullWritable.get();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(NULL, value);
		}
	}

	/**
	 * The main run methods
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: compress <in> <out>");
			return -1;
		}
		Job job = new Job(getConf(), "compress");
		job.setJarByClass(Compress.class);
		job.setMapperClass(CompressMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.getConfiguration().setBoolean("mapred.output.compress", true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Compress(), args);
		System.exit(res);
	}

}
