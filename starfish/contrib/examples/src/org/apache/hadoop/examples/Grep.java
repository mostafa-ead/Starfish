/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* Extracts matching regexs from input files and counts them. */
public class Grep extends Configured implements Tool {

	/**
	 * RegexMapper
	 */
	public static class RegexMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		private final static LongWritable one = new LongWritable(1);
		private Text text = new Text();

		private Pattern pattern;
		private int group;

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			pattern = Pattern.compile(conf.get("mapred.mapper.regex"));
			group = conf.getInt("mapred.mapper.regex.group", 0);
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				text.set(matcher.group(group));
				context.write(text, one);
			}
		}
	}

	/**
	 * Run
	 */
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err
					.println("Usage: Grep <inDir> <outDir> <regex> [<group>]");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Path tempDir = new Path("grep-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		Job grepJob = new Job(getConf(), "grep-search");
		Configuration conf = grepJob.getConfiguration();
		grepJob.setJarByClass(Grep.class);

		try {
			// Set and run the grep job
			FileInputFormat.setInputPaths(grepJob, args[0]);
			conf.set("mapred.mapper.regex", args[2]);
			if (args.length == 4)
				conf.set("mapred.mapper.regex.group", args[3]);

			grepJob.setMapperClass(RegexMapper.class);
			grepJob.setCombinerClass(LongSumReducer.class);
			grepJob.setReducerClass(LongSumReducer.class);

			FileOutputFormat.setOutputPath(grepJob, tempDir);
			grepJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			grepJob.setOutputKeyClass(Text.class);
			grepJob.setOutputValueClass(LongWritable.class);

			grepJob.waitForCompletion(true);

			// Set and run the sort job
			Job sortJob = new Job(getConf(), "grep-sort");
			sortJob.setJarByClass(Grep.class);

			FileInputFormat.setInputPaths(sortJob, tempDir);
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			sortJob.setMapperClass(InverseMapper.class);
			FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));

			// Write a single file and sort in decreasing order
			sortJob.setNumReduceTasks(1);
			sortJob
					.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			sortJob.waitForCompletion(true);

		} finally {
			FileSystem.get(conf).delete(tempDir, true);
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Grep(), args);
		System.exit(res);
	}

}
