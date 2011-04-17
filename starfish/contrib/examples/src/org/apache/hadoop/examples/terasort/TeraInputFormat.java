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

package org.apache.hadoop.examples.terasort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

/**
 * An input format that reads the first 10 characters of each line as the key
 * and the rest of the line as the value. Both key and value are represented as
 * Text.
 */
public class TeraInputFormat extends FileInputFormat<Text, Text> {

	static final String PARTITION_FILENAME = "_partition.lst";
	static final String SAMPLE_SIZE = "terasort.partitions.sample";
	private JobContext lastJob = null;
	private List<InputSplit> lastResult = null;

	static class TextSampler implements IndexedSortable {
		private ArrayList<Text> records = new ArrayList<Text>();

		public int compare(int i, int j) {
			Text left = records.get(i);
			Text right = records.get(j);
			return left.compareTo(right);
		}

		public void swap(int i, int j) {
			Text left = records.get(i);
			Text right = records.get(j);
			records.set(j, left);
			records.set(i, right);
		}

		public void addKey(Text key) {
			records.add(new Text(key));
		}

		/**
		 * Find the split points for a given sample. The sample keys are sorted
		 * and down sampled to find even split points for the partitions. The
		 * returned keys should be the start of their respective partitions.
		 * 
		 * @param numPartitions
		 *            the desired number of partitions
		 * @return an array of size numPartitions - 1 that holds the split
		 *         points
		 */
		Text[] createPartitions(int numPartitions) {
			int numRecords = records.size();
			System.out.println("Making " + numPartitions + " from "
					+ numRecords + " records");
			if (numPartitions > numRecords) {
				throw new IllegalArgumentException(
						"Requested more partitions than input keys ("
								+ numPartitions + " > " + numRecords + ")");
			}
			new QuickSort().sort(this, 0, records.size());
			float stepSize = numRecords / (float) numPartitions;
			System.out.println("Step size is " + stepSize);
			Text[] result = new Text[numPartitions - 1];
			for (int i = 1; i < numPartitions; ++i) {
				result[i - 1] = records.get(Math.round(stepSize * i));
			}
			return result;
		}
	}

	/**
	 * Use the input splits to take samples of the input and generate sample
	 * keys. By default reads 100,000 keys from 10 locations in the input, sorts
	 * them and picks N-1 keys to generate N equally sized partitions.
	 * 
	 * @param job
	 *            the job to sample
	 * @param partFile
	 *            where to write the output file to
	 * @throws IOException
	 *             if something goes wrong
	 * @throws InterruptedException
	 */
	public static void writePartitionFile(JobContext job, Path partFile)
			throws IOException, InterruptedException {
		TeraInputFormat inFormat = new TeraInputFormat();
		TextSampler sampler = new TextSampler();
		Configuration conf = job.getConfiguration();
		int partitions = job.getNumReduceTasks();
		long sampleSize = conf.getLong(SAMPLE_SIZE, 100000);
		List<InputSplit> splits = inFormat.getSplits(job);
		int samples = Math.min(10, splits.size());
		long recordsPerSample = sampleSize / samples;
		int sampleStep = splits.size() / samples;
		long records = 0;
		// take N samples from different parts of the input
		for (int i = 0; i < samples; ++i) {
			RecordReader<Text, Text> reader = inFormat.createRecordReader(
					splits.get(sampleStep * i), new TaskAttemptContext(conf,
							new TaskAttemptID()));
			while (reader.nextKeyValue()) {
				sampler.addKey(reader.getCurrentKey());
				records += 1;
				if ((i + 1) * recordsPerSample <= records) {
					break;
				}
			}
		}
		FileSystem outFs = partFile.getFileSystem(conf);
		if (outFs.exists(partFile)) {
			outFs.delete(partFile, false);
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(outFs, conf,
				partFile, Text.class, NullWritable.class);
		NullWritable nullValue = NullWritable.get();
		for (Text split : sampler.createPartitions(partitions)) {
			writer.append(split, nullValue);
		}
		writer.close();
	}

	/**
	 * Generate the list of files and make them into FileSplits.
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		if (job == lastJob) {
			return lastResult;
		}
		lastJob = job;
		lastResult = super.getSplits(job);
		return lastResult;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		RecordReader<Text, Text> rr = new TeraRecordReader();
		rr.initialize(split, context);
		return rr;
	}

	/**
	 * 
	 */
	static class TeraRecordReader extends RecordReader<Text, Text> {
		private LineRecordReader in;
		private static int KEY_LENGTH = 10;
		private Text currentKey = new Text();
		private Text currentValue = new Text();

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return currentKey;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			in = new LineRecordReader();
			in.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (in.nextKeyValue()) {
				Text line = in.getCurrentValue();
				if (line.getLength() < KEY_LENGTH) {
					currentKey.set(line);
					currentValue.clear();
				} else {
					byte[] bytes = line.getBytes();
					currentKey.set(bytes, 0, KEY_LENGTH);
					currentValue.set(bytes, KEY_LENGTH, line.getLength()
							- KEY_LENGTH);
				}
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void close() throws IOException {
			in.close();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return in.getProgress();
		}
	}

}
