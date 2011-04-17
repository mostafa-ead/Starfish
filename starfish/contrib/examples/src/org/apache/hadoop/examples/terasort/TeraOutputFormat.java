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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A streamlined text output format that writes key, value, and "\r\n".
 */
public class TeraOutputFormat extends TextOutputFormat<Text, Text> {
	static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";

	/**
	 * Set the requirement for a final sync before the stream is closed.
	 */
	public static void setFinalSync(JobContext job, boolean newValue) {
		job.getConfiguration().setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
	}

	/**
	 * Does the user want a final sync at close?
	 */
	public static boolean getFinalSync(JobContext job) {
		return job.getConfiguration().getBoolean(FINAL_SYNC_ATTRIBUTE, false);
	}

	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new TeraRecordWriter(fileOut, job);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new TeraRecordWriter(new DataOutputStream(codec
					.createOutputStream(fileOut)), job);
		}
	}

	static class TeraRecordWriter extends LineRecordWriter<Text, Text> {
		private static final byte[] newLine = "\r\n".getBytes();
		private boolean finalSync = false;

		public TeraRecordWriter(DataOutputStream out, JobContext job) {
			super(out);
			finalSync = getFinalSync(job);
		}

		@Override
		public synchronized void write(Text key, Text value) throws IOException {
			out.write(key.getBytes(), 0, key.getLength());
			out.write(value.getBytes(), 0, value.getLength());
			out.write(newLine, 0, newLine.length);
		}

		public void close() throws IOException {
			if (finalSync) {
				((FSDataOutputStream) out).sync();
			}
			super.close(null);
		}
	}

}
