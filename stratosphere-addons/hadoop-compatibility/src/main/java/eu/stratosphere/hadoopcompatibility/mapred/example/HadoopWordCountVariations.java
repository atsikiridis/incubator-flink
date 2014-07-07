/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.hadoopcompatibility.mapred.example;


import eu.stratosphere.hadoopcompatibility.mapred.StratosphereHadoopJobClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

import java.io.IOException;
import java.util.Iterator;

public class HadoopWordCountVariations {

	public static class TestTokenizeMap<K> extends TokenCountMapper<K> {
		@Override
		public void map(K key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			final Text strippedValue = new Text(value.toString().toLowerCase().replaceAll("\\W+", " "));
			super.map(key, strippedValue, output, reporter);
		}
	}

	public static class NonGenericInputFormat {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(CustomTextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setReducerClass(LongSumReducer.class);
			conf.setCombinerClass((LongSumReducer.class));

			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			StratosphereHadoopJobClient.runJob(conf);
		}

		public static class CustomTextInputFormat extends org.apache.hadoop.mapred.TextInputFormat {
		}
	}

	public static class StringTokenizer {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);


			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			StratosphereHadoopJobClient.runJob(conf);
		}
	}

	public static class WordCountCustomGroupingComparator {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setReducerClass(LongSumReducer.class);
			conf.setCombinerClass((LongSumReducer.class));
			conf.setOutputValueGroupingComparator(FirstLetterComparator.class);

			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			StratosphereHadoopJobClient.runJob(conf);
		}

		//First Letters only.
		public static class FirstLetterComparator extends WritableComparator {

			public FirstLetterComparator() {
				super(Text.class, true);
			}

			@Override
			public int compare(WritableComparable t1, WritableComparable t2) {
				final Text key1 = (Text) t1;
				final IntWritable key1Char = new IntWritable(key1.charAt(0));
				final Text key2 = (Text) t2;
				final IntWritable key2Char = new IntWritable(key2.charAt(0));
				return key1Char.compareTo(key2Char);
			}
		}
	}

	public static class WordCountCustomPartitioner {
		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setReducerClass(LongSumReducer.class);
			conf.setCombinerClass((LongSumReducer.class));
			conf.setPartitionerClass(MyPartitioner.class);
			conf.setNumReduceTasks(5);  //Will be ignored!

			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			StratosphereHadoopJobClient.runJob(conf);
		}


		public static class MyPartitioner<Text, LongWritable> extends HashPartitioner<Text, LongWritable> {

			@Override
			public int getPartition(Text key, LongWritable value, int numReduceTasks) {
				return 0;
			}
		}
	}

	public static class WordCountDifferentCombiner {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setCombinerClass((LongSumReducer.class));
			conf.setReducerClass(TestReducer.class);


			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			StratosphereHadoopJobClient.runJob(conf);
		}

		public static class TestReducer<K> extends LongSumReducer<K> {

			@Override
			public void reduce(K key, Iterator<LongWritable> values, OutputCollector<K,LongWritable> output, Reporter reporter) throws IOException{
				output.collect(key, values.next());
			}

		}
	}

	public static class WordCountNoCombiner {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setReducerClass(LongSumReducer.class);

			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			StratosphereHadoopJobClient.runJob(conf);
		}
	}

	public static class WordCountSameCombiner {

		public static void main(String[] args) throws Exception {
			final String inputPath = args[0];
			final String outputPath = args[1];

			final JobConf conf = new JobConf();

			conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
			org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

			conf.setOutputFormat(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(conf, new Path(outputPath));

			conf.setMapperClass(TestTokenizeMap.class);
			conf.setReducerClass(LongSumReducer.class);
			conf.setCombinerClass((LongSumReducer.class));

			conf.set("mapred.textoutputformat.separator", " ");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			StratosphereHadoopJobClient.runJob(conf);
		}
	}

	public static class DescendingKeyComparator extends WritableComparator {
		protected DescendingKeyComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			LongWritable key1 = (LongWritable) w1;
			LongWritable key2 = (LongWritable) w2;
			return -1 * key1.compareTo(key2);
		}
	}

}
