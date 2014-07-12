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

import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.operators.ReduceGroupOperator;
import eu.stratosphere.api.java.operators.SortedGrouping;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopGrouper;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopMapFunction;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopReduceFunction;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopPartitioner;
import eu.stratosphere.hadoopcompatibility.mapred.utils.HadoopIdentityReduce;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopInputFormat;
import eu.stratosphere.hadoopcompatibility.mapred.HadoopOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

import java.io.IOException;

/**
 * Implements a Hadoop wordcount on Stratosphere with all business logic code in Hadoop.
 * This example shows how a simple hadoop job can be run on Stratosphere.
 */
public class FullWordCount {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: FulllWordCount <input path> <result path>");
			return;
		}
		final String inputPath = args[0];
		final String outputPath = args[1];

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);

		//Hadoop's job configuration
		final JobConf hadoopJobConf = new JobConf();

		// Set up the Hadoop Input Format
		final HadoopInputFormat<LongWritable, Text> hadoopInputFormat = new HadoopInputFormat<LongWritable,
				Text>(new TextInputFormat(),LongWritable.class, Text.class, hadoopJobConf);
		TextInputFormat.addInputPath(hadoopInputFormat.getJobConf(), new Path(inputPath));

		// Create a Stratosphere job with it
		final DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopInputFormat);

		//Set the mapper implementation to be used.
		final Mapper mapper = InstantiationUtil.instantiate(TestTokenizeMap.class,
				Mapper.class);
		final DataSet<Tuple2<Text, LongWritable>> words = text.flatMap( new HadoopMapFunction<LongWritable,Text,
				Text, LongWritable>(mapper, Text.class, LongWritable.class));

		// Partitioning with the HashPartitioner.
		final int noOfPartitions =  hadoopJobConf.getNumReduceTasks();
		final UnsortedGrouping<Tuple2<Text, LongWritable>> partitioning = words.
				groupBy(new HadoopPartitioner<Text, LongWritable>(new HashPartitioner(), noOfPartitions));

		//An Identity reducer between a second groupBy operation, which represent the grouping of values for reducers.
		final GroupReduceFunction<Tuple2<Text, LongWritable>, Tuple2<Text, LongWritable>> identityRedFunction = new HadoopIdentityReduce<Text, LongWritable>();
		final ReduceGroupOperator<Tuple2<Text, LongWritable>, Tuple2<Text,LongWritable>> identityReduce = partitioning.reduceGroup(identityRedFunction);

		//Grouping values using Hadoop's GroupingComparator.
		final RawComparator<Text> hadoopComparator = WritableComparator.get(Text.class);
		final HadoopGrouper<Text,LongWritable> comparator = new HadoopGrouper<Text,LongWritable>(hadoopComparator,
				Text.class);
		final SortedGrouping<Tuple2<Text,LongWritable>> identityResult = identityReduce.groupBy((KeySelector) comparator)
				.sortGroup(0, Order.ASCENDING);

		//Specifying the reducer.
		final Reducer<Text,LongWritable,Text,LongWritable> reducer = InstantiationUtil.instantiate(LongSumReducer.class,
				Reducer.class);
		final ReduceGroupOperator<Tuple2<Text,LongWritable>,Tuple2<Text,LongWritable>> set = identityResult.
				reduceGroup(new HadoopReduceFunction<Text, LongWritable, Text, LongWritable>(reducer, Text.class,
						LongWritable.class));

		//And the OutputFormat
		final TextOutputFormat<Text, LongWritable> outputFormat = new TextOutputFormat<Text, LongWritable>();
		final HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat =
				new HadoopOutputFormat<Text, LongWritable>(outputFormat, hadoopJobConf);
		hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");
		TextOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path(outputPath));

		// Output & Execute
		set.output(hadoopOutputFormat);
		env.execute("Full WordCount");
	}

	public static class TestTokenizeMap<K> extends TokenCountMapper<K> {
		@Override
		public void map(K key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			final Text strippedValue = new Text(value.toString().toLowerCase().replaceAll("\\W+", " "));
			super.map(key, strippedValue, output, reporter);
		}
	}
}
