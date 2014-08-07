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


package org.apache.flink.runtime.operators.drivers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.GenericGroupReduce;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.GroupReduceDriver;
import org.apache.flink.runtime.operators.HadoopReduceDriver;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.runtime.util.RegularToMutableObjectIterator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("serial")
public class HadoopReduceDriverTest {

//	@Test
	public void testAllReduceDriverImmutableEmpty() {
		try {
			TestTaskContext<GenericGroupReduce<Tuple3<IntWritable, String, Integer>, Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GenericGroupReduce<Tuple3<IntWritable, String, Integer>, Tuple2<String, Integer>>, Tuple2<String,Integer>>(16*1024*1024, true);
			
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TupleTypeInfo<Tuple2<String, Integer>> typeInfo = (TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();
			TypeComparator<Tuple2<String, Integer>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
			context.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
			
			
			context.setInput1(input, typeInfo.createSerializer());
			context.setComparator1(comparator);
			context.setCollector(new DiscardingOutputCollector<Tuple2<String, Integer>>());
			
			HadoopReduceDriver<Tuple3<IntWritable, String, Integer>, Tuple2<String, Integer>> driver = new HadoopReduceDriver<Tuple3<IntWritable, String, Integer>, Tuple2<String, Integer>>();
			driver.setup(context);
			driver.prepare();
			driver.run();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
//	@Test
	public void testAllReduceDriverImmutable() {
		try { 
			TestTaskContext<GenericGroupReduce<Tuple2<Tuple2<IntWritable, String>, Integer>, Tuple2<IntWritable, String>>, Tuple2<IntWritable, String>> context =
					new TestTaskContext<GenericGroupReduce<Tuple2<Tuple2<IntWritable, String>, Integer>, Tuple2<IntWritable, String>>, Tuple2<IntWritable,String>>(16*1024*1024, true);
			
			List<Tuple2<Tuple2<IntWritable, String>, Integer>> data = new ArrayList<Tuple2<Tuple2<IntWritable, String>, Integer>>();
			
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(6), "f"), 11));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(6), "f"), 12));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(2), "b"), 2));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(1), "a"), 1));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(3), "c"), 3));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(4), "d"), 4));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(5), "e"), 6));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(4), "d"), 5));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(5), "e"), 7));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(5), "e"), 8));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(6), "f"), 9));
			data.add(new Tuple2<Tuple2<IntWritable, String>, Integer>(new Tuple2<IntWritable, String>(new IntWritable(6), "f"), 10));
			
			
			TupleTypeInfo<Tuple2<Tuple2<IntWritable, String>, Integer>> inTypeInfo = (TupleTypeInfo<Tuple2<Tuple2<IntWritable, String>, Integer>>) TypeExtractor.getForObject(data.get(0));
			TupleTypeInfo<Tuple2<IntWritable, String>> outTypeInfo = (TupleTypeInfo<Tuple2<IntWritable, String>>) TypeExtractor.getForObject(new Tuple2<IntWritable, String>(new IntWritable(1),""));
			MutableObjectIterator<Tuple2<Tuple2<IntWritable, String>, Integer>> input = new RegularToMutableObjectIterator<Tuple2<Tuple2<IntWritable, String>, Integer>>(data.iterator(), inTypeInfo.createSerializer());
			TypeComparator<Tuple2<String, Integer>> comparator = null;
			
			GatheringCollector<Tuple2<IntWritable, String>> result = new GatheringCollector<Tuple2<IntWritable,String>>(outTypeInfo.createSerializer());
			
			context.setDriverStrategy(DriverStrategy.NONE);
			context.setInput1(input, inTypeInfo.createSerializer());
			context.setCollector(result);
			context.setComparator1(comparator);
			context.setUdf(new MyRF());
			
			context.getTaskConfig().getStubParameters().setClass("hadoop.key.class", (new IntWritable(1)).getClass());
			context.getTaskConfig().getStubParameters().setClass("hadoop.sorting.comparator", (new IntWritableSortComp()).getClass());
			context.getTaskConfig().getStubParameters().setClass("hadoop.grouping.comparator", (new IntWritableGroupComp()).getClass());
			context.getTaskConfig().setFilehandlesDriver(32);
			context.getTaskConfig().setRelativeMemoryDriver(0.5);
			context.getTaskConfig().setSpillingThresholdDriver(0.7f);
	
			HadoopReduceDriver<Tuple2<Tuple2<IntWritable, String>, Integer>, Tuple2<IntWritable, String>> driver = new HadoopReduceDriver<Tuple2<Tuple2<IntWritable, String>, Integer>, Tuple2<IntWritable, String>>();
			driver.setup(context);
			driver.prepare();
			driver.run();
			
			Object[] res = result.getList().toArray();
			
			for(Object r : res) {
				System.out.println(r);
			}
			
			Object[] expected = DriverTestData.createReduceImmutableDataGroupedResult().toArray();
			
			DriverTestData.compareTupleArrays(expected, res);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
//	@Test
	public void testAllReduceDriverMutable() {
		try {
			TestTaskContext<GenericGroupReduce<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>> context =
					new TestTaskContext<GenericGroupReduce<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>>();
			
			List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
			TupleTypeInfo<Tuple2<StringValue, IntValue>> typeInfo = (TupleTypeInfo<Tuple2<StringValue, IntValue>>) TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<StringValue, IntValue>> input = new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(data.iterator(), typeInfo.createSerializer());
			TypeComparator<Tuple2<StringValue, IntValue>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
			
			GatheringCollector<Tuple2<StringValue, IntValue>> result = new GatheringCollector<Tuple2<StringValue, IntValue>>(typeInfo.createSerializer());
			
			context.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
			context.setInput1(input, typeInfo.createSerializer());
			context.setComparator1(comparator);
			context.setCollector(result);
			context.setUdf(new ConcatSumMutableReducer());
			
			GroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> driver = new GroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>();
			driver.setup(context);
			driver.prepare();
			driver.run();
			
			Object[] res = result.getList().toArray();
			Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();
			
			DriverTestData.compareTupleArrays(expected, res);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	

	
	// --------------------------------------------------------------------------------------------
	//  Test UDFs
	// --------------------------------------------------------------------------------------------
	
	public static final class ConcatSumReducer extends GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public void reduce(Iterator<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) {
			Tuple2<String, Integer> current = values.next();
			
			while (values.hasNext()) {
				Tuple2<String, Integer> next = values.next();
				next.f0 = current.f0 + next.f0;
				next.f1 = current.f1 + next.f1;
				current = next;
			}
			
			out.collect(current);
		}
	}
	
	public static final class ConcatSumMutableReducer extends GroupReduceFunction<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> {

		@Override
		public void reduce(Iterator<Tuple2<StringValue, IntValue>> values, Collector<Tuple2<StringValue, IntValue>> out) {
			Tuple2<StringValue, IntValue> current = values.next();
			
			while (values.hasNext()) {
				Tuple2<StringValue, IntValue> next = values.next();
				next.f0.append(current.f0);
				next.f1.setValue(current.f1.getValue() + next.f1.getValue());
				current = next;
			}
			
			out.collect(current);
		}
	}
	
	public static final class MyRF extends GroupReduceFunction<Tuple2<Tuple2<IntWritable, String>, Integer>, Tuple2<IntWritable, String>> {

		@Override
		public void reduce(Iterator<Tuple2<Tuple2<IntWritable, String>, Integer>> values,
				Collector<Tuple2<IntWritable, String>> out) throws Exception {
			int sum = 0;
			String str = "";
			while(values.hasNext()) {
				Tuple2<Tuple2<IntWritable, String>, Integer> t = values.next();
				sum += t.f0.f0.get();
				str += t.f0.f1;
			}
			
			out.collect(new Tuple2<IntWritable, String>(new IntWritable(sum), str));
			
		}
	}
	
	public static final class IntWritableSortComp implements Comparator<IntWritable> {

		@Override
		public int compare(IntWritable o1, IntWritable o2) {
			return 0; // o1.get() - o2.get();
		}
		
	}
	
	public static final class IntWritableGroupComp implements Comparator<IntWritable> {

		@Override
		public int compare(IntWritable o1, IntWritable o2) {
			return o1.get() - o2.get();
		}
		
	}
}
