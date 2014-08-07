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

package org.apache.flink.api.java.operators;

import java.util.Iterator;

import org.apache.flink.api.common.functions.GenericCombine;
import org.apache.flink.api.common.functions.GenericGroupReduce;
import org.apache.flink.api.common.functions.GenericMap;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.HadoopReduceOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.HadoopReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.HadoopKeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.Collector;

/**
 * This operator represents the application of a "reduceGroup" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class HadoopReduceOperator<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
				extends SingleInputUdfOperator<Tuple2<KEYIN, VALUEIN>, Tuple2<KEYOUT, VALUEOUT>, HadoopReduceOperator<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> {
	
	private final HadoopReduceFunction<KEYIN, VALUEIN, KEYOUT, VALUEOUT> function;
	
	private boolean combinable = false;
	
	
	/**
	 * Constructor for a non-grouped reduce (all reduce).
	 * 
	 * @param input The input data set to the groupReduce function.
	 * @param function The user-defined GroupReduce function.
	 */
	public HadoopReduceOperator(DataSet<Tuple2<KEYIN, VALUEIN>> input, HadoopReduceFunction<KEYIN, VALUEIN, KEYOUT, VALUEOUT> function) {
		super(input, TypeExtractor.getGroupReduceReturnTypes(function, input.getType()));
		
		this.function = function;
		checkCombinability();
	}
	
	private void checkCombinability() {
		
		// TODO check if there is a combiner in the job conf
		
//		if (function instanceof GenericCombine && function.getClass().getAnnotation(Combinable.class) != null) {
//			this.combinable = true;
//		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	public boolean isCombinable() {
		return combinable;
	}
	
	public void setCombinable(boolean combinable) {
		// sanity check that the function is a subclass of the combine interface
		if (combinable && !(function instanceof GenericCombine)) {
			throw new IllegalArgumentException("The function does not implement the combine interface.");
		}
		
		this.combinable = combinable;
	}
	

	
	@Override
	protected org.apache.flink.api.common.operators.base.HadoopReduceOperatorBase<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>, Tuple2<KEYOUT, VALUEOUT>, ?> translateToDataFlow(Operator<Tuple2<KEYIN, VALUEIN>> input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		
		KeySelector<Tuple2<KEYIN, VALUEIN>, Integer> ks = function.getHadoopKeySelector();
		HadoopKeyExtractingMapper<Tuple2<KEYIN, VALUEIN>, Integer> extractor = new HadoopKeyExtractingMapper<Tuple2<KEYIN, VALUEIN>, Integer>(ks);
		
		TypeInformation<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>> typeInfoWithKey = new TupleTypeInfo<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>>(getInputType(), BasicTypeInfo.INT_TYPE_INFO);
		MapOperatorBase<Tuple2<KEYIN, VALUEIN>, Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>, GenericMap<Tuple2<KEYIN, VALUEIN>, Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>>> mapper = 
				new MapOperatorBase<Tuple2<KEYIN, VALUEIN>, Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>, GenericMap<Tuple2<KEYIN, VALUEIN>, Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>>>(extractor, new UnaryOperatorInformation<Tuple2<KEYIN, VALUEIN>, Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>>(getInputType(), typeInfoWithKey), "Key Extractor");

//		public HadoopReduceUnwrappingOperator(GroupReduceFunction<IN, OUT> udf, String name,
//				TypeInformation<OUT> outType, TypeInformation<Tuple2<K, IN>> typeInfoWithKey, boolean combinable)
		
		HadoopReduceUnwrappingOperator<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer = new HadoopReduceUnwrappingOperator<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(function, name, getResultType(), typeInfoWithKey, combinable);
		
		reducer.setInput(mapper);
		mapper.setInput(input);
		
		// set the mapper's parallelism to the input parallelism to make sure it is chained
		mapper.setDegreeOfParallelism(input.getDegreeOfParallelism());
		reducer.setDegreeOfParallelism(this.getParallelism());
		
		return reducer;
	}
	
		
	
	public class HadoopReduceUnwrappingOperator<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
					extends HadoopReduceOperatorBase<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>, Tuple2<KEYOUT, VALUEOUT>, GenericGroupReduce<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>, Tuple2<KEYOUT, VALUEOUT>>> {

		public HadoopReduceUnwrappingOperator(HadoopReduceFunction<KEYIN, VALUEIN, KEYOUT, VALUEOUT> udf, String name,
				TypeInformation<Tuple2<KEYOUT, VALUEOUT>> outType, TypeInformation<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>> typeInfoWithKey, boolean combinable)
		{
//			super(combinable ? new TupleUnwrappingCombinableGroupReducer<IN, OUT, K>(udf) : new TupleUnwrappingNonCombinableGroupReducer<IN, OUT, K>(udf),
//					new UnaryOperatorInTuple2<IN, K>formation<Tuple2<K, IN>, OUT>(typeInfoWithKey, outType), new int[]{1}, name);
			
			super(new TupleUnwrappingNonCombinableGroupReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(udf),
					new UnaryOperatorInformation<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>, Tuple2<KEYOUT, VALUEOUT>>(typeInfoWithKey, outType), new int[]{1}, name);
			
			super.setCombinable(combinable);
			
			this.getParameters().setClass("hadoop.key.class", function.getHadoopReduceInKeyClass());
			this.getParameters().setClass("hadoop.sorting.comparator", function.getHadoopSortComparatorClass());
			this.getParameters().setClass("hadoop.grouping.comparator", function.getHadoopGroupingComparatorClass());
			
		}
	}
		
		// --------------------------------------------------------------------------------------------
		
//	@Combinable
//	public static final class TupleUnwrappingCombinableGroupReducer<IN, OUT, K> extends WrappingFunction<DummyHadoopReduceFunction<IN, OUT>>
//		implements GenericGroupReduce<Tuple2<IN, K>, OUT>, GenericCombine<Tuple2<IN, K>>
//	{
//
//		private static final long serialVersionUID = 1L;
//		
//		private TupleUnwrappingIterator<IN, K> iter;
//		private TupleWrappingCollector<IN, K> coll; 
//		
//		private TupleUnwrappingCombinableGroupReducer(DummyHadoopReduceFunction<IN, OUT> wrapped) {
//			super(wrapped);
//			this.iter = new TupleUnwrappingIterator<IN, K>();
//			this.coll = new TupleWrappingCollector<IN, K>(this.iter);
//		}
//
//		@Override
//		public void reduce(Iterator<Tuple2<IN, K>> values, Collector<OUT> out) throws Exception {
//			iter.set(values);
//			this.wrappedFunction.reduce(iter, out);
//		}
//
//		@Override
//		public void combine(Iterator<Tuple2<IN, K>> values, Collector<Tuple2<IN, K>> out) throws Exception {
//				iter.set(values);
//				coll.set(out);
//				this.wrappedFunction.combine(iter, coll);
//		}
//		
//		@Override
//		public String toString() {
//			return this.wrappedFunction.toString();
//		}
//	}
	
	public static final class TupleUnwrappingNonCombinableGroupReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
		extends WrappingFunction<HadoopReduceFunction<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>
		implements GenericGroupReduce<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>, Tuple2<KEYOUT, VALUEOUT>>
	{
	
		private static final long serialVersionUID = 1L;
		
		private final HadoopTupleUnwrappingIterator<Tuple2<KEYIN, VALUEIN>, Integer> iter;
		
		private TupleUnwrappingNonCombinableGroupReducer(HadoopReduceFunction<KEYIN, VALUEIN, KEYOUT, VALUEOUT> wrapped) {
			super(wrapped);
			this.iter = new HadoopTupleUnwrappingIterator<Tuple2<KEYIN, VALUEIN>, Integer>();
		}
		
		@Override
		public void reduce(Iterator<Tuple2<Tuple2<KEYIN, VALUEIN>, Integer>> values, Collector<Tuple2<KEYOUT, VALUEOUT>> out) throws Exception {
			iter.set(values);
			this.wrappedFunction.reduce(iter, out);
		}
		
		@Override
		public String toString() {
			return this.wrappedFunction.toString();
		}
	}
	
	public static class HadoopTupleUnwrappingIterator<T, K> implements Iterator<T>, java.io.Serializable {

		private static final long serialVersionUID = 1L;
		
		private K lastKey; 
		private Iterator<Tuple2<T, K>> iterator;
		
		public void set(Iterator<Tuple2<T, K>> iterator) {
			this.iterator = iterator;
		}
		
		public K getLastKey() {
			return lastKey;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public T next() {
			Tuple2<T, K> t = iterator.next(); 
			this.lastKey = t.f1;
			return t.f0;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
}	

