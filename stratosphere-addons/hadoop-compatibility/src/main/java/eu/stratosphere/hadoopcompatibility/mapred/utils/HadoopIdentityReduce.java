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

package eu.stratosphere.hadoopcompatibility.mapred.utils;

import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;
import org.apache.hadoop.io.Writable;

import java.util.Iterator;

/**
 * A helper reduce function that should be called between successive groupBy calls.
 */
@GroupReduceFunction.Combinable
public class HadoopIdentityReduce<K extends Writable, V extends Writable> extends GroupReduceFunction<Tuple2<K,V>,
		Tuple2<K,V>> {

	@Override
	public void reduce(final Iterator<Tuple2<K, V>> values, final Collector<Tuple2<K, V>> out) throws Exception {
		while (values.hasNext()) {
			out.collect(values.next());
		}
	}
}
