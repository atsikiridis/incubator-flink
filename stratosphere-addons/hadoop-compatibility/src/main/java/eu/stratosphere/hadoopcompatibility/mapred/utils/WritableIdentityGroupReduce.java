package eu.stratosphere.hadoopcompatibility.mapred.utils;

import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;
import org.apache.hadoop.io.Writable;

import java.util.Iterator;

public class WritableIdentityGroupReduce<K extends Writable, V extends Writable> extends GroupReduceFunction<Tuple2<K,V>, Tuple2<K,V>> {
	@Override
	public void reduce(final Iterator<Tuple2<K, V>> values, final Collector<Tuple2<K, V>> out) throws Exception {
		while (values.hasNext()) {
			out.collect(values.next());
		}
	}
}
