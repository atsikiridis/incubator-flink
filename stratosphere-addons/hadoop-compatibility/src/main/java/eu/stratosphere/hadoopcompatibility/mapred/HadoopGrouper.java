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

package eu.stratosphere.hadoopcompatibility.mapred;

import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple2;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;


public class HadoopGrouper<K extends WritableComparable,V extends Writable> extends KeySelector<Tuple2<K,V>, K> {

	private RawComparator<K> comparator;
	private JobConf jobConf;
	private List<K> keysToCompareWith;

	public HadoopGrouper(RawComparator<K> comparator) {
		this.comparator = comparator;
		this.jobConf = new JobConf();
		this.keysToCompareWith = new ArrayList<K>();
	}

	@Override
	public K getKey(final Tuple2<K, V> value) {
		final K currentKey = value.f0;
		for(K key: keysToCompareWith) {
			final int result = comparator.compare(key, currentKey);
			if (result == 0) {
				return key;
			}
		}
		keysToCompareWith.add(currentKey);
		return currentKey;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		//jobConf.setOutputValueGroupingComparator(org.apache.hadoop.io.WritableComparator.);
		jobConf.write(out);
		out.writeObject(keysToCompareWith);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		jobConf = new JobConf();
		jobConf.readFields(in);
		try {
			this.comparator = WritableComparator.get(Text.class);
		}catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop grouping comparator", e);
		}
		ReflectionUtils.setConf(comparator, jobConf);
		keysToCompareWith = (List<K>) in.readObject();
	}
}
