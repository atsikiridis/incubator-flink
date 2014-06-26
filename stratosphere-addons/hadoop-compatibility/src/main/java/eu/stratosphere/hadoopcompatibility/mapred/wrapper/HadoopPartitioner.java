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

package eu.stratosphere.hadoopcompatibility.mapred.wrapper;

import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * The wrapper of Hadoop's Partitioner interface.
 */


public class HadoopPartitioner<K2 extends Writable,V2 extends Writable> extends KeySelector<Tuple2<K2,V2>, Integer>  {

	private Partitioner<K2,V2> partitioner;
	private int noOfReduceTasks;
	private JobConf jobConf;

	public HadoopPartitioner(Partitioner<K2, V2> partitioner, int noOfReduceTasks) {
		this.partitioner = partitioner;
		this.noOfReduceTasks = noOfReduceTasks;
		this.jobConf = new JobConf();
	}

	@Override
	public Integer getKey(final Tuple2<K2, V2> value) {
		return this.partitioner.getPartition(value.f0,value.f1, this.noOfReduceTasks);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		jobConf.setPartitionerClass(partitioner.getClass());
		jobConf.write(out);
		out.writeObject(this.noOfReduceTasks);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		jobConf = new JobConf();
		jobConf.readFields(in);
		try {
			this.partitioner = InstantiationUtil.instantiate(this.jobConf.getPartitionerClass());
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop partitioner", e);
		}
		ReflectionUtils.setConf(partitioner, jobConf);
		this.noOfReduceTasks = (Integer) in.readObject();
	}
}
