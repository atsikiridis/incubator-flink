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

package org.apache.flink.api.java.typeutils.runtime;

import java.io.IOException;
import java.util.Comparator;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;

import com.esotericsoftware.kryo.Kryo;

public class HadoopComparatorWrapper<T extends Writable> extends TypeComparator<Tuple2<Tuple2<T,?>,?>> {
		
	private static final long serialVersionUID = 1L;
	
	private Class<T> type;
	
	private Class<Comparator<T>> comparatorType;
	
	private transient Comparator<T> writableComparator;
	
	private transient T reference;
	
	private transient T tempReference;
	
	private transient Kryo kryo;

	public HadoopComparatorWrapper() { }
	
	public HadoopComparatorWrapper(Class<Comparator<T>> comparatorType, Class<T> type) {
		this.comparatorType = comparatorType;
		this.type = type;
		
		this.writableComparator = InstantiationUtil.instantiate(comparatorType); 
	}
	
	@Override
	public int hash(Tuple2<Tuple2<T,?>,?> record) {
		return record.f0.f0.hashCode();
	}
	
	@Override
	public void setReference(Tuple2<Tuple2<T,?>,?> toCompare) {
		checkKryoInitialized();
		reference = this.kryo.copy(toCompare.f0.f0);
	}
	
	@Override
	public boolean equalToReference(Tuple2<Tuple2<T,?>,?> candidate) {
		return writableComparator.compare(candidate.f0.f0, reference) == 0;
	}
	
	@Override
	public int compareToReference(TypeComparator<Tuple2<Tuple2<T,?>,?>> referencedComparator) throws ClassCastException {
		final T otherRef = ((HadoopComparatorWrapper<T>) referencedComparator).reference;
		return this.writableComparator.compare(otherRef, reference);
	}
	
	@Override
	public int compare(Tuple2<Tuple2<T,?>,?> first, Tuple2<Tuple2<T,?>,?> second) throws ClassCastException {
		return this.writableComparator.compare(first.f0.f0, second.f0.f0);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException, ClassCastException {
		ensureReferenceInstantiated();
		ensureTempReferenceInstantiated();
					
		reference.readFields(firstSource);
		tempReference.readFields(secondSource);

		return writableComparator.compare(reference, tempReference);
	}

	
	@Override
	public boolean supportsNormalizedKey() {
		return false;
	}
	
	@Override
	public int getNormalizeKeyLen() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void putNormalizedKey(Tuple2<Tuple2<T,?>,?> record, MemorySegment target, int offset, int numBytes) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean invertNormalizedKey() {
		return false;
	}
	
	@Override
	public TypeComparator<Tuple2<Tuple2<T,?>,?>> duplicate() {
		return new HadoopComparatorWrapper<T>(comparatorType, type);
	}

	@Override
	public int extractKeys(final Object record, final Object[] target, final int index) {
		return 0;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return new TypeComparator[0];
	}

	// --------------------------------------------------------------------------------------------
	// unsupported normalization
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}
	
	@Override
	public void writeWithKeyNormalization(Tuple2<Tuple2<T,?>,?> record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Tuple2<Tuple2<T,?>,?> readWithKeyDenormalization(Tuple2<Tuple2<T,?>,?> reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	// --------------------------------------------------------------------------------------------
	
	private final void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(type);
		}
	}
	
	private final void ensureReferenceInstantiated() {
		if (reference == null) {
			reference = InstantiationUtil.instantiate(type, Writable.class);
		}
	}
	
	private final void ensureTempReferenceInstantiated() {
		if (tempReference == null) {
			tempReference = InstantiationUtil.instantiate(type, Writable.class);
		}
	}
	
	public static class HadoopComparatorFactory implements TypeComparatorFactory<Tuple2<Tuple2<Writable,?>,?>> {

		private Class<Writable> type;
		private Class<Comparator<Writable>> comparatorType;
		
		public HadoopComparatorFactory() {}
		
		public HadoopComparatorFactory(Class<Writable> type, Class<Comparator<Writable>> comparatorType) {
			this.type = type;
			this.comparatorType = comparatorType;
		}
		
		@Override
		public void writeParametersToConfig(Configuration config) {
			
			config.setClass("hadoop.comparator.type", type);
			config.setClass("hadoop.comparator.comparatortype", comparatorType);
		}

		@Override
		public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
			this.type = config.getClass("hadoop.comparator.type", null, cl);
			this.comparatorType = config.getClass("hadoop.comparator.comparatortype", null, cl);
		}

		@Override
		public TypeComparator<Tuple2<Tuple2<Writable,?>,?>> createComparator() {
			return new HadoopComparatorWrapper<Writable>(comparatorType, type);
		}
		
			
	}
		
}