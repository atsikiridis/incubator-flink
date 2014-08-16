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


package org.apache.flink.hadoopcompatibility.mapred.wrapper;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.util.Progress;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a dummy progress monitor / reporter
 *
	*/
	public class HadoopReporter extends StatusReporter implements Reporter {

	private static final int PROGRESS_STATUS_LEN_LIMIT = 512;

	private Progress progress;

	private AtomicBoolean progressFlag = new AtomicBoolean(false);
	private transient Counters counters;

	public HadoopReporter() {}

	public HadoopReporter(RuntimeContext context) {
		init(context);
	}

	public void init(RuntimeContext context) {
		this.counters = new FlinkHadoopCounters(context);
		this.progress = new Progress();
	}

	private void setProgressFlag() {
		progressFlag.set(true);
	}

	@Override
	public void progress() {
		// indicate that progress update needs to be sent log warning

	}
	@Override
	public void setStatus(String status) {
		if (status.length() > PROGRESS_STATUS_LEN_LIMIT) {
			status = status.substring(0, PROGRESS_STATUS_LEN_LIMIT);
		}
		progress.setStatus(status);
		// indicate that progress update needs to be sent
		setProgressFlag();
	}

	@Override
	public Counter getCounter(Enum<?> name) {
		return counters == null ? null : counters.findCounter(name);
	}

	@Override
	public Counter getCounter(String group, String name) {
		Counters.Counter counter = null;
		if (counters != null) {
			counter = counters.findCounter(group, name);
		}
		return counter;
	}

	@Override
	public void incrCounter(Enum<?> key, long amount) {
		if (counters != null) {
			counters.findCounter(key).increment(1);
		}
	}

	@Override
	public void incrCounter(String group, String counter, long amount) {
		if (counters != null) {
			counters.incrCounter(group, counter, amount);
		}
	}

	@Override
	public InputSplit getInputSplit() throws UnsupportedOperationException {
		return null;
	}

	@Override
	public float getProgress() {
		return this.progress.getProgress();
	}
}
