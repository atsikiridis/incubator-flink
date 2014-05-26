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

import java.util.Map;

import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopOutputCollector;
import org.apache.hadoop.mapred.JobConf;

import eu.stratosphere.runtime.fs.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Merge HadoopConfiguration into JobConf. This is necessary for the HDFS configuration.
 */
public class HadoopConfiguration {
	public static void mergeHadoopConf(JobConf jobConf) {
		org.apache.hadoop.conf.Configuration hadoopConf = DistributedFileSystem.getHadoopConfiguration();
		for (Map.Entry<String, String> e : hadoopConf) {
			jobConf.set(e.getKey(), e.getValue());
		}
	}

	public static void setOutputCollectorToConf(Class<? extends HadoopOutputCollector> outputClass, JobConf jobConf) {
		jobConf.getClass("stratosphere.collector", outputClass, HadoopOutputCollector.class );
	}

	/**
	 * Setters and getters for objects that follow Hadoop's serialization.
	 * Generally, these should be called by the writeObject and readObject methods of the object
	 * they are contained as fields. For an example, see the HadoopMapFunction.
	 */
	@SuppressWarnings("unchecked")
	public static Class<? extends HadoopOutputCollector> getOutputCollectorFromConf(JobConf jobConf) {
		return  (Class<? extends HadoopOutputCollector>) jobConf.getClass("stratosphere.collector",
				HadoopOutputCollector.class, OutputCollector.class);
	}

	public static void setReporterToConf(Class<? extends Reporter> reporterClass, JobConf jobConf) {
		jobConf.getClass("stratosphere.reporter", reporterClass, Reporter.class );
	}

	public static Class<? extends Reporter> getReporterFromConf(JobConf jobConf) {
		return  jobConf.getClass("stratosphere.collector", HadoopDummyReporter.class, Reporter.class );
	}
}