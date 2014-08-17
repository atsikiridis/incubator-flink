package org.apache.flink.hadoopcompatibility.mapred;


import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;

public class FlinkHadoopJobSubmitClient extends LocalJobRunner {

	public FlinkHadoopJobSubmitClient(final JobConf conf) throws IOException {
		super(conf);
	}


	public JobStatus submitJob(JobID jobid, String jobSubmitDir, Credentials credentials) throws IOException {
		throw new UnsupportedOperationException();
	}
}
