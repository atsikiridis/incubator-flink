package org.apache.flink.hadoopcompatibility.mapred;


public class HadoopCustomClassLoader  extends ClassLoader {


	public HadoopCustomClassLoader(ClassLoader parent) {
		super(parent);
	}


	protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		Class clazz;
		if (name.equals("org.apache.hadoop.mapred.JobClient")) {
			clazz = FlinkHadoopJobClient.class;
		}
		else {
			clazz = findClass(name);
		}

		if (resolve) {
			resolveClass(clazz);
		}

		return clazz;

	}
}