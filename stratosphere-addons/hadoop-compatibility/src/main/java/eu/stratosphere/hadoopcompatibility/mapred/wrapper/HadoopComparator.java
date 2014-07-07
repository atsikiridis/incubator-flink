package eu.stratosphere.hadoopcompatibility.mapred.wrapper;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;


/**
 * A wrapper for Hadoop Comparators ( implementing the RawComparator Interface).
 */
public class HadoopComparator<T extends Writable> implements Comparator<T>, Serializable {

	private RawComparator<T> comparator;
	private JobConf hadoopJobConf;

	public HadoopComparator(RawComparator<T> comparator) {
		this.comparator = comparator;
		this.hadoopJobConf = new JobConf();
	}

	@Override
	public int compare(final T t, final T t2) {
		return comparator.compare(t,t2);
	}

	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		hadoopJobConf.setOutputKeyComparatorClass(this.comparator.getClass());
		hadoopJobConf.write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		hadoopJobConf = new JobConf();
		hadoopJobConf.readFields(in);
		comparator = hadoopJobConf.getOutputKeyComparator();

	}
}
