package eu.stratosphere.hadoopcompatibility.mapred.example.driver;

import eu.stratosphere.hadoopcompatibility.mapred.StratosphereHadoopJobClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;

import java.io.IOException;
import java.util.Comparator;


public class WordCountCustomGroupingComparator {

	public static void main(String[] args) throws Exception{
		final String inputPath = args[0];
		final String outputPath = args[1];

		final JobConf conf = new JobConf();

		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		org.apache.hadoop.mapred.TextInputFormat.addInputPath(conf, new Path(inputPath));

		conf.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(TestTokenizeMap.class);
		conf.setReducerClass(LongSumReducer.class);
		conf.setCombinerClass((LongSumReducer.class));
		conf.setOutputValueGroupingComparator(FirstLetterComparator.class);

		conf.set("mapred.textoutputformat.separator", " ");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		StratosphereHadoopJobClient.runJob(conf);
	}

	//First Letters only.
	public static class FirstLetterComparator extends WritableComparator {

		public FirstLetterComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable t1, WritableComparable t2) {
			final Text key1 = (Text) t1;
			final IntWritable key1Char = new IntWritable(key1.charAt(0));
			final Text key2 = (Text) t2;
			final IntWritable key2Char = new IntWritable(key2.charAt(0));
			return key1Char.compareTo(key2Char);
		}
	}

	public static class TestTokenizeMap<K> extends TokenCountMapper<K> {
		@Override
		public void map(K key, Text value, OutputCollector<Text, LongWritable> output,
		                Reporter reporter) throws IOException {
			final Text strippedValue = new Text(value.toString().toLowerCase().replaceAll("\\W+", " "));
			super.map(key, strippedValue, output, reporter);
		}
	}

}
