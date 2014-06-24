package eu.stratosphere.test.hadoopcompatibility.mapred.driver;

import eu.stratosphere.hadoopcompatibility.mapred.example.driver.WordCountCustomPartitioner;
import eu.stratosphere.hadoopcompatibility.mapred.example.driver.WordCountDifferentCombiner;
import eu.stratosphere.test.hadoopcompatibility.HadoopTestBase;
import eu.stratosphere.test.testdata.WordCountData;

public class HadoopDriverCustomPartitioner extends HadoopTestBase {

	protected String textPath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath + "/1");
	}

	@Override
	protected void testProgram() throws Exception {
		WordCountCustomPartitioner.main(new String[]{textPath, resultPath});
	}
}
