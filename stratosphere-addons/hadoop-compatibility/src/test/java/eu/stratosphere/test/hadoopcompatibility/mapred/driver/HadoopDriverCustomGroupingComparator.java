package eu.stratosphere.test.hadoopcompatibility.mapred.driver;

import eu.stratosphere.hadoopcompatibility.mapred.example.driver.WordCountCustomGroupingComparator;
import eu.stratosphere.test.hadoopcompatibility.HadoopTestBase;
import eu.stratosphere.test.testdata.WordCountData;
import org.apache.commons.lang3.StringUtils;

public class HadoopDriverCustomGroupingComparator extends HadoopTestBase {

	protected String textPath;
	protected String resultPath;

	private static final String RESULT = "alter 1\n" +
			"brudersphaeren 1\n" +
			"drei 6\n" +
			"erzengel 2\n" +
			"faust 1\n" +
			"goethe 1\n" +
			"himmel 4\n" +
			"in 2\n" +
			"mephistopheles 1\n" +
			"nachher 2\n" +
			"prolog 1\n" +
			"raphael 1\n" +
			"sonne 1\n" +
			"treten 4\n" +
			"vor 1\n" +
			"weise 2";

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT.substring(0,
				StringUtils.ordinalIndexOf(WordCountData.TEXT, "\n", 5)));
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath + "/1");
	}

	@Override
	protected void testProgram() throws Exception {
		WordCountCustomGroupingComparator.main(new String[]{textPath, resultPath});
	}

}
