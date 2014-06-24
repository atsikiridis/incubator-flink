package eu.stratosphere.test.hadoopcompatibility;

import eu.stratosphere.test.util.JavaProgramTestBase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;


public abstract class HadoopTestBase extends JavaProgramTestBase {

	/**
	 * Hadoop tests should not sort the result.
	 */
	public void compareResultsByLinesInMemory(String expectedResultStr, String resultPath) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, false);

		String[] result = list.toArray(new String[list.size()]);
		String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");
		Arrays.sort(expected);

		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
		Assert.assertArrayEquals(expected, result);
	}

}
