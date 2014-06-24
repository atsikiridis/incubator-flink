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
package eu.stratosphere.test.hadoopcompatibility.mapred;

import eu.stratosphere.hadoopcompatibility.mapred.example.FullWordCount;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.JavaProgramTestBase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;

public class HadoopFullJobITCase extends JavaProgramTestBase {

	protected String textPath;
	protected String resultPath;

	public void compareResultsByLinesInMemory(String expectedResultStr, String resultPath) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, false);

		String[] result = list.toArray(new String[list.size()]);
		//Arrays.sort(result);

		String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");
		Arrays.sort(expected);

		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
		Assert.assertArrayEquals(expected, result);
	}

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
		FullWordCount.main(new String[]{textPath, resultPath});
	}
}