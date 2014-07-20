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

package org.apache.flink.test.hadoopcompatibility.mapred.driver;

import org.apache.flink.test.hadoopcompatibility.HadoopTestBase;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;

public class HadoopDriverCustomGroupingComparatorITCase extends HadoopTestBase {

	protected String textPath;
	protected String resultPath;

	private static final String RESULT = "a 1\n" +
			"b 1\n" +
			"d 6\n" +
			"e 2\n" +
			"f 1\n" +
			"g 1\n" +
			"h 4\n" +
			"i 2\n" +
			"m 1\n" +
			"n 2\n" +
			"p 1\n" +
			"r 1\n" +
			"s 1\n" +
			"t 4\n" +
			"v 1\n" +
			"w 2";

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
		HadoopWordCountVariations.WordCountCustomGroupingComparator.main(new String[]{textPath, resultPath});
	}

	public void compareResultsByLinesInMemory(String expectedResultStr, String resultPath) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, false);

		String[] result = list.toArray(new String[list.size()]);

		String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");
		Arrays.sort(expected);

		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);

		for (int i=0; i< result.length; i++) {
			result[i] = result[i].replace(result[i].substring(1, result[i].indexOf(" ")), "");
		}
		Assert.assertArrayEquals(expected, result);
	}
}


