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
package eu.stratosphere.test.hadoopcompatibility.mapred.driver;

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
		HadoopWordCountVariations.WordCountCustomGroupingComparator.main(new String[]{textPath, resultPath});
	}

}
