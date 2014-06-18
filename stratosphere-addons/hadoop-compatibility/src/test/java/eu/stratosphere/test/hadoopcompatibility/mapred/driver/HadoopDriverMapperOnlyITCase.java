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

import eu.stratosphere.hadoopcompatibility.mapred.example.driver.StringTokenizer;
import eu.stratosphere.hadoopcompatibility.mapred.example.driver.WordCountDifferentCombiner;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.JavaProgramTestBase;

public class HadoopDriverMapperOnlyITCase extends JavaProgramTestBase {

	protected String textPath;
	protected String resultPath;


	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.TEXT, resultPath + "/1");
	}

	@Override
	protected void testProgram() throws Exception {
		StringTokenizer.main(new String[]{textPath, resultPath});
	}
}
