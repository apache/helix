/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.alerts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SumEachOperator extends Operator {

	public SumEachOperator() {
		minInputTupleLists = 1;
		maxInputTupleLists = Integer.MAX_VALUE;
		inputOutputTupleListsCountsEqual = true;
		numOutputTupleLists = -1;
	}

	//for each column, generate sum
	@Override
	public List<Iterator<Tuple<String>>> execute(List<Iterator<Tuple<String>>> input) {
		List<Iterator<Tuple<String>>> out = new ArrayList<Iterator<Tuple<String>>>();
		for (Iterator<Tuple<String>> currIt : input) {
			Tuple<String> currSum = null;
			while (currIt.hasNext()) {
				currSum = sumTuples(currSum, currIt.next());
			}
			ArrayList<Tuple<String>> currOutList = new ArrayList<Tuple<String>>();
			currOutList.add(currSum);
			out.add(currOutList.iterator());
		}
		return out;
	}

}
