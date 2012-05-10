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

public class SumOperator extends Operator {

	public SumOperator() {
		minInputTupleLists = 1;
		maxInputTupleLists = Integer.MAX_VALUE;
		inputOutputTupleListsCountsEqual = false;
		numOutputTupleLists = 1;
	}

	
	public List<Iterator<Tuple<String>>> singleSetToIter(ArrayList<Tuple<String>> input) 
	{
		List out = new ArrayList();
		out.add(input.iterator());
		return out;
	}
	
	@Override
	public List<Iterator<Tuple<String>>> execute(List<Iterator<Tuple<String>>> input) {
		ArrayList<Tuple<String>> output = new ArrayList<Tuple<String>>();
		if (input == null || input.size() == 0) {
			return singleSetToIter(output);
		}
		while (true) { //loop through set of iters, return when 1 runs out (not completing the row in progress)
			Tuple<String> rowSum = null;
			for (Iterator<Tuple<String>> it : input) {
				if (!it.hasNext()) { //when any iterator runs out, we are done
					return singleSetToIter(output);
				}
				rowSum = sumTuples(rowSum, it.next());
			}
			output.add(rowSum);
		}
	}
}
