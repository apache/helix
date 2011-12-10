package com.linkedin.clustermanager.alerts;

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
