package com.linkedin.clustermanager.alerts;

import java.util.Iterator;
import java.util.List;

public class SumAllOperator extends Operator {

	public SumAllOperator() {
		minInputTupleLists = 1;
		maxInputTupleLists = Integer.MAX_VALUE;
		inputOutputTupleListsCountsEqual = true;
		numOutputTupleLists = -1;
	}

	@Override
	public List<Iterator<Tuple<String>>> execute(List<Iterator<Tuple<String>>> input) {
		// TODO Auto-generated method stub
		return null;
	}

}
