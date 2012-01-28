package com.linkedin.helix.alerts;

import java.util.Iterator;
import java.util.List;

public class ExpandOperator extends Operator {

	public ExpandOperator() {
		minInputTupleLists = 1;
		maxInputTupleLists = Integer.MAX_VALUE;
		inputOutputTupleListsCountsEqual = true;
	}

	@Override
	public List<Iterator<Tuple<String>>> execute(List<Iterator<Tuple<String>>> input) {
		//TODO: confirm this is a no-op operator
		return input;
	}

}
