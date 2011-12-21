package com.linkedin.clustermanager.alerts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiplyOperator extends Operator {

	public MultiplyOperator() {
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
			Tuple<String> rowProduct = null;
			for (Iterator<Tuple<String>> it : input) {
				if (!it.hasNext()) { //when any iterator runs out, we are done
					return singleSetToIter(output);
				}
				rowProduct = multiplyTuples(rowProduct, it.next());
			}
			output.add(rowProduct);
		}
	}

}
