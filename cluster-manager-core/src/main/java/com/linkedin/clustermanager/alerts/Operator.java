package com.linkedin.clustermanager.alerts;

import java.util.Iterator;
import java.util.List;

public abstract class Operator {
	
	public int minInputTupleLists;
	public int maxInputTupleLists;
	public int numOutputTupleLists = -1;
	public boolean inputOutputTupleListsCountsEqual = false;
	
	public Operator()
	{
		
	}
	
	public abstract List<Iterator<Tuple<String>>> execute(List<Iterator<Tuple<String>>> input);
}
