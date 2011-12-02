package com.linkedin.clustermanager.alerts;

import java.util.Iterator;

public abstract class Operator {
	
	public static int minInputTupleWidth;
	public static int maxInputTupleWidth;
	public static int minOutputTupleWidth;
	public static int maxOutputTupleWidth;
	
	public Operator()
	{
		
	}
	
	public abstract Iterator<Tuple> execute(Iterator<Tuple> input);
}
