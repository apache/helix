package com.linkedin.clustermanager.alerts;

public abstract class Aggregator {

	int _numArgs;
	
	public Aggregator()
	{
		
	}
	
	public abstract Tuple merge(Tuple oldVal, Tuple newVal, String... args);

	public int getRequiredNumArgs()
	{
		return _numArgs;
	}
	
}
