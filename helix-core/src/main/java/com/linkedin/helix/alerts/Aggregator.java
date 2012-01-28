package com.linkedin.helix.alerts;

public abstract class Aggregator {

	int _numArgs;
	
	public Aggregator()
	{
		
	}
	
	/*
	 * Take curr and new values.  Update curr.
	 */
	public abstract void merge(Tuple<String> currVal, Tuple<String> newVal, 
			Tuple<String> currTime, Tuple<String> newTime, String... args);
	
	public int getRequiredNumArgs()
	{
		return _numArgs;
	}
	
}
