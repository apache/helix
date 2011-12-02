package com.linkedin.clustermanager.alerts;

public abstract class Aggregator {

	public Aggregator()
	{
		
	}
	
	public abstract Tuple merge(Tuple oldVal, Tuple newVal, String... args);
	 
}
