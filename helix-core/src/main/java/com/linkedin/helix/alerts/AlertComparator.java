package com.linkedin.helix.alerts;

public abstract class AlertComparator {

	public AlertComparator()
	{
		
	}
	
	public abstract boolean evaluate(Tuple<String> leftTup, Tuple<String> rightTup);
	
}
