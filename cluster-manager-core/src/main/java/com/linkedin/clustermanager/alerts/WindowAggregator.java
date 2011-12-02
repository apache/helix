package com.linkedin.clustermanager.alerts;

public class WindowAggregator extends Aggregator {

	
	int _windowSize;
	
	public WindowAggregator(String windowSize) 
	{
		_windowSize = Integer.parseInt(windowSize);
		_numArgs = 1;
	}
	
	public WindowAggregator()
	{
		this("1");
	}
	
	@Override
	public Tuple merge(Tuple oldVal, Tuple newVal, String... args) {
		// TODO Auto-generated method stub
		return null;
	}

	
}
