package com.linkedin.clustermanager.alerts;

public class Stat {
	String _name;
	Tuple<String> _value;
	Tuple<String> _timestamp;
	
	public Stat(String name, Tuple<String> value, Tuple<String> timestamp)
	{
		_name = name;
		_value = value;
		_timestamp = timestamp;
	}
	
	public String getName()
	{
		return _name;
	}
	
	public Tuple<String> getValue()
	{
		return _value;
	}
	
	public Tuple<String> getTimestamp()
	{
		return _timestamp;
	}
}
