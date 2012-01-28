package com.linkedin.helix.alerts;

public class Alert {
	
	String _name;
	
	String _expression;
	String _comparator;
	Tuple<String> _constant;
	
	public Alert(String name, String expression, String comparator, Tuple<String> constant)
	{
		_name=name;
		_expression=expression;
		_comparator=comparator;
		_constant=constant;
	}
	
	public String getName()
	{
		return _name;
	}
	
	public String getExpression()
	{
		return _expression;
	}
	
	public String getComparator()
	{
		return _comparator;
	}
	
	public Tuple<String> getConstant() 
	{
		return _constant;
	}
}
