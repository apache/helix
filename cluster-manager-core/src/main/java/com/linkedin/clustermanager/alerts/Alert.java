package com.linkedin.clustermanager.alerts;

public class Alert {
	
	String _name;
	
	String _expression;
	String _comparator;
	String _constant;
	
	public Alert(String name, String expression, String comparator, String constant)
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
	
	public String getConstant() 
	{
		return _constant;
	}
}
