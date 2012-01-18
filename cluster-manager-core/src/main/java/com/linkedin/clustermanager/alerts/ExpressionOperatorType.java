package com.linkedin.clustermanager.alerts;

public enum ExpressionOperatorType {
	//each
	EACH(true),
	//standard math
	SUM(false),
	MULTIPLY(false),
	SUBTRACT(false),
	DIVIDE(false),
	//aggregation types
	ACCUMULATE(true),
	DECAY(false),
	WINDOW(false);
	
	boolean isBase;
	
	private ExpressionOperatorType(boolean isBase) 
	{
		this.isBase = isBase;
	}
	
	boolean isBaseOp()
	{
		return isBase;
	}
}
