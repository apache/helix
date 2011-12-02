package com.linkedin.clustermanager.alerts;

import java.util.Vector;

public class Tuple<T> {
	Vector<T> elements;
	
	public Tuple() 
	{
		elements = new Vector<T>();
	}
	
	public int size()
	{
		return elements.size();
	}
}
