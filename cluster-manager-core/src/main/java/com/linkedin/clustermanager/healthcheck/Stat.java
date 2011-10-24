package com.linkedin.clustermanager.healthcheck;

public abstract class Stat implements Comparable<Stat> {
	
	String opType;
	String measurementType;
	String resourceName;
	String partitionName;
	String nodeName;
	long creationTime;
	
	
	public Stat(String opType, String measurementType, String resourceName,
			String partitionName, String nodeName)
	{
		this.opType = opType;	
		this.measurementType = measurementType;
		this.resourceName = resourceName;
		this.partitionName = partitionName;
		this.nodeName = nodeName;
		creationTime = System.currentTimeMillis();
	}
	
	public static String serialize(Stat rs) 
	{
		return null;
	}
	
	public static Stat deserialize(String s) 
	{
		return null;
	}
	
	public int compareTo() 
	{
		return 0;
	}
	
	public long getCreationTime()
	{
		return creationTime;
	}
}
