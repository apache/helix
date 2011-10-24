package com.linkedin.clustermanager.healthcheck;

public class RawStat extends Stat {

	long value;
	
	public RawStat(String opType, String measurementType, String resourceName,
			String partitionName, String nodeName) {
		super(opType, measurementType, resourceName, partitionName, nodeName);
	}

	public void setValue(long val) 
	{
		value = val;
	}
	
	public long getValue() 
	{
		return value;
	}
	
	@Override
	public int compareTo(Stat o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
