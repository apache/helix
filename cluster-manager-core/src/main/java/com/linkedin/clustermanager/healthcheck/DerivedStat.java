package com.linkedin.clustermanager.healthcheck;

public abstract class DerivedStat extends Stat {

	public DerivedStat(String opType, String measurementType,
			String resourceName, String partitionName, String nodeName) {
		super(opType, measurementType, resourceName, partitionName, nodeName);
	}

	@Override
	public int compareTo(Stat o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean contains(RawStat rs) 
	{
		//TODO
		//for each rawstat component, see if this contains that component or the component
		//is set to null.  If true for all components, return true.
		return false;
	}

	public abstract void applyRawStat(RawStat rs);
	
}
