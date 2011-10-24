package com.linkedin.clustermanager.healthcheck;

public interface StatCollector {
	
	//Not sure 1 interface for raw and derived stats collections works, if we decide 
	//the functionality for each are mostly mutually exclusive.
	
	//functions for editing raw stats
	public void setStat(RawStat rs, long value);
	
	public void incrementStat(RawStat rs, long value);
	
	//functions for editing derived stats
	
	//apply RawStat to each DerivedStat that contains it
	public void applyStat(RawStat rs);
}
