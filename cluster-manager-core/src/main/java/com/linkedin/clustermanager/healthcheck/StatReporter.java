package com.linkedin.clustermanager.healthcheck;

import java.util.HashSet;

public class StatReporter {
	
	//stats that we maintain and store
	HashSet<Stat> maintainedStats;
	
	//stats computable on-demand from maintained stats
	HashSet<Stat> computedStat;
	
	public StatReporter() 
	{
		
	}
	
	private void shuffleStatMaintainence() 
	{
		//compute what stats should be maintained, compute
		//open issue: what happens to data when we unmaintain a stat
		
		//first cut: maintain all the raw stats or maintain all derived stats
		
		//
	}
	
	public void addStat(Stat s) 
	{
		//add the Stat (to maintainedStats by default)
		
		//open question: should we add all raw stats here?
	}
	
	public void applyStat(Stat s)
	{
		
	}
	
	public Stat getStat(Stat s) 
	{
		//if a stat is found with same name, return it.  else, null
		return null;
	}
	
	public Stat getStat(String s) 
	{
		//s is serialized form of stat name.  search for that
		return null;
	}
}
