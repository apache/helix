package com.linkedin.clustermanager.alerts;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.controller.stages.ClusterDataCache;
import com.linkedin.clustermanager.controller.stages.StatsAggregationStage;
import com.linkedin.clustermanager.model.HealthStat;

public class StatsHolder {

	private static final Logger logger = Logger
		    .getLogger(StatsHolder.class.getName());
	
	ClusterDataAccessor _accessor;
	ClusterDataCache _cache;
	
	public StatsHolder(ClusterManager manager)
	{
		_accessor = manager.getDataAccessor();
		_cache = new ClusterDataCache();
	}
	
	public Iterator<String> getAllStats() 
	{
		return null;
	}
	
	public void applyStat(String stat)
	{
		
	}
	
	public void addStat(String stat)
	{
		 ZNRecord rec = new ZNRecord("persistent stats");
		 Map<String, Map<String, String>> stats = new HashMap<String, Map<String, String>>();
		 Map<String, String> statFields = new HashMap<String, String>();
		 statFields.put("time", "0");
		 statFields.put("value", "0");
		 stats.put(stat, statFields);
		 rec.setMapFields(stats);
		 boolean retVal = _accessor.setProperty(PropertyType.GLOBALSTATS, rec);
		 logger.debug("addStat retVal: "+retVal);
				 
		/*
		_cache.refresh(_accessor);
		HealthStat currentStats = _cache.getGlobalStats(); //TODO: revise what this returns
		 Map<String, Map<String, String>> persistentStatsMap = currentStats.getMapFields();
		 for (String key : persistentStatsMap.keySet()) {
			 logger.debug("key: "+key);
		 }
		 */
	}
}
