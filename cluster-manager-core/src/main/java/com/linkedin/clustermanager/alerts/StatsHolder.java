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
	Map<String, Map<String, String>> _statMap;
	
	public StatsHolder(ClusterManager manager)
	{
		_accessor = manager.getDataAccessor();
		_cache = new ClusterDataCache();
	}
	
	public void refreshStats()
	{
		_cache.refresh(_accessor);
		_statMap = _cache.getPersistentStats();
		//TODO: confirm this a good place to init the _statMap when null
		if (_statMap == null) {
			_statMap = new HashMap<String, Map<String, String>>();
		}
	}
	
	public void persistStats() 
	{
		//XXX: Am I using _accessor too directly here?
		ZNRecord statsRec = _accessor.getProperty(PropertyType.PERSISTENTSTATS);
		if (statsRec == null) {
			statsRec = new ZNRecord("PersistentStats"); //TODO: fix naming of this record, if it matters
		}
		statsRec.setMapFields(_statMap);
		 boolean retVal = _accessor.setProperty(PropertyType.PERSISTENTSTATS, statsRec);
		 logger.debug("persistStats retVal: "+retVal);
	}
	
	public Iterator<String> getAllStats() 
	{
		return null;
	}
	
	public void applyStat(String stat)
	{
		
	}
	
	
	//add parsing of stat (or is that in expression holder?)  at least add validate
	public void addStat(String exp) throws Exception
	{
		refreshStats(); //get current stats

		String[] parsedStats = ExpressionParser.getBaseStats(exp);

		for (String stat : parsedStats) {
			if (_statMap.containsKey(stat)) {
				logger.debug("Stat "+stat+" already exists; not adding");
				continue;
			}		
			Map<String, String> statFields = new HashMap<String, String>(); //new fields for this stat
			statFields.put("time", "0");
			statFields.put("value", "0");
			_statMap.put(stat, statFields); //add new stat to map
		}
		persistStats(); //save stats
				 
	}
}
