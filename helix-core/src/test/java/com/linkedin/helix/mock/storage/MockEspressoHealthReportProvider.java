package com.linkedin.helix.mock.storage;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.helix.alerts.StatsHolder;
import com.linkedin.helix.healthcheck.HealthReportProvider;

public class MockEspressoHealthReportProvider extends HealthReportProvider {

	private final String _reportName = "RestQueryStats";
	private HashMap<String, Map<String,String>> _statMap;
	private final String DB_NAME = "DBName";
	
	public MockEspressoHealthReportProvider()
	{
		super();
		_statMap = new HashMap<String, Map<String,String>>();
	}
	
	public String buildMapKey(String dbName)
	{
		return _reportName+"@"+DB_NAME+"="+dbName;
	}
	
	public void setStat(String dbName, String statName, String statVal)
	{
		String key = buildMapKey(dbName);
		Map<String, String> dbStatMap = _statMap.get(key);
		if (dbStatMap == null) {
			dbStatMap = new HashMap<String,String>();
			_statMap.put(key, dbStatMap);
		}
		dbStatMap.put(statName,  statVal);
		String currTime = String.valueOf(System.currentTimeMillis());
		dbStatMap.put(StatsHolder.TIMESTAMP_NAME, currTime);
	}
	
	@Override
	public Map<String, String> getRecentHealthReport() {
		return null;
	}

	@Override
	public Map<String, Map<String, String>> getRecentPartitionHealthReport() {
		return _statMap;
	}
	
	@Override
	public void resetStats() {
	_statMap.clear();
	}
	
	public String getReportName() 
	{
		return _reportName;
	}

}
