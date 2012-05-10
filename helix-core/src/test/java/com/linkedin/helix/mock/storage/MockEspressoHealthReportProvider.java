/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
		String currTime = String.valueOf(System.currentTimeMillis());
		setStat(dbName, statName, statVal, currTime);
	}
	
	/*
	 * This version takes a fixed timestamp to ease with testing
	 */
	public void setStat(String dbName, String statName, String statVal, String timestamp)
	{
		String key = buildMapKey(dbName);
		Map<String, String> dbStatMap = _statMap.get(key);
		if (dbStatMap == null) {
			dbStatMap = new HashMap<String,String>();
			_statMap.put(key, dbStatMap);
		}
		dbStatMap.put(statName,  statVal);
		dbStatMap.put(StatsHolder.TIMESTAMP_NAME, timestamp);
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
