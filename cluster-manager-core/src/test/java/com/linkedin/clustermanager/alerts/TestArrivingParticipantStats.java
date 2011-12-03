package com.linkedin.clustermanager.alerts;

import java.util.HashMap;
import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.Mocks.MockManager;

public class TestArrivingParticipantStats {
    protected static final String CLUSTER_NAME = "TestCluster";
	
    protected static final String VALUE_NAME = "value";
    protected static final String TIMESTAMP_NAME = "timestamp";
    
    
	MockManager _clusterManager;
	StatsHolder _statsHolder;
	
	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_clusterManager = new MockManager(CLUSTER_NAME);
		_statsHolder = new StatsHolder(_clusterManager);
	}
	
	public Map<String,String> getStatFields(String value, String timestamp)
	{
		Map<String, String> statMap = new HashMap<String,String>();
		statMap.put(VALUE_NAME, value);
		statMap.put(TIMESTAMP_NAME, timestamp);
		return statMap;
	}
	
	public boolean statRecordContains(ZNRecord rec, String statName) 
	{
		Map<String,Map<String,String>> stats = rec.getMapFields();
		return stats.containsKey(statName);
	}
	
	public boolean statRecordHasValue(ZNRecord rec, String statName, String value)
	{
		Map<String,Map<String,String>> stats = rec.getMapFields();
		Map<String, String> statFields = stats.get(statName);
		return (statFields.get(VALUE_NAME).equals(value));
	}
	
	public boolean statRecordHasTimestamp(ZNRecord rec, String statName, String timestamp)
	{
		Map<String,Map<String,String>> stats = rec.getMapFields();
		Map<String, String> statFields = stats.get(statName);
		return (statFields.get(TIMESTAMP_NAME).equals(timestamp));
	}
	
	@Test (groups = {"unitTest"})
	  public void testAddOneParticipantStat() throws Exception
	  {
		 //add a persistent stat
		 String persistentStat = "window(5)(dbFoo.partition10.latency)";
		 _statsHolder.addStat(persistentStat);
		 
		 //generate incoming stat
		 String incomingStatName = "(dbFoo.partition10.latency)";
		 Map<String, String> statFields = getStatFields("0","0");
		 _statsHolder.applyStat(incomingStatName, statFields);
		 
		 //check persistent stats
		 ZNRecord rec = _clusterManager.getDataAccessor().getProperty(PropertyType.PERSISTENTSTATS);
		 
		 System.out.println("rec: "+rec.toString());
		 AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0"));
		 AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0"));
	  }
	
}
