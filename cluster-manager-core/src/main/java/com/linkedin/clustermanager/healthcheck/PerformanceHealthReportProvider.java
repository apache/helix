package com.linkedin.clustermanager.healthcheck;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class PerformanceHealthReportProvider extends HealthReportProvider {

	private static final Logger _logger = Logger.getLogger(PerformanceHealthReportProvider.class);
	  
	public final static String _testStat = "testStat";
	public final static String _readLatencyStat = "readLatencyStat";
	public final static String _requestCountStat = "requestCountStat";
	public final static String _partitionRequestCountStat = "partitionRequestCountStat";
	
	public static final String _performanceCounters = "performanceCounters";
	
	public int readLatencyCount = 0;
	public double readLatencySum = 0;
	
	public int requestCount = 0;
	
	//private final Map<String, String> _partitionCountsMap = new HashMap<String, String>();
	
	
	private final Map<String, HashMap<String,String>> _partitionStatMaps = new HashMap<String, HashMap<String,String>>();
	
	 public PerformanceHealthReportProvider()
	 {}
	
	@Override
	public Map<String, String> getRecentHealthReport() {
		long testStat = 10;
		
		 Map<String, String> result = new TreeMap<String, String>();
		    
		 result.put(_testStat, "" + testStat);
		 result.put(_readLatencyStat, "" + readLatencySum/(double)readLatencyCount);
		 result.put(_requestCountStat, "" + requestCount);
		 
		 return result;
	}

	@Override
	public Map<String, Map<String, String>> getRecentPartitionHealthReport() {
		Map<String, Map<String, String>> result = new TreeMap<String, Map<String, String>>();
		for (String statName : _partitionStatMaps.keySet()) {
			result.put(statName, _partitionStatMaps.get(statName));
		}
		return result;
	}
	
	HashMap<String, String> getStatMap(String statName, boolean createIfMissing) {
		//check if map for this stat exists.  if not, create it
		HashMap<String, String> statMap;
		if (! _partitionStatMaps.containsKey(statName)) {
			if (!createIfMissing) {
				return null;
			}
			statMap = new HashMap<String, String>();
			_partitionStatMaps.put(statName, statMap);
		}
		else {
			statMap = _partitionStatMaps.get(statName);
		}
		return statMap;
	}

	//TODO:
	//Currently participant is source of truth and updates ZK. We want ZK to be source of truth.
	//Revise this approach the participant sends deltas of stats to controller (ZK?) and have controller do aggregation
	//and update ZK.  Make sure to wipe the participant between uploads.
	String getPartitionStat(HashMap<String, String> partitionMap, String partitionName) {
		return partitionMap.get(partitionName);
	}
	
	void setPartitionStat(HashMap<String, String> partitionMap, String partitionName, String value) {
		partitionMap.put(partitionName, value);
	}
	
	public void incrementPartitionStat(String statName, String partitionName) {
		HashMap<String, String> statMap = getStatMap(statName, true);
		String currValStr = getPartitionStat(statMap, partitionName);
		double currVal;
		if (currValStr == null) {
			currVal = 1.0;
		}
		else {
			currVal = Double.parseDouble(getPartitionStat(statMap, partitionName));
			currVal++;
		}		
		setPartitionStat(statMap, partitionName, String.valueOf(currVal));
	}
	
	public void submitPartitionStat(String statName, String partitionName, String value) 
	{
		HashMap<String, String> statMap = getStatMap(statName, true);
		setPartitionStat(statMap, partitionName, value);
	}
	
	public String getPartitionStat(String statName, String partitionName) 
	{
		HashMap<String, String> statMap = getStatMap(statName, false);
		if (statMap == null) {
			return null;
		}
		else {
			return statMap.get(partitionName);
		}
	}
	
	public void resetPartitionStats()
	{
		_partitionStatMaps.clear();
	}
	
	public String getReportName()
	  {
	    return _performanceCounters;
	  }
}
