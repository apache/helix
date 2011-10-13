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
	
	private final Map<String, String> _partitionCountsMap = new HashMap<String, String>();
	
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
		result.put(_partitionRequestCountStat, _partitionCountsMap);
		return result;
	}
	
	public void submitReadLatency(double latency) {
		readLatencyCount++;
		readLatencySum+=latency;
	}
	
	public void submitRequestCount(int count) {
		requestCount += count;
	}
	
	public void submitIncrementPartitionRequestCount(String partitionName) {
		if (_partitionCountsMap.containsKey(partitionName)) {
			int oldCount = Integer.parseInt((_partitionCountsMap.get(partitionName)).substring(1));
			submitPartitionRequestCount(partitionName, oldCount+1);
		}
		else {
			submitPartitionRequestCount(partitionName, 1);
		}
	}
	
	public void submitPartitionRequestCount(String partitionName, int count) {
		_partitionCountsMap.put(partitionName, ""+count);
	}
	
	public String getReportName()
	  {
	    return _performanceCounters;
	  }
}
