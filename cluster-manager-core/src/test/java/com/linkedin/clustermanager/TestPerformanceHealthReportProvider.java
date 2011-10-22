package com.linkedin.clustermanager;

import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.healthcheck.PerformanceHealthReportProvider;

public class TestPerformanceHealthReportProvider {
	
	 final String STAT_NAME = "Stat_123";
	 final String PARTITION_NAME = "Partition_456";
	 final String FAKE_STAT_NAME = "Stat_ABC";
	 final String FAKE_PARTITION_NAME = "Partition_DEF";
	 final String STORED_STAT = "789";
	
	PerformanceHealthReportProvider _healthProvider;
	
	public void incrementPartitionStat() throws Exception
	{
		_healthProvider.incrementPartitionStat(STAT_NAME, PARTITION_NAME);
	}
	
	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_healthProvider = new PerformanceHealthReportProvider();
	}
	 
	 @Test (groups = {"unitTest"})
	  public void testGetRecentHealthReports() throws Exception
	  {
		 _healthProvider.getRecentHealthReport();
		 _healthProvider.getRecentPartitionHealthReport();
	  }
	
	 @Test (groups = {"unitTest"})
	 public void testIncrementPartitionStat() throws Exception
	 {
		 //stat does not exist yet
		 _healthProvider.incrementPartitionStat(STAT_NAME, PARTITION_NAME);
		 //stat does exist
		 _healthProvider.incrementPartitionStat(STAT_NAME, PARTITION_NAME);
		 String retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertEquals((double)2.0, Double.parseDouble(retrievedStat));
		 
		 //set to some other value
		 _healthProvider.submitPartitionStat(STAT_NAME, PARTITION_NAME, STORED_STAT);
		 _healthProvider.incrementPartitionStat(STAT_NAME, PARTITION_NAME);
		 retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertEquals(Double.parseDouble(retrievedStat), Double.parseDouble(STORED_STAT)+1);
	 }
	 
	 @Test (groups = {"unitTest"})
	 public void testSetGetPartitionStat() throws Exception
	 {
		 _healthProvider.submitPartitionStat(STAT_NAME, PARTITION_NAME, STORED_STAT);
		 String retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 //check on correct retrieval for real stat, real partition
		 AssertJUnit.assertEquals(STORED_STAT, retrievedStat);
		 
		 //real stat, fake partition
		 retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, FAKE_PARTITION_NAME);
		 AssertJUnit.assertNull(retrievedStat);
		 
		 //fake stat, real partition
		 retrievedStat = _healthProvider.getPartitionStat(FAKE_STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertNull(retrievedStat);
		 
		 //fake stat, fake partition
		 retrievedStat = _healthProvider.getPartitionStat(FAKE_STAT_NAME, FAKE_PARTITION_NAME);
		 AssertJUnit.assertNull(retrievedStat);
	 }
	 
	 @Test (groups = {"unitTest"})
	 public void testGetPartitionHealthReport() throws Exception
	 {
		 //test empty map case
		 Map<String, Map<String, String>> resultMap = _healthProvider.getRecentPartitionHealthReport();
		 AssertJUnit.assertEquals(resultMap.size(), 0);
		 
		 //test non-empty case
		 testSetGetPartitionStat();
		 resultMap = _healthProvider.getRecentPartitionHealthReport();
		 //check contains 1 stat
		 AssertJUnit.assertEquals(1, resultMap.size());
		 //check contains STAT_NAME STAT
		 AssertJUnit.assertTrue(resultMap.keySet().contains(STAT_NAME));
		 Map<String, String> statMap = resultMap.get(STAT_NAME);
		 //check statMap has size 1
		 AssertJUnit.assertEquals(1, statMap.size());
		 //check contains PARTITION_NAME
		 AssertJUnit.assertTrue(statMap.keySet().contains(PARTITION_NAME));
		 //check stored val
		 String statVal = statMap.get(PARTITION_NAME);
		 AssertJUnit.assertEquals(statVal, STORED_STAT);
	 }
	 
	 @Test (groups = {"unitTest"})
	 public void testPartitionStatReset() throws Exception
	 {
		 incrementPartitionStat();
		 //ensure stat appears
		 String retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertEquals((double)1.0, Double.parseDouble(retrievedStat));
		 //reset partition stats
		 _healthProvider.resetPartitionStats();
		 retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertEquals(null, retrievedStat);
	 }
	 
	 @Test (groups = {"unitTest"})
	  public void testGetReportName() throws Exception
	  {
		 _healthProvider.getReportName();
	  }
}
