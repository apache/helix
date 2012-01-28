package com.linkedin.helix;

import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.Mocks.MockManager;
import com.linkedin.helix.healthcheck.PerformanceHealthReportProvider;

public class TestPerformanceHealthReportProvider {

	 protected static final String CLUSTER_NAME = "TestCluster";
	 protected final String STAT_NAME = "Stat_123";
	 protected final String PARTITION_NAME = "Partition_456";
	 protected final String FAKE_STAT_NAME = "Stat_ABC";
	 protected final String FAKE_PARTITION_NAME = "Partition_DEF";
	 protected final String STORED_STAT = "789";
	 protected final String INSTANCE_NAME = "instance:1";

	PerformanceHealthReportProvider _healthProvider;
	MockManager _clusterManager;

	public void incrementPartitionStat() throws Exception
	{
		_clusterManager = new MockManager(CLUSTER_NAME);
		_healthProvider.incrementPartitionStat(STAT_NAME, PARTITION_NAME);
	}

	public void transmitReport() throws Exception
	{
		_clusterManager = new MockManager(CLUSTER_NAME);
		 Map<String, Map<String, String>> partitionReport = _healthProvider
	                .getRecentPartitionHealthReport();
		 ZNRecord record = new ZNRecord(_healthProvider.getReportName());
		 if (partitionReport != null) {
         	record.setMapFields(partitionReport);
         }
		 _clusterManager.getDataAccessor().setProperty(PropertyType.HEALTHREPORT,
		                                               record,
		                                               INSTANCE_NAME,
		                                               record.getId());
	}

	@BeforeMethod ()
	public void setup()
	{
		_healthProvider = new PerformanceHealthReportProvider();
	}

	 @Test ()
	  public void testGetRecentHealthReports() throws Exception
	  {
		 _healthProvider.getRecentHealthReport();
		 _healthProvider.getRecentPartitionHealthReport();
	  }

	 @Test ()
	 public void testIncrementPartitionStat() throws Exception
	 {
		 //stat does not exist yet
		 _healthProvider.incrementPartitionStat(STAT_NAME, PARTITION_NAME);
		 transmitReport();
		 //stat does exist
		 _healthProvider.incrementPartitionStat(STAT_NAME, PARTITION_NAME);
		 transmitReport();
		 String retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertEquals(2.0, Double.parseDouble(retrievedStat));

		 //set to some other value
		 _healthProvider.submitPartitionStat(STAT_NAME, PARTITION_NAME, STORED_STAT);
		 transmitReport();
		 _healthProvider.incrementPartitionStat(STAT_NAME, PARTITION_NAME);
		 transmitReport();
		 retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertEquals(Double.parseDouble(retrievedStat), Double.parseDouble(STORED_STAT)+1);
	 }

	 @Test ()
	 public void testSetGetPartitionStat() throws Exception
	 {
		 _healthProvider.submitPartitionStat(STAT_NAME, PARTITION_NAME, STORED_STAT);
		 transmitReport();
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

	 @Test ()
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

	 @Test ()
	 public void testPartitionStatReset() throws Exception
	 {
		 incrementPartitionStat();
		 //ensure stat appears
		 String retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertEquals(1.0, Double.parseDouble(retrievedStat));
		 //reset partition stats
		 _healthProvider.resetStats();
		 transmitReport();
		 retrievedStat = _healthProvider.getPartitionStat(STAT_NAME, PARTITION_NAME);
		 AssertJUnit.assertEquals(null, retrievedStat);
	 }

	 @Test ()
	  public void testGetReportName() throws Exception
	  {
		 _healthProvider.getReportName();
	  }
}
