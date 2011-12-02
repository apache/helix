package com.linkedin.clustermanager.alerts;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.healthcheck.PerformanceHealthReportProvider;

public class TestStatAdd {

	protected static final String CLUSTER_NAME = "TestCluster";
	
	MockManager _clusterManager;
	StatsHolder _statsHolder;
	
	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_clusterManager = new MockManager(CLUSTER_NAME);
		_statsHolder = new StatsHolder(_clusterManager);
	}
	
	 @Test (groups = {"unitTest"})
	  public void testAddStat() throws Exception
	  {
		 String stat = "window(5)(dbFoo.partition10.latency)";
		 _statsHolder.addStat(stat);
		 ZNRecord rec = _clusterManager.getDataAccessor().getProperty(PropertyType.GLOBALSTATS);
		 System.out.println("rec: "+rec.toString());
	  }
	
}
