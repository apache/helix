package com.linkedin.clustermanager.alerts;

import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.Mocks.MockManager;

public class TestAddAlerts {

protected static final String CLUSTER_NAME = "TestCluster";
	
	MockManager _clusterManager;
	AlertsHolder _alertsHolder;
	
	public final String EXP = AlertParser.EXPRESSION_NAME;
	public final String CMP = AlertParser.COMPARATOR_NAME;
	public final String CON = AlertParser.CONSTANT_NAME;
	
	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_clusterManager = new MockManager(CLUSTER_NAME);
		_alertsHolder = new AlertsHolder(_clusterManager);
	}
	
	
	public boolean alertRecordContains(ZNRecord rec, String alertName) 
	{
		Map<String,Map<String,String>> alerts = rec.getMapFields();
		return alerts.containsKey(alertName);
	}
	
	public int alertsSize(ZNRecord rec)
	{
		Map<String,Map<String,String>> alerts = rec.getMapFields();
		return alerts.size();
	}
	
	@Test (groups = {"unitTest"})
	  public void testAddAlert() throws Exception
	  {
		 String alert = EXP + "(accumulate()(dbFoo.partition10.latency))"
					+ CMP + "(GREATER)" + CON + "(10)";
		 _alertsHolder.addAlert(alert);
		 ZNRecord rec = _clusterManager.getDataAccessor().getProperty(PropertyType.ALERTS);
		 System.out.println("alert: "+alert);
		 System.out.println("rec: "+rec.toString());
		 AssertJUnit.assertTrue(alertRecordContains(rec,alert));
		 AssertJUnit.assertEquals(1, alertsSize(rec));
	  }
	
	@Test (groups = {"unitTest"})
	  public void testAddTwoAlerts() throws Exception
	  {
		 String alert1 = EXP + "(accumulate()(dbFoo.partition10.latency))"
					+ CMP + "(GREATER)" + CON + "(10)";
		 String alert2 = EXP + "(accumulate()(dbFoo.partition10.latency))"
					+ CMP + "(GREATER)" + CON + "(100)";
		 _alertsHolder.addAlert(alert1);
		 _alertsHolder.addAlert(alert2);
		 ZNRecord rec = _clusterManager.getDataAccessor().getProperty(PropertyType.ALERTS);
		 //System.out.println("alert: "+alert1);
		 System.out.println("rec: "+rec.toString());
		 AssertJUnit.assertTrue(alertRecordContains(rec,alert1));
		 AssertJUnit.assertTrue(alertRecordContains(rec,alert2));
		 AssertJUnit.assertEquals(2, alertsSize(rec));
	  }
}
