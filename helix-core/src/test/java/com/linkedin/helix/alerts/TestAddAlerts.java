package com.linkedin.helix.alerts;

import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.helix.Mocks.MockManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.stages.HealthDataCache;

public class TestAddAlerts {

protected static final String CLUSTER_NAME = "TestCluster";

	MockManager _helixManager;
	AlertsHolder _alertsHolder;

	public final String EXP = AlertParser.EXPRESSION_NAME;
	public final String CMP = AlertParser.COMPARATOR_NAME;
	public final String CON = AlertParser.CONSTANT_NAME;

	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_helixManager = new MockManager(CLUSTER_NAME);
		_alertsHolder = new AlertsHolder(_helixManager, new HealthDataCache());
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
		 ZNRecord rec = _helixManager.getDataAccessor().getProperty(PropertyType.ALERTS);
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
		 ZNRecord rec = _helixManager.getDataAccessor().getProperty(PropertyType.ALERTS);
		 //System.out.println("alert: "+alert1);
		 System.out.println("rec: "+rec.toString());
		 AssertJUnit.assertTrue(alertRecordContains(rec,alert1));
		 AssertJUnit.assertTrue(alertRecordContains(rec,alert2));
		 AssertJUnit.assertEquals(2, alertsSize(rec));
	  }

	@Test (groups = {"unitTest"})
	  public void testAddTwoWildcardAlert() throws Exception
	  {
		 String alert1 = EXP + "(accumulate()(dbFoo.partition*.put*))"
					+ CMP + "(GREATER)" + CON + "(10)";
		 _alertsHolder.addAlert(alert1);
		 ZNRecord rec = _helixManager.getDataAccessor().getProperty(PropertyType.ALERTS);
		 //System.out.println("alert: "+alert1);
		 System.out.println("rec: "+rec.toString());
		 AssertJUnit.assertTrue(alertRecordContains(rec,alert1));
		 AssertJUnit.assertEquals(1, alertsSize(rec));
	  }

	//add 2 wildcard alert here
}
