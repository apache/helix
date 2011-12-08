package com.linkedin.clustermanager.alerts;

import java.util.HashMap;
import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.Mocks.MockManager;


public class TestEvaluateAlerts {
	protected static final String CLUSTER_NAME = "TestCluster";

	MockManager _clusterManager;
	AlertsHolder _alertsHolder;
	StatsHolder _statsHolder;

	public final String EXP = AlertParser.EXPRESSION_NAME;
	public final String CMP = AlertParser.COMPARATOR_NAME;
	public final String CON = AlertParser.CONSTANT_NAME;

	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_clusterManager = new MockManager(CLUSTER_NAME);
		_alertsHolder = new AlertsHolder(_clusterManager);
		_statsHolder = new StatsHolder(_clusterManager);
	}
	
	public Map<String,String> getStatFields(String value, String timestamp)
	{
		Map<String, String> statMap = new HashMap<String,String>();
		statMap.put(StatsHolder.VALUE_NAME, value);
		statMap.put(StatsHolder.TIMESTAMP_NAME, timestamp);
		return statMap;
	}
	
	public void addSimpleStat() throws ClusterManagerException
	{
		String stat = "accumulate()(dbFoo.partition10.latency)";
		_statsHolder.addStat(stat);
	}
	
	public void addSimpleAlert() throws ClusterManagerException
	{
		String alert = EXP + "(accumulate()(dbFoo.partition10.latency))"
				+ CMP + "(GREATER)" + CON + "(100)";
	     _alertsHolder.addAlert(alert);
	}

	public void addArrivingSimpleStat() throws ClusterManagerException
	{
		String incomingStatName = "dbFoo.partition10.latency";
		Map<String, String> statFields = getStatFields("110","0");
		_statsHolder.applyStat(incomingStatName, statFields);
	}
	
	@Test (groups = {"unitTest"})
	public void testSimpleAlertFires()
	{
		addSimpleStat();
		addSimpleAlert();
		addArrivingSimpleStat();
		AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
	}
	
}
