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
	
	public String addSimpleStat() throws ClusterManagerException
	{
		String stat = "accumulate()(dbFoo.partition10.latency)";
		_statsHolder.addStat(stat);
		return stat;
	}
	
	public String addWildcardStat() throws ClusterManagerException
	{
		String stat = "accumulate()(dbFoo.partition*.latency)";
		_statsHolder.addStat(stat);
		return stat;
	}
	
	public String addSimpleAlert() throws ClusterManagerException
	{
		String alert = EXP + "(accumulate()(dbFoo.partition10.latency))"
				+ CMP + "(GREATER)" + CON + "(100)";
	     _alertsHolder.addAlert(alert);
	     return alert;
	}
	
	public String addWildcardAlert() throws ClusterManagerException
	{
		String alert = EXP + "(accumulate()(dbFoo.partition*.latency))"
				+ CMP + "(GREATER)" + CON + "(100)";
	     _alertsHolder.addAlert(alert);
	     return alert;
	}

	public String addExpandWildcardAlert() throws ClusterManagerException
	{
		String alert = EXP + "(accumulate()(dbFoo.partition*.latency)|EXPAND)"
				+ CMP + "(GREATER)" + CON + "(100)";
	     _alertsHolder.addAlert(alert);
	     return alert;
	}
	
	public String addArrivingSimpleStat() throws ClusterManagerException
	{
		String incomingStatName = "dbFoo.partition10.latency";
		Map<String, String> statFields = getStatFields("110","0");
		_statsHolder.applyStat(incomingStatName, statFields);
		return incomingStatName;
	}
	
	@Test (groups = {"unitTest"})
	public void testSimpleAlertFires()
	{
		String stat = addSimpleStat();
		String alert = addSimpleAlert();
		addArrivingSimpleStat();
		Map<String, Map<String, Boolean>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
		boolean alertFired = alertResult.get(alert).get(stat);
		 AssertJUnit.assertTrue(alertFired);
	}
	
	@Test (groups = {"unitTest"})
	public void testWildcardAlertFires()
	{
		String stat = addWildcardStat();
		String alert = addWildcardAlert();
		addArrivingSimpleStat();
		Map<String, Map<String, Boolean>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
		String wildcardBinding = "10";
		boolean alertFired = alertResult.get(alert).get(wildcardBinding);
		AssertJUnit.assertTrue(alertFired);	
	}
	
	@Test (groups = {"unitTest"})
	public void testExpandOperatorWildcardAlertFires()
	{
		String stat = addWildcardStat();
		String alert = addExpandWildcardAlert();
		addArrivingSimpleStat();
		Map<String, Map<String, Boolean>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
		String wildcardBinding = "10";
		boolean alertFired = alertResult.get(alert).get(wildcardBinding);
		AssertJUnit.assertTrue(alertFired);	
	}
	
	//test with multiple stats no wildcard (need to implement an operator for it)
	//test with multiple stats and wildcard, some fire, some don't
	//test with multiple stats, and multiple stats added.
	//test with multiple ops, collapsing from 2+ list width to 1
	//test non-fired alerts
	//test multiple different stats, make sure they all appear
	//test using sumall
	//anything else, look around at the code
	
	//next: review all older tests
	//next: actually write the fired alerts to ZK
	
}
