package com.linkedin.helix.alerts;

import java.util.HashMap;
import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.Mocks.MockManager;
import com.linkedin.helix.controller.stages.HealthDataCache;

public class TestEvaluateAlerts {
  protected static final String CLUSTER_NAME = "TestCluster";

  MockManager _helixManager;
  AlertsHolder _alertsHolder;
  StatsHolder _statsHolder;

  public final String EXP = AlertParser.EXPRESSION_NAME;
  public final String CMP = AlertParser.COMPARATOR_NAME;
  public final String CON = AlertParser.CONSTANT_NAME;

  @BeforeMethod (groups = {"unitTest"})
  public void setup()
  {
    HealthDataCache cache = new HealthDataCache();
    _helixManager = new MockManager(CLUSTER_NAME);
    _alertsHolder = new AlertsHolder(_helixManager, cache);
    _statsHolder = _alertsHolder._statsHolder;
  }

  public Map<String,String> getStatFields(String value, String timestamp)
  {
    Map<String, String> statMap = new HashMap<String,String>();
    statMap.put(StatsHolder.VALUE_NAME, value);
    statMap.put(StatsHolder.TIMESTAMP_NAME, timestamp);
    return statMap;
  }

  public String getSimpleStat() throws HelixException
  {
    String stat = "accumulate()(dbFoo.partition10.latency)";
    //_statsHolder.addStat(stat);
    return stat;
  }

  public String addPairOfStats() throws HelixException
  {
    String stat = "accumulate()(dbFoo.partition10.latency, dbFoo.partition11.latency)";
    _statsHolder.addStat(stat);
    _statsHolder.persistStats();
    return stat;
  }

  public String getWildcardStat() throws HelixException
  {
    String stat = "accumulate()(dbFoo.partition*.latency)";
    //_statsHolder.addStat(stat);
    return stat;
  }

  public String addSimpleAlert() throws HelixException
  {
    String alert = EXP + "(accumulate()(dbFoo.partition10.latency))"
        + CMP + "(GREATER)" + CON + "(100)";
       _alertsHolder.addAlert(alert);
       return alert;
  }

  public String addWildcardAlert() throws HelixException
  {
    String alert = EXP + "(accumulate()(dbFoo.partition*.latency))"
        + CMP + "(GREATER)" + CON + "(100)";
       _alertsHolder.addAlert(alert);
       return alert;
  }

  public String addTwoWildcardAlert() throws HelixException
  {
    String alert = EXP + "(accumulate()(dbFoo.partition*.put*))"
        + CMP + "(GREATER)" + CON + "(100)";
       _alertsHolder.addAlert(alert);
       return alert;
  }


  public String addExpandWildcardAlert() throws HelixException
  {
    String alert = EXP + "(accumulate()(dbFoo.partition*.latency)|EXPAND)"
        + CMP + "(GREATER)" + CON + "(100)";
       _alertsHolder.addAlert(alert);
       return alert;
  }

  public String addExpandSumAlert() throws HelixException
  {
    String alert = EXP + "(accumulate()(dbFoo.partition10.latency,dbFoo.partition11.latency)|EXPAND|SUM)"
        + CMP + "(GREATER)" + CON + "(100)";
       _alertsHolder.addAlert(alert);
       return alert;
  }

  public String addExpandSumWildcardAlert() throws HelixException
  {
    String alert = EXP + "(accumulate()(dbFoo.partition*.success,dbFoo.partition*.failure)|EXPAND|SUM)"
        + CMP + "(GREATER)" + CON + "(100)";
       _alertsHolder.addAlert(alert);
       return alert;
  }

  public String addExpandSumEachWildcardAlert() throws HelixException
  {
    String alert = EXP + "(accumulate()(dbFoo.partition*.success,dbFoo.partition*.failure)|EXPAND|SUMEACH)"
        + CMP + "(GREATER)" + CON + "(100)";
       _alertsHolder.addAlert(alert);
       return alert;
  }

  public String addExpandSumEachSumWildcardAlert() throws HelixException
  {
    String alert = EXP + "(accumulate()(dbFoo.partition*.success,dbFoo.partition*.failure)|EXPAND|SUMEACH|SUM)"
        + CMP + "(GREATER)" + CON + "(100)";
       _alertsHolder.addAlert(alert);
       return alert;
  }

  public String addArrivingSimpleStat() throws HelixException
  {
    _statsHolder.refreshStats();
    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("110","0");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();
    return incomingStatName;
  }

  public String addArrivingPairOfStats() throws HelixException
  {
    _statsHolder.refreshStats();
    String incomingStatName1 = "dbFoo.partition10.latency";
    String incomingStatName2 = "dbFoo.partition11.latency";
    Map<String, String> statFields = getStatFields("50","0");
    _statsHolder.applyStat(incomingStatName1, statFields);
    statFields = getStatFields("51","0");
    _statsHolder.applyStat(incomingStatName2, statFields);
    _statsHolder.persistStats();
    return null;
  }

  @Test (groups = {"unitTest"})
  public void testSimpleAlertFires()
  {
    String alert = addSimpleAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert);
    _statsHolder.refreshStats(); //need to refresh since not triggered by stats aggregation stage
    addArrivingSimpleStat();
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    boolean alertFired = alertResult.get(alert).get(AlertProcessor.noWildcardAlertKey).isFired();
     AssertJUnit.assertTrue(alertFired);
  }

  @Test (groups = {"unitTest"})
  public void testSimpleAlertNoStatArrivesFires()
  {
    String alert = addSimpleAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert);
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    AssertJUnit.assertEquals(null, alertResult.get(AlertProcessor.noWildcardAlertKey));
  }

  @Test (groups = {"unitTest"})
  public void testWildcardAlertFires()
  {
    String alert = addWildcardAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert);
    String incomingStatName = addArrivingSimpleStat();

    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    String wildcardBinding = incomingStatName;
    boolean alertFired = alertResult.get(alert).get(wildcardBinding).isFired();
    AssertJUnit.assertTrue(alertFired);
  }

  @Test (groups = {"unitTest"})
  public void testExpandOperatorWildcardAlertFires()
  {
    String alert = addExpandWildcardAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert);
    String incomingStatName = addArrivingSimpleStat();
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    String wildcardBinding = incomingStatName;
    boolean alertFired = alertResult.get(alert).get(wildcardBinding).isFired();
    AssertJUnit.assertTrue(alertFired);
  }

  @Test (groups = {"unitTest"})
  public void testExpandSumOperatorAlertFires()
  {
    String alert = addExpandSumAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert);
    addArrivingPairOfStats();
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    boolean alertFired = alertResult.get(alert).get(AlertProcessor.noWildcardAlertKey).isFired();
    AssertJUnit.assertTrue(alertFired);
  }
/**
 *
 * We need to re-decide how to support the feature to specify more than one stats in
 * an alert.
 *
 * Probabaly instead of
 * "(dbFoo.partition*.success,dbFoo.partition*.failure)", use the form
 * "(dbFoo.partition*.(success, failure))" as it seems that the stat source is always the
 * same.
 *
  //@Test (groups = {"unitTest"})
  public void testExpandSumOperatorWildcardAlert()
  {
    String alert = addExpandSumWildcardAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert);
    String part10SuccStat = "dbFoo.partition10.success";
    String part10FailStat = "dbFoo.partition10.failure";
    String part11SuccStat = "dbFoo.partition11.success";
    String part11FailStat = "dbFoo.partition11.failure";


    Map<String, String> statFields = getStatFields("50","0");
    _statsHolder.applyStat(part10SuccStat, statFields);
    statFields = getStatFields("51","0");
    _statsHolder.applyStat(part10FailStat, statFields);
    statFields = getStatFields("50","0");
    _statsHolder.applyStat(part11SuccStat, statFields);
    statFields = getStatFields("49","0");
    _statsHolder.applyStat(part11FailStat, statFields);
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    boolean alertFired = alertResult.get(alert).get("10").isFired(); //10 should fire
    AssertJUnit.assertTrue(alertFired);
    alertFired = alertResult.get(alert).get("11").isFired(); //11 should not fire
    AssertJUnit.assertFalse(alertFired);
  }

  //@Test (groups = {"unitTest"})
  public void testExpandSumEachSumOperatorWildcardAlert()
  {
    String alert = addExpandSumEachSumWildcardAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert);
    String part10SuccStat = "dbFoo.partition10.success";
    String part10FailStat = "dbFoo.partition10.failure";
    String part11SuccStat = "dbFoo.partition11.success";
    String part11FailStat = "dbFoo.partition11.failure";


    Map<String, String> statFields = getStatFields("50","0");
    _statsHolder.applyStat(part10SuccStat, statFields);
    statFields = getStatFields("51","0");
    _statsHolder.applyStat(part10FailStat, statFields);
    statFields = getStatFields("50","0");
    _statsHolder.applyStat(part11SuccStat, statFields);
    statFields = getStatFields("49","0");
    _statsHolder.applyStat(part11FailStat, statFields);
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    boolean alertFired = alertResult.get(alert).get(_statsHolder.getStatsList().get(0)).isFired(); //10 should fire
    AssertJUnit.assertTrue(alertFired);
  }

  //@Test (groups = {"unitTest"})
  public void testTwoAlerts()
  {
    //alert 1
    String alert1 = addSimpleAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert1);
    addArrivingSimpleStat();

    //alert 2
    String alert2 = addExpandSumWildcardAlert();
    stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert2);
    String part10SuccStat = "dbFoo.partition10.success";
    String part10FailStat = "dbFoo.partition10.failure";
    String part11SuccStat = "dbFoo.partition11.success";
    String part11FailStat = "dbFoo.partition11.failure";


    Map<String, String> statFields = getStatFields("50","0");
    _statsHolder.applyStat(part10SuccStat, statFields);
    statFields = getStatFields("51","0");
    _statsHolder.applyStat(part10FailStat, statFields);
    statFields = getStatFields("50","0");
    _statsHolder.applyStat(part11SuccStat, statFields);
    statFields = getStatFields("49","0");
    _statsHolder.applyStat(part11FailStat, statFields);
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());

    //alert 1 check
    boolean alertFired = alertResult.get(alert1).get(AlertProcessor.noWildcardAlertKey).isFired();
    AssertJUnit.assertTrue(alertFired);

    //alert 2 check
    alertFired = alertResult.get(alert2).get("10").isFired(); //10 should fire
    AssertJUnit.assertTrue(alertFired);
    alertFired = alertResult.get(alert2).get("11").isFired(); //11 should not fire
    AssertJUnit.assertFalse(alertFired);

  }
*/
  @Test (groups = {"unitTest"})
    public void testAddWildcardInFirstStatToken() throws Exception
    {
    String alert = "EXP(decay(1)(instance*.reportingage))CMP(GREATER)CON(300)";
     _alertsHolder.addAlert(alert);
     _statsHolder.persistStats();
     
     _statsHolder.refreshStats();
     //generate incoming stat
     String incomingStatName = "instance10.reportingage";
     Map<String, String> statFields = getStatFields("301","10");
     _statsHolder.refreshStats();
     
     _statsHolder.applyStat(incomingStatName, statFields);
     _statsHolder.persistStats();

     Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
     String wildcardBinding = incomingStatName;
     boolean alertFired = alertResult.get(alert).get(wildcardBinding).isFired();
     AssertJUnit.assertTrue(alertFired);
    }

  @Test (groups = {"unitTest"})
  public void testTwoWildcardAlertFires()
  {
    //error is with * and )
    String alert = addTwoWildcardAlert();
    String stat = AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert);
    String incomingStatName = "dbFoo.partition10.putCount";
    Map<String, String> statFields = getStatFields("110","0");
    _statsHolder.refreshStats();
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    String wildcardBinding = incomingStatName; //XXX: this is not going to work...need "Count" in here too.
    boolean alertFired = alertResult.get(alert).get(wildcardBinding).isFired();
    AssertJUnit.assertTrue(alertFired);
  }

  /* only supporting wildcards at end of components right now
  @Test (groups = {"unitTest"})
  public void testTwoWildcardsNotAtEndFires()
  {
    String alert = EXP + "(accumulate()(dbFoo.partition*.*Count))"
        + CMP + "(GREATER)" + CON + "(100)";
    _alertsHolder.addAlert(alert);
    String incomingStatName = "dbFoo.partition10.putCount";
    Map<String, String> statFields = getStatFields("110","0");
    _statsHolder.applyStat(incomingStatName, statFields);
    Map<String, Map<String, AlertValueAndStatus>> alertResult = AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(), _statsHolder.getStatsList());
    String wildcardBinding = "10,put"; //XXX: this is not going to work...need "Count" in here too.
    boolean alertFired = alertResult.get(alert).get(wildcardBinding).isFired();
    AssertJUnit.assertTrue(alertFired);
  }
  */

  //test using sumall
  //test using rows where some tuples are null (no stat sent)
  //test with window tuples where some windows are different lengths
  //anything else, look around at the code

  //next: review all older tests
  //next: actually write the fired alerts to ZK

}