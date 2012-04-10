package com.linkedin.helix.integration;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.alerts.AlertValueAndStatus;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.healthcheck.HealthStatsAggregationTask;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollectorImpl;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.mock.storage.MockEspressoHealthReportProvider;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockTransition;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestSimpleAlert extends ZkIntegrationTestBase
{
  ZkClient _zkClient;
  protected ClusterSetup _setupTool = null;
  protected final String _alertStr = "EXP(decay(1.0)(localhost_12918.RestQueryStats@DBName=TestDB0.latency))CMP(GREATER)CON(10)";
  protected final String _alertStatusStr = _alertStr; //+" : (*)";
  protected final String _dbName = "TestDB0";

  @BeforeClass ()
  public void beforeClass() throws Exception
  {
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());

    _setupTool = new ClusterSetup(ZK_ADDR);
  }

  @AfterClass
  public void afterClass()
  {
    _zkClient.close();
  }

  public class SimpleAlertTransition extends MockTransition
  {
    int _alertValue;
    public SimpleAlertTransition(int value)
    {
      _alertValue = value;
    }
    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      HelixManager manager = context.getManager();
      DataAccessor accessor = manager.getDataAccessor();
      String fromState = message.getFromState();
      String toState = message.getToState();
      String instance = message.getTgtName();
      String partition = message.getPartitionName();

      if (fromState.equalsIgnoreCase("SLAVE")
          && toState.equalsIgnoreCase("MASTER"))
      {

    	//add a stat and report to ZK
    	//perhaps should keep reporter per instance...
    	ParticipantHealthReportCollectorImpl reporter =
    			new ParticipantHealthReportCollectorImpl(manager, instance);
    	MockEspressoHealthReportProvider provider = new
    			MockEspressoHealthReportProvider();
    	reporter.addHealthReportProvider(provider);
    	String statName = "latency";
    	provider.setStat(_dbName, statName,""+(0.1+_alertValue));
    	reporter.transmitHealthReports();

    	/*
        for (int i = 0; i < 5; i++)
        {
          accessor.setProperty(PropertyType.HEALTHREPORT,
                               new ZNRecord("mockAlerts" + i),
                               instance,
                               "mockAlerts");
          try
          {
            Thread.sleep(1000);
          }
          catch (InterruptedException e)
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        */
      }
    }

  }

  @Test()
  public void testSimpleAlert() throws Exception
  {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START TestSimpleAlert at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName,
                            ZK_ADDR,
                            12918,        // participant start port
                            "localhost",  // participant name prefix
                            "TestDB",     // resource  name prefix
                            1,            // resources
                            10,           // partitions per resource
                            5,            // number of nodes //change back to 5!!!
                            3,            // replicas //change back to 3!!!
                            "MasterSlave",
                            true);        // do rebalance

    // enableHealthCheck(clusterName);

    _setupTool.getClusterManagementTool().addAlert(clusterName, _alertStr);

    StartCMResult cmResult = TestHelper.startController(clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);
    // start participants
    for (int i = 0; i < 5; i++) //!!!change back to 5
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName,
                                            instanceName,
                                            ZK_ADDR,
                                            new SimpleAlertTransition(15));
      new Thread(participants[i]).start();
    }

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // HealthAggregationTask is supposed to run by a timer every 30s
    // To make sure HealthAggregationTask is run, we invoke it explicitly for this test
    new HealthStatsAggregationTask(cmResult._manager).run();
    //sleep for a few seconds to give stats stage time to trigger
    Thread.sleep(3000);

    // other verifications go here
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    //for (int i = 0; i < 1; i++) //change 1 back to 5
    //{
      //String instance = "localhost_" + (12918 + i);
      String instance = "localhost_12918";
      ZNRecord record = accessor.getProperty(PropertyType.ALERT_STATUS);
      Map<String, Map<String,String>> recMap = record.getMapFields();
      Set<String> keySet = recMap.keySet();
      Map<String,String> alertStatusMap = recMap.get(_alertStatusStr);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), Double.parseDouble("15.1"));
      Assert.assertTrue(fired);
      
      // Verify Alert history from ZK
      ZNRecord alertHistory = accessor.getProperty(PropertyType.ALERT_HISTORY);
      
      String deltakey = (String) (alertHistory.getMapFields().keySet().toArray()[0]);
      Map<String, String> delta = alertHistory.getMapField(deltakey);
      Assert.assertTrue(delta.size() == 1);
      Assert.assertTrue(delta.get("EXP(decay(1.0)(localhost_12918.RestQueryStats@DBName#TestDB0.latency))CMP(GREATER)CON(10)--(%)").equals("ON"));
    //}

    System.out.println("END TestSimpleAlert at " + new Date(System.currentTimeMillis()));
  }
  

  @Test()
  public void testSimpleWildcardAlert() throws Exception
  {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START TestSimpleAlert at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName,
                            ZK_ADDR,
                            12918,        // participant start port
                            "localhost",  // participant name prefix
                            "TestDB",     // resource  name prefix
                            1,            // resources
                            10,           // partitions per resource
                            5,            // number of nodes //change back to 5!!!
                            3,            // replicas //change back to 3!!!
                            "MasterSlave",
                            true);        // do rebalance

    // enableHealthCheck(clusterName);
    String alertwildcard = "EXP(decay(1.0)(localhost*.RestQueryStats@DBName=TestDB0.latency))CMP(GREATER)CON(10)";
    _setupTool.getClusterManagementTool().addAlert(clusterName, alertwildcard);

    StartCMResult cmResult = TestHelper.startController(clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);
    // start participants
    for (int i = 0; i < 5; i++) //!!!change back to 5
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName,
                                            instanceName,
                                            ZK_ADDR,
                                            new SimpleAlertTransition(i * 5));
      new Thread(participants[i]).start();
    }

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // HealthAggregationTask is supposed to run by a timer every 30s
    // To make sure HealthAggregationTask is run, we invoke it explicitly for this test
    new HealthStatsAggregationTask(cmResult._manager).run();
    //sleep for a few seconds to give stats stage time to trigger
    Thread.sleep(1000);

    // other verifications go here
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    ZNRecord record = accessor.getProperty(PropertyType.ALERT_STATUS);
    Map<String, Map<String,String>> recMap = record.getMapFields();
    for(int i = 0; i < 2; i++)
    {
      String alertString = "(localhost_"+(12918 + i)+".RestQueryStats@DBName=TestDB0.latency)";
      Map<String,String> alertStatusMap = recMap.get(alertwildcard+" : " + alertString);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), (double)i * 5 + 0.1);
      Assert.assertFalse(fired);
    }
    for(int i = 2; i < 5; i++)
    {
      String alertString = "(localhost_"+(12918 + i)+".RestQueryStats@DBName=TestDB0.latency)";
      Map<String,String> alertStatusMap = recMap.get(alertwildcard+" : " + alertString);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), (double)i * 5 + 0.1);
      Assert.assertTrue(fired);
    }
    ZNRecord alertHistory = accessor.getProperty(PropertyType.ALERT_HISTORY);
    
    String deltakey = (String) (alertHistory.getMapFields().keySet().toArray()[0]);
    Map<String, String> delta = alertHistory.getMapField(deltakey);
    Assert.assertTrue(delta.size() == 3);
    for(int i = 2; i < 5; i++)
    {
      String alertString = "(localhost_"+(12918 + i)+".RestQueryStats@DBName#TestDB0.latency)GREATER(10)";
      Assert.assertTrue(delta.get(alertString).equals("ON"));
    }
    
    // Drop and add another alert
    _setupTool.getClusterManagementTool().dropAlert(clusterName, alertwildcard);
    alertwildcard = "EXP(decay(1.0)(localhost*.RestQueryStats@DBName=TestDB0.latency))CMP(GREATER)CON(15)";
    _setupTool.getClusterManagementTool().addAlert(clusterName, alertwildcard);
    new HealthStatsAggregationTask(cmResult._manager).run();
    Thread.sleep(1000);
    
    record = accessor.getProperty(PropertyType.ALERT_STATUS);
    recMap = record.getMapFields();
    for(int i = 0; i < 3; i++)
    {
      String alertString = "(localhost_"+(12918 + i)+".RestQueryStats@DBName=TestDB0.latency)";
      Map<String,String> alertStatusMap = recMap.get(alertwildcard+" : " + alertString);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), (double)i * 5 + 0.1);
      Assert.assertFalse(fired);
    }
    for(int i = 3; i < 5; i++)
    {
      String alertString = "(localhost_"+(12918 + i)+".RestQueryStats@DBName=TestDB0.latency)";
      Map<String,String> alertStatusMap = recMap.get(alertwildcard+" : " + alertString);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), (double)i * 5 + 0.1);
      Assert.assertTrue(fired);
    }
    alertHistory = accessor.getProperty(PropertyType.ALERT_HISTORY);
    
    deltakey = (String) (alertHistory.getMapFields().keySet().toArray()[1]);
    delta = alertHistory.getMapField(deltakey);
    Assert.assertTrue(delta.size() == 2);
    for(int i = 3; i < 5; i++)
    {
      String alertString = "(localhost_"+(12918 + i)+".RestQueryStats@DBName#TestDB0.latency)GREATER(15)";
      Assert.assertTrue(delta.get(alertString).equals("ON"));
    }
    
    
    //}

    System.out.println("END TestSimpleAlert at " + new Date(System.currentTimeMillis()));
  }
}
