package org.apache.helix.healthcheck;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Date;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.TestHelper.StartCMResult;
import org.apache.helix.ZNRecord;
import org.apache.helix.alerts.AlertValueAndStatus;
import org.apache.helix.api.State;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.mock.participant.MockEspressoHealthReportProvider;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestSimpleWildcardAlert extends ZkIntegrationTestBase {
  ZkClient _zkClient;
  protected ClusterSetup _setupTool = null;
  protected final String _alertStr =
      "EXP(decay(1.0)(localhost_12918.RestQueryStats@DBName=TestDB0.latency))CMP(GREATER)CON(10)";
  protected final String _alertStatusStr = _alertStr; // +" : (*)";
  protected final String _dbName = "TestDB0";

  @BeforeClass()
  public void beforeClass() throws Exception {
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());

    _setupTool = new ClusterSetup(ZK_ADDR);
  }

  @AfterClass
  public void afterClass() {
    _zkClient.close();
  }

  public class SimpleAlertTransition extends MockTransition {
    int _alertValue;

    public SimpleAlertTransition(int value) {
      _alertValue = value;
    }

    @Override
    public void doTransition(Message message, NotificationContext context) {
      HelixManager manager = context.getManager();
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      State fromState = message.getFromState();
      State toState = message.getToState();
      String instance = message.getTgtName();

      if (fromState.toString().equalsIgnoreCase("SLAVE")
          && toState.toString().equalsIgnoreCase("MASTER")) {

        // add a stat and report to ZK
        // perhaps should keep reporter per instance...
        ParticipantHealthReportCollectorImpl reporter =
            new ParticipantHealthReportCollectorImpl(manager, instance);
        MockEspressoHealthReportProvider provider = new MockEspressoHealthReportProvider();
        reporter.addHealthReportProvider(provider);
        String statName = "latency";
        provider.setStat(_dbName, statName, "" + (0.1 + _alertValue));
        reporter.transmitHealthReports();

        /*
         * for (int i = 0; i < 5; i++)
         * {
         * accessor.setProperty(PropertyType.HEALTHREPORT,
         * new ZNRecord("mockAlerts" + i),
         * instance,
         * "mockAlerts");
         * try
         * {
         * Thread.sleep(1000);
         * }
         * catch (InterruptedException e)
         * {
         * // TODO Auto-generated catch block
         * e.printStackTrace();
         * }
         * }
         */
      }
    }

  }

  @Test()
  public void testSimpleWildcardAlert() throws Exception {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START testSimpleWildcardAlert at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12944, // participant start port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes //change back to 5!!!
        3, // replicas //change back to 3!!!
        "MasterSlave", true); // do rebalance

    // enableHealthCheck(clusterName);

    StartCMResult cmResult =
        TestHelper.startController(clusterName, "controller_0", ZK_ADDR,
            HelixControllerMain.STANDALONE);
    cmResult._manager.stopTimerTasks();

    String alertwildcard =
        "EXP(decay(1.0)(localhost*.RestQueryStats@DBName=TestDB0.latency))CMP(GREATER)CON(10)";

    _setupTool.getClusterManagementTool().addAlert(clusterName, alertwildcard);
    // start participants
    for (int i = 0; i < 5; i++) // !!!change back to 5
    {
      String instanceName = "localhost_" + (12944 + i);

      participants[i] =
          new MockParticipant(clusterName, instanceName, ZK_ADDR, new SimpleAlertTransition(i * 5));
      participants[i].syncStart();
      // new Thread(participants[i]).start();
    }

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    Thread.sleep(1000);
    // HealthAggregationTask is supposed to run by a timer every 30s
    // To make sure HealthAggregationTask is run, we invoke it explicitly for this test
    new HealthStatsAggregator(cmResult._manager).aggregate();
    // sleep for a few seconds to give stats stage time to trigger
    Thread.sleep(1000);

    // other verifications go here
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    ZNRecord record = accessor.getProperty(keyBuilder.alertStatus()).getRecord();
    Map<String, Map<String, String>> recMap = record.getMapFields();
    for (int i = 0; i < 2; i++) {
      String alertString = "(localhost_" + (12944 + i) + ".RestQueryStats@DBName=TestDB0.latency)";
      Map<String, String> alertStatusMap = recMap.get(alertwildcard + " : " + alertString);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), (double) i * 5 + 0.1);
      Assert.assertFalse(fired);
    }
    for (int i = 2; i < 5; i++) {
      String alertString = "(localhost_" + (12944 + i) + ".RestQueryStats@DBName=TestDB0.latency)";
      Map<String, String> alertStatusMap = recMap.get(alertwildcard + " : " + alertString);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), (double) i * 5 + 0.1);
      Assert.assertTrue(fired);
    }
    ZNRecord alertHistory = accessor.getProperty(keyBuilder.alertHistory()).getRecord();

    String deltakey = (String) (alertHistory.getMapFields().keySet().toArray()[0]);
    Map<String, String> delta = alertHistory.getMapField(deltakey);
    Assert.assertEquals(delta.size(), 3);
    for (int i = 2; i < 5; i++) {
      String alertString =
          "(localhost_" + (12944 + i) + ".RestQueryStats@DBName#TestDB0.latency)GREATER(10)";
      Assert.assertTrue(delta.get(alertString).equals("ON"));
    }

    // Drop and add another alert
    _setupTool.getClusterManagementTool().dropAlert(clusterName, alertwildcard);
    alertwildcard =
        "EXP(decay(1.0)(localhost*.RestQueryStats@DBName=TestDB0.latency))CMP(GREATER)CON(15)";
    _setupTool.getClusterManagementTool().addAlert(clusterName, alertwildcard);
    new HealthStatsAggregator(cmResult._manager).aggregate();
    Thread.sleep(1000);

    record = accessor.getProperty(keyBuilder.alertStatus()).getRecord();
    recMap = record.getMapFields();
    for (int i = 0; i < 3; i++) {
      String alertString = "(localhost_" + (12944 + i) + ".RestQueryStats@DBName=TestDB0.latency)";
      Map<String, String> alertStatusMap = recMap.get(alertwildcard + " : " + alertString);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), (double) i * 5 + 0.1);
      Assert.assertFalse(fired);
    }
    for (int i = 3; i < 5; i++) {
      String alertString = "(localhost_" + (12944 + i) + ".RestQueryStats@DBName=TestDB0.latency)";
      Map<String, String> alertStatusMap = recMap.get(alertwildcard + " : " + alertString);
      String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
      boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
      Assert.assertEquals(Double.parseDouble(val), (double) i * 5 + 0.1);
      Assert.assertTrue(fired);
    }
    alertHistory = accessor.getProperty(keyBuilder.alertHistory()).getRecord();

    deltakey = (String) (alertHistory.getMapFields().keySet().toArray()[1]);
    delta = alertHistory.getMapField(deltakey);
    Assert.assertTrue(delta.size() == 2);
    for (int i = 3; i < 5; i++) {
      String alertString =
          "(localhost_" + (12944 + i) + ".RestQueryStats@DBName#TestDB0.latency)GREATER(15)";
      Assert.assertTrue(delta.get(alertString).equals("ON"));
    }

    System.out.println("END testSimpleWildcardAlert at " + new Date(System.currentTimeMillis()));
  }
}
