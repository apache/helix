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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDummyAlerts extends ZkIntegrationTestBase
{
  public class DummyAlertsTransition extends MockTransition
  {
    private final AtomicBoolean _done = new AtomicBoolean(false);

    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      HelixManager manager = context.getManager();
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();
      
      String instance = message.getTgtName();
      if (_done.getAndSet(true) == false)
      {
        for (int i = 0; i < 5; i++)
        {
//          System.out.println(instance + " sets healthReport: " + "mockAlerts" + i);
          accessor.setProperty(keyBuilder.healthReport(instance, "mockAlerts"),
                               new HealthStat(new ZNRecord("mockAlerts" + i)));
        }
      }
    }

  }

  @Test()
  public void testDummyAlerts() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    MockParticipant[] participants = new MockParticipant[n];

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start
                                                         // port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    enableHealthCheck(clusterName);
    setupTool.getClusterManagementTool()
             .addAlert(clusterName,
                       "EXP(decay(1.0)(*.defaultPerfCounters@defaultPerfCounters.availableCPUs))CMP(GREATER)CON(2)");

    // start controller
    ClusterController controller =
        new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    
    // start participants
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] =
          new MockParticipant(clusterName,
                              instanceName,
                              ZK_ADDR,
                              new DummyAlertsTransition());
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // other verifications go here
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    for (int i = 0; i < n; i++)
    {
      String instance = "localhost_" + (12918 + i);
      ZNRecord record = null;
      for(int j = 0; j < 10; j++)
      {
        record =
            accessor.getProperty(keyBuilder.healthReport(instance, "mockAlerts")).getRecord();
        if(record.getId().equals("mockAlerts4"))
        {
          break;
        }
        else
        {
          Thread.sleep(500);
        }
      }
      Assert.assertEquals(record.getId(), "mockAlerts4");
    }

    // clean up
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }
    
    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
