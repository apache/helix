/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.integration;

import java.util.Date;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.manager.zk.ZKHelixManager;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestStandAloneCMSessionExpiry extends ZkIntegrationTestBase
{
  private static Logger      LOG          =
                                              Logger.getLogger(TestStandAloneCMSessionExpiry.class);
  protected final String     CLUSTER_NAME = "CLUSTER_" + "TestStandAloneCMSessionExpiry";
  protected static final int NODE_NR      = 5;

  class ZkClusterManagerWithSessionExpiry extends ZKHelixManager
  {
    public ZkClusterManagerWithSessionExpiry(String clusterName,
                                             String instanceName,
                                             InstanceType instanceType,
                                             String zkConnectString) throws Exception
    {
      super(clusterName, instanceName, instanceType, zkConnectString);
    }

    public void expireSession() throws Exception
    {
      ZkIntegrationTestBase.simulateSessionExpiry(_zkClient);
    }
    
    public long getZkSessionId()
    {
      ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
      ZooKeeper zookeeper = connection.getZookeeper();

      return zookeeper.getSessionId();
    }
  }

  @Test()
  public void testStandAloneCMSessionExpiry() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("RUN testStandAloneCMSessionExpiry() at "
        + new Date(System.currentTimeMillis()));

    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);

    TestHelper.setupCluster(CLUSTER_NAME,
                            ZK_ADDR,
                            12918,
                            PARTICIPANT_PREFIX,
                            "TestDB",
                            1,
                            20,
                            NODE_NR,
                            3,
                            "MasterSlave",
                            true);

    MockParticipant[] participants = new MockParticipant[NODE_NR];
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
      HelixManager manager =
          new ZkClusterManagerWithSessionExpiry(CLUSTER_NAME,
                                                instanceName,
                                                InstanceType.PARTICIPANT,
                                                ZK_ADDR);
      participants[i] = new MockParticipant(manager);
      new Thread(participants[i]).start();
    }
    ZkClusterManagerWithSessionExpiry controller =
        new ZkClusterManagerWithSessionExpiry(CLUSTER_NAME,
                                              "controller_0",
                                              InstanceType.CONTROLLER,
                                              ZK_ADDR);
    controller.connect();

    boolean result;
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                          CLUSTER_NAME));
    Assert.assertTrue(result);

    // participant session expiry
    ZkClusterManagerWithSessionExpiry manager =
        (ZkClusterManagerWithSessionExpiry) participants[0].getManager();
    long oldSessionId = manager.getZkSessionId();
    manager.expireSession();
    long newSessionId = manager.getZkSessionId();
    Assert.assertNotSame(newSessionId, oldSessionId);

    setupTool.addResourceToCluster(CLUSTER_NAME, "TestDB1", 10, "MasterSlave");
    setupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB1", 3);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                          CLUSTER_NAME));
    Assert.assertTrue(result);

    // controller session expiry
    oldSessionId = controller.getZkSessionId();
    controller.expireSession();
    newSessionId = controller.getZkSessionId();
    Assert.assertNotSame(newSessionId, oldSessionId);
    setupTool.addResourceToCluster(CLUSTER_NAME, "TestDB2", 8, "MasterSlave");
    setupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB2", 3);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                          CLUSTER_NAME));
    Assert.assertTrue(result);

    // clean up
    controller.disconnect();
    for (int i = 0; i < NODE_NR; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("STOP testStandAloneCMSessionExpiry() at "
        + new Date(System.currentTimeMillis()));
  }

}
