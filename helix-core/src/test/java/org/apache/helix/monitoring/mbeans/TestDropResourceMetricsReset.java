package org.apache.helix.monitoring.mbeans;

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

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDropResourceMetricsReset extends ZkTestBase {
  private final CountDownLatch _registerLatch = new CountDownLatch(1);
  private final CountDownLatch _unregisterLatch = new CountDownLatch(1);

  @Test
  public void testBasic() throws Exception {
    final int NUM_PARTICIPANTS = 4;
    final int NUM_PARTITIONS = 64;
    final int NUM_REPLICAS = 1;
    final String RESOURCE_NAME = "TestDB0";

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ParticipantMonitorListener listener =
        new ParticipantMonitorListener("ClusterStatus", clusterName, RESOURCE_NAME);

    // Set up cluster
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "MasterSlave", RebalanceMode.FULL_AUTO, // use FULL_AUTO mode to test node tagging
        true); // do rebalance

    // Start participants and controller
    ClusterSetup setupTool = new ClusterSetup(_zkclient);
    MockParticipant[] participants = new MockParticipant[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      participants[i] =
          new MockParticipant(_zkaddr, clusterName, "localhost_" + (12918 + i));
      participants[i].syncStart();
    }
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // Verify that the bean was created
    boolean noTimeout = _registerLatch.await(30000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(noTimeout);

    // Drop the resource
    setupTool.dropResourceFromCluster(clusterName, RESOURCE_NAME);

    // Verify that the bean was removed
    noTimeout = _unregisterLatch.await(30000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(noTimeout);

    // Clean up
    listener.disconnect();
    controller.syncStop();
    for (MockParticipant participant : participants) {
      participant.syncStop();
    }
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private ObjectName getObjectName(String resourceName, String clusterName)
      throws MalformedObjectNameException {
    String clusterBeanName =
        String.format("%s=%s", ClusterStatusMonitor.CLUSTER_DN_KEY, clusterName);
    String resourceBeanName =
        String.format("%s,%s=%s", clusterBeanName, ClusterStatusMonitor.RESOURCE_DN_KEY,
            resourceName);
    return new ObjectName(String.format("%s: %s", ClusterStatusMonitor.CLUSTER_STATUS_KEY,
        resourceBeanName));
  }

  private class ParticipantMonitorListener extends ClusterMBeanObserver {
    private final ObjectName _objectName;

    public ParticipantMonitorListener(String domain, String cluster, String resource)
        throws InstanceNotFoundException, IOException, MalformedObjectNameException,
        NullPointerException {
      super(domain);
      _objectName = getObjectName(resource, cluster);
    }

    @Override
    public void onMBeanRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      if (mbsNotification.getMBeanName().equals(_objectName)) {
        _registerLatch.countDown();
      }
    }

    @Override
    public void onMBeanUnRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      if (mbsNotification.getMBeanName().equals(_objectName)) {
        _unregisterLatch.countDown();
      }
    }
  }
}
