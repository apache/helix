package org.apache.helix.integration;

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

import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixInstanceTag extends ZkStandAloneCMTestBase {
  @Test
  public void testInstanceTag() throws Exception {
    HelixManager manager = _controller;
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    String DB2 = "TestDB2";
    int partitions = 100;
    String DB2tag = "TestDB2_tag";
    int replica = 2;
    for (int i = 0; i < 2; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _setupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, instanceName, DB2tag);
    }
    _setupTool.addResourceToCluster(CLUSTER_NAME, DB2, partitions, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, DB2, DB2tag, replica);

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                CLUSTER_NAME)));
    Assert.assertTrue(result, "Cluster verification fails");

    ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView(DB2));
    Set<String> hosts = new HashSet<String>();
    for (String p : ev.getPartitionSet()) {
      for (String hostName : ev.getStateMap(p).keySet()) {
        InstanceConfig config =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(hostName));
        Assert.assertTrue(config.containsTag(DB2tag));
        hosts.add(hostName);
      }
    }
    Assert.assertEquals(hosts.size(), 2);

    String DB3 = "TestDB3";
    String DB3Tag = "TestDB3_tag";
    partitions = 10;
    replica = 3;
    for (int i = 1; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _setupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, instanceName, DB3Tag);
    }
    _setupTool.addResourceToCluster(CLUSTER_NAME, DB3, partitions, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, DB3, DB3Tag, replica);

    result =
        ClusterStateVerifier
            .verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                CLUSTER_NAME)));
    Assert.assertTrue(result, "Cluster verification fails");

    ev = accessor.getProperty(accessor.keyBuilder().externalView(DB3));
    hosts = new HashSet<String>();
    for (String p : ev.getPartitionSet()) {
      for (String hostName : ev.getStateMap(p).keySet()) {
        InstanceConfig config =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(hostName));
        Assert.assertTrue(config.containsTag(DB3Tag));
        hosts.add(hostName);
      }
    }
    Assert.assertEquals(hosts.size(), 4);
  }
}
