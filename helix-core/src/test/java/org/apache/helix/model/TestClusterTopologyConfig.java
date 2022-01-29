package org.apache.helix.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Iterator;
import org.apache.helix.HelixException;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestClusterTopologyConfig {

  @Test
  public void testClusterNonTopologyAware() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setTopologyAwareEnabled(false);
    ClusterTopologyConfig clusterTopologyConfig = ClusterTopologyConfig.createFromClusterConfig(testConfig);
    Assert.assertEquals(clusterTopologyConfig.getEndNodeType(), Topology.Types.INSTANCE.name());
    Assert.assertEquals(clusterTopologyConfig.getFaultZoneType(), Topology.Types.INSTANCE.name());
    Assert.assertTrue(clusterTopologyConfig.getTopologyKeyDefaultValue().isEmpty());
  }

  @Test
  public void testClusterValidTopology() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setTopologyAwareEnabled(true);
    testConfig.setTopology("/zone/instance");
    // no fault zone setup
    ClusterTopologyConfig clusterTopologyConfig = ClusterTopologyConfig.createFromClusterConfig(testConfig);
    Assert.assertEquals(clusterTopologyConfig.getEndNodeType(), "instance");
    Assert.assertEquals(clusterTopologyConfig.getFaultZoneType(), "instance");
    Assert.assertEquals(clusterTopologyConfig.getTopologyKeyDefaultValue().size(), 2);
    // with fault zone
    testConfig.setFaultZoneType("zone");
    testConfig.setTopology(" /zone/instance  ");
    clusterTopologyConfig = ClusterTopologyConfig.createFromClusterConfig(testConfig);
    Assert.assertEquals(clusterTopologyConfig.getEndNodeType(), "instance");
    Assert.assertEquals(clusterTopologyConfig.getFaultZoneType(), "zone");
    Assert.assertEquals(clusterTopologyConfig.getTopologyKeyDefaultValue().size(), 2);
    String[] keys = new String[] {"zone", "instance"};
    Iterator<String> itr = clusterTopologyConfig.getTopologyKeyDefaultValue().keySet().iterator();
    for (String k : keys) {
      Assert.assertEquals(k, itr.next());
    }

    testConfig.setTopology("/rack/zone/instance");
    clusterTopologyConfig = ClusterTopologyConfig.createFromClusterConfig(testConfig);
    Assert.assertEquals(clusterTopologyConfig.getEndNodeType(), "instance");
    Assert.assertEquals(clusterTopologyConfig.getFaultZoneType(), "zone");
    Assert.assertEquals(clusterTopologyConfig.getTopologyKeyDefaultValue().size(), 3);
    keys = new String[] {"rack", "zone", "instance"};
    itr = clusterTopologyConfig.getTopologyKeyDefaultValue().keySet().iterator();
    for (String k : keys) {
      Assert.assertEquals(k, itr.next());
    }
  }

  @Test(expectedExceptions = HelixException.class)
  public void testClusterInvalidTopology() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setTopologyAwareEnabled(true);
    testConfig.setTopology("/zone/instance");
    testConfig.setFaultZoneType("rack");
    ClusterTopologyConfig.createFromClusterConfig(testConfig);
  }
}
