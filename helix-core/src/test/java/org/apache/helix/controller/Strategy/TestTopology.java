package org.apache.helix.controller.Strategy;

import org.apache.helix.HelixProperty;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class TestTopology {
  private static Logger logger = Logger.getLogger(TestAutoRebalanceStrategy.class);

  @Test
  public void testCreateClusterTopology() {
    ClusterConfig clusterConfig = new ClusterConfig("Test_Cluster");

    String topology = "/Rack/Sub-Rack/Host/Instance";
    clusterConfig.getRecord().getSimpleFields()
        .put(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name(), topology);
    clusterConfig.getRecord().getSimpleFields()
        .put(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name(), "Sub-Rack");

    List<String> allNodes = new ArrayList<String>();
    List<String> liveNodes = new ArrayList<String>();
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<String, InstanceConfig>();

    Map<String, Integer> nodeToWeightMap = new HashMap<String, Integer>();

    for (int i = 0; i < 100; i++) {
      String instance = "localhost_" + i;
      InstanceConfig config = new InstanceConfig(instance);
      String rack_id = "rack_" + i/25;
      String sub_rack_id = "subrack-" + i/5;

      String domain =
          String.format("Rack=%s, Sub-Rack=%s, Host=%s", rack_id, sub_rack_id, instance);
      config.setDomain(domain);
      config.setHostName(instance);
      config.setPort("9000");
      allNodes.add(instance);

      int weight = 0;
      if (i % 10 != 0) {
        liveNodes.add(instance);
        weight = 1000;
        if (i % 3 == 0) {
          // set random instance weight.
          weight = (i+1) * 100;
          config.setWeight(weight);
        }
      }

      instanceConfigMap.put(instance, config);

      if (!nodeToWeightMap.containsKey(rack_id)) {
        nodeToWeightMap.put(rack_id, 0);
      }
      nodeToWeightMap.put(rack_id, nodeToWeightMap.get(rack_id) + weight);
      if (!nodeToWeightMap.containsKey(sub_rack_id)) {
        nodeToWeightMap.put(sub_rack_id, 0);
      }
      nodeToWeightMap.put(sub_rack_id, nodeToWeightMap.get(sub_rack_id) + weight);
    }

    Topology topo = new Topology(allNodes, liveNodes, instanceConfigMap, clusterConfig);

    Assert.assertTrue(topo.getEndNodeType().equals("Instance"));
    Assert.assertTrue(topo.getFaultZoneType().equals("Sub-Rack"));

    List<Node> faultZones = topo.getFaultZones();
    Assert.assertEquals(faultZones.size(), 20);

    Node root = topo.getRootNode();

    Assert.assertEquals(root.getChildrenCount("Rack"), 4);
    Assert.assertEquals(root.getChildrenCount("Sub-Rack"), 20);
    Assert.assertEquals(root.getChildrenCount("Host"), 100);
    Assert.assertEquals(root.getChildrenCount("Instance"), 100);


    // validate weights.
    for (Node rack : root.getChildren()) {
      Assert.assertEquals(rack.getWeight(), (long)nodeToWeightMap.get(rack.getName()));
      for (Node subRack : rack.getChildren()) {
        Assert.assertEquals(subRack.getWeight(), (long)nodeToWeightMap.get(subRack.getName()));
      }
    }
  }

  @Test
  public void testCreateClusterTopologyWithDefaultTopology() {
    ClusterConfig clusterConfig = new ClusterConfig("Test_Cluster");

    List<String> allNodes = new ArrayList<String>();
    List<String> liveNodes = new ArrayList<String>();
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<String, InstanceConfig>();

    Map<String, Integer> nodeToWeightMap = new HashMap<String, Integer>();

    for (int i = 0; i < 100; i++) {
      String instance = "localhost_" + i;
      InstanceConfig config = new InstanceConfig(instance);
      String zoneId = "rack_" + i / 10;
      config.setZoneId(zoneId);
      config.setHostName(instance);
      config.setPort("9000");
      allNodes.add(instance);

      int weight = 0;
      if (i % 10 != 0) {
        liveNodes.add(instance);
        weight = 1000;
        if (i % 3 == 0) {
          // set random instance weight.
          weight = (i + 1) * 100;
          config.setWeight(weight);
        }
      }

      instanceConfigMap.put(instance, config);

      if (!nodeToWeightMap.containsKey(zoneId)) {
        nodeToWeightMap.put(zoneId, 0);
      }
      nodeToWeightMap.put(zoneId, nodeToWeightMap.get(zoneId) + weight);
    }

    Topology topo = new Topology(allNodes, liveNodes, instanceConfigMap, clusterConfig);

    Assert.assertTrue(topo.getEndNodeType().equals(Topology.Types.INSTANCE.name()));
    Assert.assertTrue(topo.getFaultZoneType().equals(Topology.Types.ZONE.name()));

    List<Node> faultZones = topo.getFaultZones();
    Assert.assertEquals(faultZones.size(), 10);

    Node root = topo.getRootNode();

    Assert.assertEquals(root.getChildrenCount(Topology.Types.ZONE.name()), 10);
    Assert.assertEquals(root.getChildrenCount(topo.getEndNodeType()), 100);

    // validate weights.
    for (Node rack : root.getChildren()) {
      Assert.assertEquals(rack.getWeight(), (long) nodeToWeightMap.get(rack.getName()));
    }
  }
}
