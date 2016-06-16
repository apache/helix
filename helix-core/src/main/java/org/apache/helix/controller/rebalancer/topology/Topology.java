package org.apache.helix.controller.rebalancer.topology;

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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;


/**
 * Topology represents the structure of a cluster (the hierarchy of the nodes, its fault boundary, etc).
 * This class is intended for topology-aware partition placement.
 */
public class Topology {
  private static Logger logger = Logger.getLogger(Topology.class);
  public enum Types {
    ROOT,
    ZONE,
    INSTANCE
  }
  private static final int DEFAULT_NODE_WEIGHT = 1000;

  private final MessageDigest md;
  private Node _root; // root of the tree structure of all nodes;
  private List<String> _allInstances;
  private List<String> _liveInstances;
  private Map<String, InstanceConfig> _instanceConfigMap;

  public Topology(final List<String> allNodes, final List<String> liveNodes,
      final Map<String, InstanceConfig> instanceConfigMap) {
    try {
      md = MessageDigest.getInstance("SHA-1");
      _allInstances = allNodes;
      _liveInstances = liveNodes;
      _instanceConfigMap = instanceConfigMap;
    } catch (NoSuchAlgorithmException ignore) {
      throw new IllegalArgumentException(ignore);
    }
    createClusterTopology();
  }

  public String getEndNodeType() {
    return Types.INSTANCE.name();
  }

  public String getFaultZoneType() {
    return Types.ZONE.name();
  }

  public Node getRootNode() {
    return _root;
  }

  public void createClusterTopology() {
    Map<String, List<String>> zoneInstances = new HashMap<String, List<String>>();
    Map<String, Integer> instanceWeights = new HashMap<String, Integer>();
    for (String ins : _allInstances) {
      InstanceConfig config = _instanceConfigMap.get(ins);
      if (config == null) {
        throw new HelixException(String.format("Config for instance %s is not found!", ins));
      }
      String zone = config.getZoneId();
      if (zone == null) {
        //TODO: we should allow non-rack cluster for back-compatible. This should be solved once we
        //have the hierarchy style of domain id for instance.
        throw new HelixException(String
            .format("ZONE_ID for instance %s is not set, failed the topology-aware placement!",
                ins));
      }
      if (!zoneInstances.containsKey(zone)) {
        zoneInstances.put(zone, new ArrayList<String>());
      }
      zoneInstances.get(zone).add(ins);

      int weight = config.getWeight();
      if (weight < 0 || weight == InstanceConfig.WEIGHT_NOT_SET) {
        weight = DEFAULT_NODE_WEIGHT;
      }
      instanceWeights.put(ins, weight);
    }

    _root = makeClusterTree(zoneInstances, instanceWeights, _liveInstances);
  }

  /**
   * Creates a tree representing the cluster structure.
   */
  private Node makeClusterTree(Map<String, List<String>> zoneInstances,
      Map<String, Integer> instanceWeights, List<String> liveInstances) {
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(computeId("root"));
    root.setType(Types.ROOT.name());
    // zones
    List<Node> zones = new ArrayList<Node>();
    for (Map.Entry<String, List<String>> e: zoneInstances.entrySet()) {
      String zoneName = e.getKey();
      List<String> instanceNames = e.getValue();
      Node zone = new Node();
      zones.add(zone);
      zone.setName(zoneName);
      zone.setId(computeId(zoneName));
      zone.setType(Types.ZONE.name());
      zone.setParent(root);

      int zoneWeight = 0;
      // instance nodes
      List<Node> instances = new ArrayList<Node>();
      for (String insName : instanceNames) {
        Node ins = new Node();
        instances.add(ins);
        ins.setName(insName);
        ins.setId(computeId(insName));
        ins.setType(Types.INSTANCE.name());
        ins.setParent(zone);
        if (liveInstances.contains(insName)) {
          // node is alive
          int weight = instanceWeights.get(insName);
          ins.setWeight(weight);
          zoneWeight += weight;
        } else {
          ins.setFailed(true);
          ins.setWeight(0);
        }
      }
      zone.setChildren(instances);
      zone.setWeight(zoneWeight);
    }
    root.setChildren(zones);
    computeWeight(root);
    return root;
  }

  private long computeId(String name) {
    byte[] h = md.digest(name.getBytes());
    return bstrTo32bit(h);
  }

  private void computeWeight(Node node) {
    int weight = 0;
    for (Node child : node.getChildren()) {
      weight += child.getWeight();
    }
    node.setWeight(weight);
  }

  private long bstrTo32bit(byte[] bstr) {
    if (bstr.length < 4) {
      throw new IllegalArgumentException("hashed is less than 4 bytes!");
    }
    // need to "simulate" unsigned int
    return (long) (((ord(bstr[0]) << 24) | (ord(bstr[1]) << 16) | (ord(bstr[2]) << 8) | (ord(
        bstr[3])))) & 0xffffffffL;
  }

  private int ord(byte b) {
    return b & 0xff;
  }
}
