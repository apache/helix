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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.model.ClusterConfig;
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

  private final MessageDigest _md;
  private Node _root; // root of the tree structure of all nodes;
  private List<String> _allInstances;
  private List<String> _liveInstances;
  private Map<String, InstanceConfig> _instanceConfigMap;
  private ClusterConfig _clusterConfig;
  private String _faultZoneType;
  private String _endNodeType;
  private boolean _useDefaultTopologyDef;
  private LinkedHashSet<String> _types;

  /* default names for domain paths, if value is not specified for a domain path, the default one is used */
  // TODO: default values can be defined in clusterConfig.
  private Map<String, String> _defaultDomainPathValues = new HashMap<String, String>();

  public Topology(final List<String> allNodes, final List<String> liveNodes,
      final Map<String, InstanceConfig> instanceConfigMap, ClusterConfig clusterConfig) {
    try {
      _md = MessageDigest.getInstance("SHA-1");
      _allInstances = allNodes;
      _liveInstances = liveNodes;
      _instanceConfigMap = instanceConfigMap;
      _clusterConfig = clusterConfig;
      _types = new LinkedHashSet<String>();

      String topologyDef = _clusterConfig.getTopology();
      if (topologyDef != null) {
        // Customized cluster topology definition is configured.
        String[] types = topologyDef.trim().split("/");
        for (int i = 0; i < types.length; i++) {
          if (types[i].length() != 0) {
            _types.add(types[i]);
          }
        }
        if (_types.size() == 0) {
          logger.error("Invalid cluster topology definition " + topologyDef);
          throw new HelixException("Invalid cluster topology definition " + topologyDef);
        } else {
          String lastType = null;
          for (String type : _types) {
            _defaultDomainPathValues.put(type, "Helix_default_" + type);
            lastType = type;
          }
          _endNodeType = lastType;
          _faultZoneType = clusterConfig.getFaultZoneType();
          if (_faultZoneType == null) {
            _faultZoneType = _endNodeType;
          }
          if (!_types.contains(_faultZoneType)) {
            throw new HelixException(String
                .format("Invalid fault zone type %s, not present in topology definition %s.",
                    _faultZoneType, topologyDef));
          }
          _useDefaultTopologyDef = false;
        }
      } else {
        // Use default cluster topology definition, i,e. /root/zone/instance
        _types.add(Types.ZONE.name());
        _types.add(Types.INSTANCE.name());
        _endNodeType = Types.INSTANCE.name();
        _faultZoneType = Types.ZONE.name();
        _useDefaultTopologyDef = true;
      }
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalArgumentException(ex);
    }
    if (_useDefaultTopologyDef) {
      _root = createClusterTreeWithDefaultTopologyDef();
    } else {
      _root = createClusterTreeWithCustomizedTopology();
    }
  }

  public String getEndNodeType() {
    return _endNodeType;
  }

  public String getFaultZoneType() {
    return _faultZoneType;
  }

  public Node getRootNode() {
    return _root;
  }

  public List<Node> getFaultZones() {
    if (_root != null) {
      return _root.findChildren(getFaultZoneType());
    }
    return Collections.emptyList();
  }

  /**
   * Returns all leaf nodes that belong in the tree. Returns itself if this node is a leaf.
   *
   * @return
   */
  public static List<Node> getAllLeafNodes(Node root) {
    List<Node> nodes = new ArrayList<Node>();
    if (root.isLeaf()) {
      nodes.add(root);
    } else {
      for (Node child : root.getChildren()) {
        nodes.addAll(getAllLeafNodes(child));
      }
    }
    return nodes;
  }

  /**
   * Clone a node tree structure, with node weight updated using specified new weight,
   * and all nodes in @failedNodes as failed.
   *
   * @param root          origin root of the tree
   * @param newNodeWeight map of node name to its new weight. If absent, keep its original weight.
   * @param failedNodes   set of nodes that need to be failed.
   * @return new root node.
   */
  public static Node clone(Node root, Map<String, Integer> newNodeWeight, Set<String> failedNodes) {
    Node newRoot = cloneTree(root, newNodeWeight, failedNodes);
    computeWeight(newRoot);
    return newRoot;
  }

  private static Node cloneTree(Node root, Map<String, Integer> newNodeWeight, Set<String> failedNodes) {
    Node newRoot = new Node(root);
    if (newNodeWeight.containsKey(root.getName())) {
      newRoot.setWeight(newNodeWeight.get(root.getName()));
    }
    if (failedNodes.contains(root.getName())) {
      newRoot.setFailed(true);
      newRoot.setWeight(0);
    }

    List<Node> children = root.getChildren();
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        Node newChild = cloneTree(children.get(i), newNodeWeight, failedNodes);
        newChild.setParent(root);
        newRoot.addChild(newChild);
      }
    }

    return newRoot;
  }

  /**
   * Creates a tree representing the cluster structure using default cluster topology definition
   * (i,e no topology definition given and no domain id set).
   */
  private Node createClusterTreeWithDefaultTopologyDef() {
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(computeId("root"));
    root.setType(Types.ROOT.name());

    for (String ins : _allInstances) {
      InstanceConfig config = _instanceConfigMap.get(ins);
      if (config == null) {
        throw new HelixException(String.format("Config for instance %s is not found!", ins));
      }
      String zone = config.getZoneId();
      if (zone == null) {
        //TODO: we should allow non-rack cluster for back-compatible. This should be solved once
        // we have the hierarchy style of domain id for instance.
        throw new HelixException(String
            .format("ZONE_ID for instance %s is not set, failed the topology-aware placement!",
                ins));
      }
      Map<String, String> pathValueMap = new HashMap<String, String>();
      pathValueMap.put(Types.ZONE.name(), zone);
      pathValueMap.put(Types.INSTANCE.name(), ins);

      int weight = config.getWeight();
      if (weight < 0 || weight == InstanceConfig.WEIGHT_NOT_SET) {
        weight = DEFAULT_NODE_WEIGHT;
      }
      root = addEndNode(root, ins, pathValueMap, weight, _liveInstances);
    }

    return root;
  }

  /**
   * Creates a tree representing the cluster structure using default cluster topology definition
   * (i,e no topology definition given and no domain id set).
   */
  private Node createClusterTreeWithCustomizedTopology() {
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(computeId("root"));
    root.setType(Types.ROOT.name());

    for (String ins : _allInstances) {
      InstanceConfig insConfig = _instanceConfigMap.get(ins);
      if (insConfig == null) {
        throw new HelixException(String.format("Config for instance %s is not found!", ins));
      }
      String domain = insConfig.getDomain();
      if (domain == null) {
        throw new HelixException(String
            .format("Domain for instance %s is not set, failed the topology-aware placement!",
                ins));
      }

      String[] pathPairs = domain.trim().split(",");
      Map<String, String> pathValueMap = new HashMap<String, String>();
      for (String pair : pathPairs) {
        String[] values = pair.trim().split("=");
        if (values.length != 2 || values[0].isEmpty() || values[1].isEmpty()) {
          throw new HelixException(String.format(
              "Domain-Value pair %s for instance %s is not valid, failed the topology-aware placement!",
              pair, ins));
        }
        String type = values[0];
        String value = values[1];

        if (!_types.contains(type)) {
          logger.warn(String
              .format("Path %s defined in domain of instance %s not recognized, ignored!", pair,
                  ins));
          continue;
        }
        pathValueMap.put(type, value);
      }

      int weight = insConfig.getWeight();
      if (weight < 0 || weight == InstanceConfig.WEIGHT_NOT_SET) {
        weight = DEFAULT_NODE_WEIGHT;
      }

      root = addEndNode(root, ins, pathValueMap, weight, _liveInstances);
    }

    return root;
  }


  /**
   * Add an end node to the tree, create all the paths to the leaf node if not present.
   */
  private Node addEndNode(Node root, String instanceName, Map<String, String> pathNameMap,
      int instanceWeight, List<String> liveInstances) {
    Node current = root;
    List<Node> pathNodes = new ArrayList<Node>();
    for (String path : _types) {
      String pathValue = pathNameMap.get(path);
      if (pathValue == null || pathValue.isEmpty()) {
        pathValue = _defaultDomainPathValues.get(path);
      }
      pathNodes.add(current);
      if (!current.hasChild(pathValue)) {
        Node n = new Node();
        n.setName(pathValue);
        n.setId(computeId(pathValue));
        n.setType(path);
        n.setParent(current);

        // if it is leaf node.
        if (path.equals(_endNodeType)) {
          if (liveInstances.contains(instanceName)) {
            // node is alive
            n.setWeight(instanceWeight);
            // add instance weight to all of its parent nodes.
            for (Node node :  pathNodes) {
              node.addWeight(instanceWeight);
            }
          } else {
            n.setFailed(true);
            n.setWeight(0);
          }
        }
        current.addChild(n);
      }
      current = current.getChild(pathValue);
    }
    return root;
  }

  private long computeId(String name) {
    byte[] h = _md.digest(name.getBytes());
    return bstrTo32bit(h);
  }

  private static void computeWeight(Node node) {
    int weight = 0;
    for (Node child : node.getChildren()) {
      if (!child.isFailed()) {
        weight += child.getWeight();
      }
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
