package org.apache.helix.rest.metadatastore;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TrieRoutingData implements RoutingData {
  private final TrieNode _rootNode;

  // TODO: THIS IS A TEMPORARY PLACEHOLDER. A proper constructor will be created, which will not
  // take in a TrieNode; it instead initializes the rootNode and creates a trie based on
  // some input data. The constructor is blocked by the implementation of RoutingDataAccessor, and
  // will therefore be implemented later.
  public TrieRoutingData(TrieNode rootNode) {
    _rootNode = rootNode;
  }

  static class TrieNode {
    final Map<String, TrieNode> _children;
    final boolean _isLeaf;
    final String _zkRealmAddress;

    TrieNode(Map<String, TrieNode> children, String name, boolean isLeaf, String zkRealmAddress) {
      _children = children;
      _isLeaf = isLeaf;
      _zkRealmAddress = zkRealmAddress;
    }
  }

  public Map<String, String> getAllMappingUnderPath(String zkPath) {
    TrieNode curNode;
    try {
      curNode = findTrieNode(zkPath, false);
    } catch (IllegalArgumentException e) {
      return Collections.emptyMap();
    }

    Map<String, String> resultMap = new HashMap<>();
    if (zkPath.substring(zkPath.length() - 1).equals("/")) {
      zkPath = zkPath.substring(0, zkPath.length() - 1);
    }
    addAllAddressesToMapping(resultMap, curNode, zkPath);
    return resultMap;
  }

  public String getZkRealm(String zkPath) throws IllegalArgumentException {
    TrieNode leafNode = findTrieNode(zkPath, true);
    return leafNode._zkRealmAddress;
  }

  /**
   * If findLeafAlongPath is false, then starting from the root node, find the trie node that the
   * given zkPath is pointing to and return it; raise IllegalArgumentException if the zkPath does
   * not point to any node. If findLeafAlongPath is true, then starting from the root node, find the
   * leaf node along the provided zkPath; raise IllegalArgumentException if the zkPath does not
   * point to any node or if there is no leaf node along the path.
   * @param zkPath - the zkPath where the search is conducted
   * @param findLeafAlongPath - whether the search is for a leaf node on the path
   * @return the node pointed by the zkPath or a leaf node along the path
   * @throws IllegalArgumentException - when the zkPath points to nothing or when no leaf node is found
   */
  private TrieNode findTrieNode(String zkPath, boolean findLeafAlongPath)
      throws IllegalArgumentException {
    if (zkPath.equals("/") || zkPath.equals("")) {
      if (findLeafAlongPath && !_rootNode._isLeaf) {
        throw new IllegalArgumentException("no leaf node found along the zkPath");
      }
      return _rootNode;
    }

    if (zkPath.substring(0, 1).equals("/")) {
      zkPath = zkPath.substring(1);
    }
    String[] splitZkPath = zkPath.split("/", 0);

    TrieNode curNode = _rootNode;
    if (findLeafAlongPath && curNode._isLeaf) {
      return curNode;
    }
    Map<String, TrieNode> curChildren = curNode._children;
    for (String pathSection : splitZkPath) {
      curNode = curChildren.get(pathSection);
      if (curNode == null) {
        throw new IllegalArgumentException("the provided zkPath is missing from the trie");
      }
      if (findLeafAlongPath && curNode._isLeaf) {
        return curNode;
      }
      curChildren = curNode._children;
    }
    if (findLeafAlongPath) {
      throw new IllegalArgumentException("no leaf node found along the zkPath");
    }
    return curNode;
  }

  /**
   * Given a trie node, search for all leaf nodes that descend from the given trie node, and add
   * all complete paths and zkRealmAddresses of these leaf nodes to a provided mapping. This method
   * recursively calls itself.
   * @param mapping - where the results (complete paths of leaf nodes and their zkRealmAddresses)
   *          are stored
   * @param curNode - the current trie node where the search starts
   * @param curPath - the complete path of the current trie node
   */
  private static void addAllAddressesToMapping(Map<String, String> mapping, TrieNode curNode,
      String curPath) {
    if (curNode._isLeaf) {
      mapping.put(curPath, curNode._zkRealmAddress);
      return;
    }

    for (Map.Entry<String, TrieNode> entry : curNode._children.entrySet()) {
      addAllAddressesToMapping(mapping, entry.getValue(), curPath + "/" + entry.getKey());
    }
  }
}
