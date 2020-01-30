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
import java.util.Stack;

public class TrieRoutingData implements MetadataStoreRoutingData {
  private final String DELIMITER = "/";

  private final TrieNode _rootNode;

  // TODO: THIS IS A TEMPORARY PLACEHOLDER. A proper constructor will be created, which will not
  // take in a TrieNode; it instead initializes the rootNode and creates a trie based on
  // some input data. The constructor is blocked by the implementation of RoutingDataAccessor, and
  // will therefore be implemented later.
  public TrieRoutingData(TrieNode rootNode) {
    _rootNode = rootNode;
  }

  public Map<String, String> getAllMappingUnderPath(String path) {
    TrieNode curNode;
    try {
      curNode = findTrieNode(path, false);
    } catch (IllegalArgumentException e) {
      return Collections.emptyMap();
    }

    Map<String, String> resultMap = new HashMap<>();
    Stack<TrieNode> nodeStack = new Stack<>();
    nodeStack.push(curNode);
    while (!nodeStack.isEmpty()) {
      if (nodeStack.peek()._isLeaf) {
        resultMap.put(nodeStack.peek()._name, nodeStack.peek()._realmAddress);
        nodeStack.pop();
      } else {
        curNode = nodeStack.pop();
        for (TrieNode child : curNode._children.values()) {
          nodeStack.push(child);
        }
      }
    }
    return resultMap;
  }

  public String getMetadataStoreRealm(String path) throws IllegalArgumentException {
    TrieNode leafNode = findTrieNode(path, true);
    return leafNode._realmAddress;
  }

  /**
   * If findLeafAlongPath is false, then starting from the root node, find the trie node that the
   * given path is pointing to and return it; raise IllegalArgumentException if the path does
   * not point to any node. If findLeafAlongPath is true, then starting from the root node, find the
   * leaf node along the provided path; raise IllegalArgumentException if the path does not
   * point to any node or if there is no leaf node along the path.
   * @param path - the path where the search is conducted
   * @param findLeafAlongPath - whether the search is for a leaf node on the path
   * @return the node pointed by the path or a leaf node along the path
   * @throws IllegalArgumentException - when the path points to nothing or when no leaf node is
   *           found
   */
  private TrieNode findTrieNode(String path, boolean findLeafAlongPath)
      throws IllegalArgumentException {
    if (path.equals(DELIMITER) || path.equals("")) {
      if (findLeafAlongPath && !_rootNode._isLeaf) {
        throw new IllegalArgumentException("no leaf node found along the path");
      }
      return _rootNode;
    }

    if (path.substring(0, 1).equals(DELIMITER)) {
      path = path.substring(1);
    }
    String[] splitPath = path.split(DELIMITER, 0);

    TrieNode curNode = _rootNode;
    if (findLeafAlongPath && curNode._isLeaf) {
      return curNode;
    }
    Map<String, TrieNode> curChildren = curNode._children;
    for (String pathSection : splitPath) {
      curNode = curChildren.get(pathSection);
      if (curNode == null) {
        throw new IllegalArgumentException("the provided path is missing from the trie");
      }
      if (findLeafAlongPath && curNode._isLeaf) {
        return curNode;
      }
      curChildren = curNode._children;
    }
    if (findLeafAlongPath) {
      throw new IllegalArgumentException("no leaf node found along the path");
    }
    return curNode;
  }

  static class TrieNode {
    final Map<String, TrieNode> _children;
    final boolean _isLeaf;
    final String _name;
    final String _realmAddress;

    TrieNode(Map<String, TrieNode> children, String name, boolean isLeaf, String realmAddress) {
      _children = children;
      _isLeaf = isLeaf;
      _name = name;
      _realmAddress = realmAddress;
    }
  }
}
