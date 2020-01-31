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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * This is a class that uses a data structure similar to trie to represent metadata store routing
 * data. It is not exactly a trie because it in essence stores a mapping (from sharding keys to
 * realm addresses) instead of pure text information; also, only the terminal nodes store meaningful
 * information (realm addresses).
 */
public class TrieRoutingData implements MetadataStoreRoutingData {
  private static final String DELIMITER = "/";

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
    } catch (NoSuchElementException e) {
      return Collections.emptyMap();
    }

    Map<String, String> resultMap = new HashMap<>();
    Deque<TrieNode> nodeStack = new ArrayDeque<>();
    nodeStack.push(curNode);
    while (!nodeStack.isEmpty()) {
      curNode = nodeStack.pop();
      if (curNode._isLeaf) {
        resultMap.put(curNode._name, curNode._realmAddress);
      } else {
        for (TrieNode child : curNode._children.values()) {
          nodeStack.push(child);
        }
      }
    }
    return resultMap;
  }

  public String getMetadataStoreRealm(String path) throws NoSuchElementException {
    TrieNode leafNode = findTrieNode(path, true);
    return leafNode._realmAddress;
  }

  /**
   * If findLeafAlongPath is false, then starting from the root node, find the trie node that the
   * given path is pointing to and return it; raise NoSuchElementException if the path does
   * not point to any node. If findLeafAlongPath is true, then starting from the root node, find the
   * leaf node along the provided path; raise NoSuchElementException if the path does not
   * point to any node or if there is no leaf node along the path.
   * @param path - the path where the search is conducted
   * @param findLeafAlongPath - whether the search is for a leaf node on the path
   * @return the node pointed by the path or a leaf node along the path
   * @throws NoSuchElementException - when the path points to nothing or when no leaf node is
   *           found
   */
  private TrieNode findTrieNode(String path, boolean findLeafAlongPath)
      throws NoSuchElementException {
    if (path.equals(DELIMITER) || path.equals("")) {
      if (findLeafAlongPath && !_rootNode._isLeaf) {
        throw new NoSuchElementException("No leaf node found along the path. Path: " + path);
      }
      return _rootNode;
    }

    String[] splitPath;
    if (path.substring(0, 1).equals(DELIMITER)) {
      splitPath = path.substring(1).split(DELIMITER, 0);
    } else {
      splitPath = path.split(DELIMITER, 0);
    }

    TrieNode curNode = _rootNode;
    if (findLeafAlongPath && curNode._isLeaf) {
      return curNode;
    }
    Map<String, TrieNode> curChildren = curNode._children;
    for (String pathSection : splitPath) {
      curNode = curChildren.get(pathSection);
      if (curNode == null) {
        throw new NoSuchElementException(
            "The provided path is missing from the trie. Path: " + path);
      }
      if (findLeafAlongPath && curNode._isLeaf) {
        return curNode;
      }
      curChildren = curNode._children;
    }
    if (findLeafAlongPath) {
      throw new NoSuchElementException("No leaf node found along the path. Path: " + path);
    }
    return curNode;
  }

  // TODO: THE CLASS WILL BE CHANGED TO PRIVATE ONCE THE CONSTRUCTOR IS CREATED.
  static class TrieNode {
    /**
     * This field is a mapping between trie key and children nodes. For example, node "a" has
     * children "ab" and "ac", therefore the keys are "b" and "c" respectively.
     */
    Map<String, TrieNode> _children;
    /**
     * This field means if the node is a terminal node in the tree sense, not the trie sense. Any
     * node that has children cannot possibly be a leaf node because only the node without children
     * can store information. If a node is leaf, then it shouldn't have any children.
     */
    final boolean _isLeaf;
    /**
     * This field aligns the traditional trie design: it entails the complete path/prefix leading to
     * the current node. For example, the name of root node is "/", then the name of its child node
     * is "/a", and the name of the child's child node is "/a/b".
     */
    final String _name;
    /**
     * This field represents the data contained in a node(which represents a path), and is only
     * available to the terminal nodes.
     */
    final String _realmAddress;

    TrieNode(Map<String, TrieNode> children, String name, boolean isLeaf, String realmAddress) {
      _children = children;
      _isLeaf = isLeaf;
      _name = name;
      _realmAddress = realmAddress;
    }
  }
}
