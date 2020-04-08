package org.apache.helix.msdcommon.datamodel;

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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.msdcommon.util.ZkValidationUtil;


/**
 * This is a class that uses a data structure similar to trie to represent metadata store routing
 * data. It is not exactly a trie because it in essence stores a mapping (from sharding keys to
 * realm addresses) instead of pure text information; also, only the terminal nodes store meaningful
 * information (realm addresses).
 */
public class TrieRoutingData implements MetadataStoreRoutingData {
  private static final String DELIMITER = "/";

  private final TrieNode _rootNode;

  public TrieRoutingData(Map<String, List<String>> routingData)
      throws InvalidRoutingDataException {
    if (routingData == null || routingData.isEmpty()) {
      throw new InvalidRoutingDataException("routingData cannot be null or empty");
    }

    if (!containsShardingKey(routingData)) {
      throw new InvalidRoutingDataException("routingData needs at least 1 sharding key");
    }

    if (isRootShardingKey(routingData)) {
      Map.Entry<String, List<String>> entry = routingData.entrySet().iterator().next();
      _rootNode = new TrieNode(Collections.emptyMap(), "/", true, entry.getKey());
    } else {
      _rootNode = new TrieNode(new HashMap<>(), "/", false, "");
      constructTrie(routingData);
    }
  }

  public Map<String, String> getAllMappingUnderPath(String path)
      throws IllegalArgumentException {
    if (!ZkValidationUtil.isPathValid(path)) {
      throw new IllegalArgumentException("Provided path is not a valid Zookeeper path: " + path);
    }

    TrieNode curNode = getLongestPrefixNodeAlongPath(path);
    if (!curNode.getPath().equals(path)) {
      return Collections.emptyMap();
    }

    Map<String, String> resultMap = new HashMap<>();
    Deque<TrieNode> nodeStack = new ArrayDeque<>();
    nodeStack.push(curNode);
    while (!nodeStack.isEmpty()) {
      curNode = nodeStack.pop();
      if (curNode.isShardingKey()) {
        resultMap.put(curNode.getPath(), curNode.getRealmAddress());
      } else {
        for (TrieNode child : curNode.getChildren().values()) {
          nodeStack.push(child);
        }
      }
    }
    return resultMap;
  }

  public String getMetadataStoreRealm(String path)
      throws IllegalArgumentException, NoSuchElementException {
    if (!ZkValidationUtil.isPathValid(path)) {
      throw new IllegalArgumentException("Provided path is not a valid Zookeeper path: " + path);
    }

    TrieNode node = getLongestPrefixNodeAlongPath(path);
    if (!node.isShardingKey()) {
      throw new NoSuchElementException(
          "No sharding key found within the provided path. Path: " + path);
    }
    return node.getRealmAddress();
  }

  public String getShardingKeyInPath(String path)
      throws IllegalArgumentException, NoSuchElementException {
    if (!ZkValidationUtil.isPathValid(path)) {
      throw new IllegalArgumentException("Provided path is not a valid Zookeeper path: " + path);
    }

    TrieNode node = getLongestPrefixNodeAlongPath(path);
    if (!node.isShardingKey()) {
      throw new NoSuchElementException(
          "No sharding key found within the provided path. Path: " + path);
    }
    return node.getPath();
  }

  public boolean isShardingKeyInsertionValid(String shardingKey) {
    if (!ZkValidationUtil.isPathValid(shardingKey)) {
      throw new IllegalArgumentException(
          "Provided shardingKey is not a valid Zookeeper path: " + shardingKey);
    }

    TrieNode node = getLongestPrefixNodeAlongPath(shardingKey);
    return !node.isShardingKey() && !node.getPath().equals(shardingKey);
  }

  public boolean containsKeyRealmPair(String shardingKey, String realmAddress) {
    if (!ZkValidationUtil.isPathValid(shardingKey)) {
      throw new IllegalArgumentException(
          "Provided shardingKey is not a valid Zookeeper path: " + shardingKey);
    }

    TrieNode node = getLongestPrefixNodeAlongPath(shardingKey);
    return node.getPath().equals(shardingKey) && node.getRealmAddress().equals(realmAddress);
  }

  /*
   * Given a path, find a trie node that represents the longest prefix of the path. For example,
   * given "/a/b/c", the method starts at "/", and attempts to reach "/a", then attempts to reach
   * "/a/b", then ends on "/a/b/c"; if any of the node doesn't exist, the traversal terminates and
   * the last seen existing node is returned.
   * Note:
   * 1. When the returned TrieNode is a sharding key, it is the only sharding key along the
   * provided path (the path points to this sharding key);
   * 2. When the returned TrieNode is not a sharding key but it represents the provided path, the
   * provided path is a prefix(parent) to a sharding key;
   * 3. When the returned TrieNode is not a sharding key and it does not represent the provided
   * path (meaning the traversal ended before the last node of the path is reached), the provided
   * path is not associated with any sharding key and can be added as a sharding key without
   * creating ambiguity cases among sharding keys.
   * @param path - the path where the search is conducted
   * @return a TrieNode that represents the longest prefix of the path
   */
  private TrieNode getLongestPrefixNodeAlongPath(String path) {
    if (path.equals(DELIMITER)) {
      return _rootNode;
    }

    TrieNode curNode = _rootNode;
    TrieNode nextNode;
    for (String pathSection : path.substring(1).split(DELIMITER, 0)) {
      nextNode = curNode.getChildren().get(pathSection);
      if (nextNode == null) {
        return curNode;
      }
      curNode = nextNode;
    }
    return curNode;
  }

  /*
   * Checks if there is any sharding key in the routing data
   * @param routingData - a mapping from "sharding keys" to "realm addresses" to be parsed into a
   *          trie
   * @return whether there is any sharding key
   */
  private boolean containsShardingKey(Map<String, List<String>> routingData) {
    for (Map.Entry<String, List<String>> entry : routingData.entrySet()) {
      if (entry.getValue().size() > 0) {
        return true;
      }
    }
    return false;
  }

  /*
   * Checks for the edge case when the only sharding key in provided routing data is the delimiter.
   * When this is the case, the trie is valid and contains only one node, which
   * is the root node, and the root node is a leaf node with a realm address associated with it.
   * @param routingData - a mapping from "sharding keys" to "realm addresses" to be parsed into a
   *          trie
   * @return whether the edge case is true
   */
  private boolean isRootShardingKey(Map<String, List<String>> routingData) {
    if (routingData.size() == 1) {
      for (List<String> shardingKeys : routingData.values()) {
        return shardingKeys.size() == 1 && shardingKeys.get(0).equals(DELIMITER);
      }
    }

    return false;
  }

  /*
   * Constructs a trie based on the provided routing data. It loops through all sharding keys and
   * constructs the trie in a top down manner.
   * @param routingData- a mapping from "sharding keys" to "realm addresses" to be parsed into a
   *          trie
   * @throws InvalidRoutingDataException - when there is an empty sharding key (edge case that
   *           always renders the routing data invalid); when there is a sharding key which already
   *           contains a sharding key (invalid); when there is a sharding key that is a part of
   *           another sharding key (invalid); when a sharding key doesn't have a leading delimiter
   */
  private void constructTrie(Map<String, List<String>> routingData)
      throws InvalidRoutingDataException {
    for (Map.Entry<String, List<String>> entry : routingData.entrySet()) {
      for (String shardingKey : entry.getValue()) {
        // Missing leading delimiter is invalid
        if (!ZkValidationUtil.isPathValid(shardingKey)) {
          throw new InvalidRoutingDataException(
              "Sharding key is not a valid Zookeeper path: " + shardingKey);
        }

        // Root can only be a sharding key if it's the only sharding key. Since this method is
        // running, the special case has already been checked, therefore it's definitely invalid
        if (shardingKey.equals(DELIMITER)) {
          throw new InvalidRoutingDataException(
              "There exist other sharding keys. Root cannot be a sharding key.");
        }

        // Locate the next delimiter
        int nextDelimiterIndex = shardingKey.indexOf(DELIMITER, 1);
        int prevDelimiterIndex = 0;
        String keySection = shardingKey.substring(prevDelimiterIndex + 1,
            nextDelimiterIndex > 0 ? nextDelimiterIndex : shardingKey.length());
        TrieNode curNode = _rootNode;
        TrieNode nextNode = curNode.getChildren().get(keySection);

        // If the key section is not the last section yet, go in the loop; if the key section is the
        // last section, exit
        while (nextDelimiterIndex > 0) {
          // If the node is already a leaf node, the current sharding key is invalid; if the node
          // doesn't exist, construct a node and continue
          if (nextNode != null && nextNode.isShardingKey()) {
            throw new InvalidRoutingDataException(
                shardingKey + " cannot be a sharding key because " + shardingKey
                    .substring(0, nextDelimiterIndex)
                    + " is its parent key and is also a sharding key.");
          } else if (nextNode == null) {
            nextNode =
                new TrieNode(new HashMap<>(), shardingKey.substring(0, nextDelimiterIndex), false,
                    "");
            curNode.addChild(keySection, nextNode);
          }
          prevDelimiterIndex = nextDelimiterIndex;
          nextDelimiterIndex = shardingKey.indexOf(DELIMITER, prevDelimiterIndex + 1);
          keySection = shardingKey.substring(prevDelimiterIndex + 1,
              nextDelimiterIndex > 0 ? nextDelimiterIndex : shardingKey.length());
          curNode = nextNode;
          nextNode = curNode.getChildren().get(keySection);
        }

        // If the last node already exists, it's a part of another sharding key, making the current
        // sharding key invalid
        if (nextNode != null) {
          throw new InvalidRoutingDataException(shardingKey
              + " cannot be a sharding key because it is a parent key to another sharding key.");
        }
        nextNode = new TrieNode(new HashMap<>(), shardingKey, true, entry.getKey());
        curNode.addChild(keySection, nextNode);
      }
    }
  }

  private static class TrieNode {
    /*
     * This field is a mapping between trie key and children nodes. For example, node "a" has
     * children "ab" and "ac", therefore the keys are "b" and "c" respectively.
     */
    private Map<String, TrieNode> _children;
    /*
     * This field states whether the path represented by the node is a sharding key
     */
    private final boolean _isShardingKey;
    /*
     * This field contains the complete path/prefix leading to the current node. For example, the
     * name of root node is "/", then the name of its child node
     * is "/a", and the name of the child's child node is "/a/b".
     */
    private final String _path;
    /*
     * This field represents the data contained in a node(which represents a path), and is only
     * available to the terminal nodes.
     */
    private final String _realmAddress;

    TrieNode(Map<String, TrieNode> children, String path, boolean isShardingKey,
        String realmAddress) {
      _children = children;
      _isShardingKey = isShardingKey;
      _path = path;
      _realmAddress = realmAddress;
    }

    public Map<String, TrieNode> getChildren() {
      return _children;
    }

    public boolean isShardingKey() {
      return _isShardingKey;
    }

    public String getPath() {
      return _path;
    }

    public String getRealmAddress() {
      return _realmAddress;
    }

    public void addChild(String key, TrieNode node) {
      _children.put(key, node);
    }
  }
}