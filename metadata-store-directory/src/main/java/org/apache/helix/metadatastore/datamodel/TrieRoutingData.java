package org.apache.helix.metadatastore.datamodel;

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

import org.apache.helix.metadatastore.datamodel.MetadataStoreRoutingData;
import org.apache.helix.metadatastore.exception.InvalidRoutingDataException;


/**
 * This is a class that uses a data structure similar to trie to represent metadata store routing
 * data. It is not exactly a trie because it in essence stores a mapping (from sharding keys to
 * realm addresses) instead of pure text information; also, only the terminal nodes store meaningful
 * information (realm addresses).
 */
public class TrieRoutingData implements MetadataStoreRoutingData {
  private static final String DELIMITER = "/";

  private final TrieNode _rootNode;

  public TrieRoutingData(Map<String, List<String>> routingData) throws InvalidRoutingDataException {
    if (routingData == null || routingData.isEmpty()) {
      throw new InvalidRoutingDataException("routingData cannot be null or empty");
    }

    if (isRootShardingKey(routingData)) {
      Map.Entry<String, List<String>> entry = routingData.entrySet().iterator().next();
      _rootNode = new TrieNode(Collections.emptyMap(), "/", true, entry.getKey());
    } else {
      _rootNode = new TrieNode(new HashMap<>(), "/", false, "");
      constructTrie(routingData);
    }
  }

  public Map<String, String> getAllMappingUnderPath(String path) throws IllegalArgumentException {
    if (path.isEmpty() || !path.substring(0, 1).equals(DELIMITER)) {
      throw new IllegalArgumentException(
          "Provided path is empty or does not have a leading \"" + DELIMITER + "\" character: "
              + path);
    }

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
    if (path.isEmpty() || !path.substring(0, 1).equals(DELIMITER)) {
      throw new IllegalArgumentException(
          "Provided path is empty or does not have a leading \"" + DELIMITER + "\" character: "
              + path);
    }

    TrieNode leafNode = findTrieNode(path, true);
    return leafNode.getRealmAddress();
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
    if (path.equals(DELIMITER)) {
      if (findLeafAlongPath && !_rootNode.isShardingKey()) {
        throw new NoSuchElementException("No leaf node found along the path. Path: " + path);
      }
      return _rootNode;
    }

    TrieNode curNode = _rootNode;
    if (findLeafAlongPath && curNode.isShardingKey()) {
      return curNode;
    }
    Map<String, TrieNode> curChildren = curNode.getChildren();
    for (String pathSection : path.substring(1).split(DELIMITER, 0)) {
      curNode = curChildren.get(pathSection);
      if (curNode == null) {
        throw new NoSuchElementException(
            "The provided path is missing from the trie. Path: " + path);
      }
      if (findLeafAlongPath && curNode.isShardingKey()) {
        return curNode;
      }
      curChildren = curNode.getChildren();
    }
    if (findLeafAlongPath) {
      throw new NoSuchElementException("No leaf node found along the path. Path: " + path);
    }
    return curNode;
  }

  /**
   * Checks for the edge case when the only sharding key in provided routing data is the delimiter
   * or an empty string. When this is the case, the trie is valid and contains only one node, which
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

  /**
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
        if (shardingKey.isEmpty() || !shardingKey.substring(0, 1).equals(DELIMITER)) {
          throw new InvalidRoutingDataException(
              "Sharding key does not have a leading \"" + DELIMITER + "\" character: "
                  + shardingKey);
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
    /**
     * This field is a mapping between trie key and children nodes. For example, node "a" has
     * children "ab" and "ac", therefore the keys are "b" and "c" respectively.
     */
    private Map<String, TrieNode> _children;
    /**
     * This field states whether the path represented by the node is a sharding key
     */
    private final boolean _isShardingKey;
    /**
     * This field contains the complete path/prefix leading to the current node. For example, the
     * name of root node is "/", then the name of its child node
     * is "/a", and the name of the child's child node is "/a/b".
     */
    private final String _path;
    /**
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
