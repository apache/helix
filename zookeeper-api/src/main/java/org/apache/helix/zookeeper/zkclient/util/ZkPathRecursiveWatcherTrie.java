package org.apache.helix.zookeeper.zkclient.util;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.zookeeper.zkclient.RecursivePersistListener;


/**
 * ZkPathRecursiveWatcherTrie will be used as a registry of persistent recursive watchers.
 * When persist recursive watcher is registered on path /X, ZK will send out data change for any
 * data/child changing under the tree structure of /X. The event only include the path of changed
 * ZNode.
 * In ZkClient, when ever we get a dataChange event for path /X/Y/Z, we need to track back the path
 * and notify all registered recursive persist listener and notify them about the node change.
 * ref: https://zookeeper.apache.org/doc/r3.7.1/zookeeperProgrammers.html#sc_WatchPersistentRecursive
 */
public class ZkPathRecursiveWatcherTrie {

  /** Root node of PathTrie */
  private final TrieNode _rootNode;

  static class TrieNode {

    final String _value;   // Segmented ZNode path at current level
    // A map of segmented ZNode path of next level to TrieNode, We keep the same initial
    // children size as Zk server
    final Map<String, TrieNode> _children = new HashMap<>(4);
    // A list of recursive persist watcher on the current path
    Set<RecursivePersistListener> _recursiveListeners = new HashSet<>(4);

    /**
     * Create a trie node with parent as parameter.
     *
     * @param value the value stored in this node
     */
    private TrieNode(String value) {
      _value = value;
    }

    /**
     * The value stored in this node.
     *
     * @return the value stored in this node
     */
    public String getValue() {
      return this._value;
    }

    /**
     * Add a child to the existing node.
     *
     * @param childName the string name of the child
     * @param node the node that is the child
     */
    void addChild(String childName, TrieNode node) {
      this._children.putIfAbsent(childName, node);
    }

    /**
     * Return the child of a node mapping to the input child name.
     *
     * @param childName the name of the child
     * @return the child of a node
     */
    @VisibleForTesting
    TrieNode getChild(String childName) {
      return this._children.get(childName);
    }

    /**
     * Get the list of children of this trienode.
     *
     * @return A collection containing the node's children
     */
    @VisibleForTesting
    Map<String, TrieNode> getChildren() {
      return _children;
    }

    /**
     * Get the set of RecursivePersistWatcherListener
     * Returns an empty set if no listener is registered on the path
     * @return
     */
    @VisibleForTesting
    Set<RecursivePersistListener> getRecursiveListeners() {
      return _recursiveListeners;
    }

    @Override
    public String toString() {
      return "TrieNode [name=" + _value + ", children=" + _children.keySet() + "]";
    }
  }

  /**
   * Construct a new PathTrie with a root node.
   */
  public ZkPathRecursiveWatcherTrie() {
    this._rootNode = new TrieNode( "/");
  }

  /**
   * Add a path to the path trie. All paths are relative to the root node.
   *
   * @param path the path to add RecursivePersistListener
   * @param listener the RecursivePersistListener to be added
   */
  public void addRecursiveListener(final String path, RecursivePersistListener listener) {
    Objects.requireNonNull(path, "Path cannot be null");

    if (path.isEmpty()) {
      throw new IllegalArgumentException("Empty path: " + path);
    }
    final List<String>  pathComponents = split(path);

    synchronized(this) {
      TrieNode parent = _rootNode;
      for (final String part : pathComponents) {
        parent = parent.getChildren().computeIfAbsent(part, (p)-> new TrieNode(part) );
      }
      parent._recursiveListeners.add(listener);
    }
  }

  /**
   * Clear all nodes in the trie.
   */
  public synchronized void clear() {
    _rootNode.getChildren().clear();
  }

  private static List<String> split(final String path) {
    return Stream.of(path.split("/")).filter(t -> !t.trim().isEmpty()).collect(Collectors.toList());
  }

  // only for test
  @VisibleForTesting
  TrieNode getRootNode() {
    return _rootNode;
  }
}
