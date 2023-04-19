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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.zookeeper.zkclient.RecursivePersistListener;


/**
 * ZkPathRecursiveWatcherTrie will be used for a registry for ZK persist recursive watcher.
 * When persist recursive watcher is registered on path /X, ZK will send out data change for any
 * data/child changing under the tree structure of /X. The event only include the path of changed
 * ZNode.
 * In ZkClient, when ever we get a dataChange event for path /X/Y/Z, we need to track back the path
 * and notify all registered recursive persist listener and notify them about the node change.
 * ref: https://zookeeper.apache.org/doc/r3.7.1/zookeeperProgrammers.html#sc_WatchPersistentRecursive
 */
public class ZkPathRecursiveWatcherTrie {

  /** Root node of PathTrie */
  private final TrieNode rootNode;

  static class TrieNode {

    final String value;   // Segmented ZNode path at current level
    final Map<String, TrieNode> children; // A map of segmented ZNode path of next level to TrieNode
    Set<RecursivePersistListener> recursiveListeners;
    // A list of recursive persist watcher on the current path

    /**
     * Create a trie node with parent as parameter.
     *
     * @param value the value stored in this node
     */
    private TrieNode(String value) {
      this.value = value;
      this.children =
          new HashMap<>(4);  // We keep the same number of init children side as Zk serer
      this.recursiveListeners = new HashSet<>(4);
    }


    /**
     * The value stored in this node.
     *
     * @return the value stored in this node
     */
    public String getValue() {
      return this.value;
    }

    /**
     * Add a child to the existing node.
     *
     * @param childName the string name of the child
     * @param node the node that is the child
     */
    void addChild(String childName, TrieNode node) {
      this.children.putIfAbsent(childName, node);
    }

    /**
     * Return the child of a node mapping to the input child name.
     *
     * @param childName the name of the child
     * @return the child of a node
     */
    @VisibleForTesting
    TrieNode getChild(String childName) {
      return this.children.get(childName);
    }

    /**
     * Get the list of children of this trienode.
     *
     * @return A collection containing the node's children
     */
    @VisibleForTesting
    Collection<String> getChildren() {
      return children.keySet();
    }

    /**
     * Get the set of RecursivePersistWatcherListener
     * Returns an empty set if no listener is registered on the path
     * @return
     */
    @VisibleForTesting
    Set<RecursivePersistListener> getRecursiveListeners() {
      return recursiveListeners;
    }

    @Override
    public String toString() {
      return "TrieNode [name=" + value + ", children=" + children.keySet() + "]";
    }
  }

  /**
   * Construct a new PathTrie with a root node.
   */
  public ZkPathRecursiveWatcherTrie() {
    this.rootNode = new TrieNode( "/");
  }

  /**
   * Add a path to the path trie. All paths are relative to the root node.
   *
   * @param path the path to add RecursivePersistListener
   * @param listener the RecursivePersistListener to be added
   */
  public void addRecursiveListener(final String path, RecursivePersistListener listener) {
    Objects.requireNonNull(path, "Path cannot be null");

    if (path.length() == 0) {
      throw new IllegalArgumentException("Invalid path: " + path);
    }
    final String[] pathComponents = split(path);

    synchronized(this) {
      TrieNode parent = rootNode;
      for (final String part : pathComponents) {
        TrieNode child = parent.getChild(part);
        if (child == null) {
          child = new TrieNode(part);
          parent.addChild(part, child);
        }
        parent = child;
      }
      parent.recursiveListeners.add(listener);
    }
  }

  /**
   * Clear all nodes in the trie.
   */
  public void clear() {
    synchronized(this) {
      rootNode.getChildren().clear();
    }
  }

  private static String[] split(final String path) {
    return Stream.of(path.split("/")).filter(t -> !t.trim().isEmpty()).toArray(String[]::new);
  }

  // only for test
  @VisibleForTesting
  TrieNode getRootNode() {
    return rootNode;
  }
}
