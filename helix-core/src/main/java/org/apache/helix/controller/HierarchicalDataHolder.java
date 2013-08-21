package org.apache.helix.controller;

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

import java.io.FileFilter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

/**
 * Generic class that will read the data given the root path.
 */
public class HierarchicalDataHolder<T> {
  private static final Logger logger = Logger.getLogger(HierarchicalDataHolder.class.getName());
  AtomicReference<Node<T>> root;

  /**
   * currentVersion, gets updated when data is read from original source
   */
  AtomicLong currentVersion;
  private final ZkClient _zkClient;
  private final String _rootPath;
  private final FileFilter _filter;

  public HierarchicalDataHolder(ZkClient client, String rootPath, FileFilter filter) {
    this._zkClient = client;
    this._rootPath = rootPath;
    this._filter = filter;
    // Node<T> initialValue = new Node<T>();
    root = new AtomicReference<HierarchicalDataHolder.Node<T>>();
    currentVersion = new AtomicLong(1);
    refreshData();
  }

  public long getVersion() {
    return currentVersion.get();
  }

  public boolean refreshData() {
    Node<T> newRoot = new Node<T>();
    boolean dataChanged = refreshRecursively(root.get(), newRoot, _rootPath);
    if (dataChanged) {
      currentVersion.getAndIncrement();
      root.set(newRoot);
      return true;
    } else {
      return false;
    }
  }

  // private void refreshRecursively(Node<T> oldRoot, Stat oldStat, Node<T>
  // newRoot,Stat newStat, String path)
  private boolean refreshRecursively(Node<T> oldRoot, Node<T> newRoot, String path) {
    boolean dataChanged = false;
    Stat newStat = _zkClient.getStat(path);
    Stat oldStat = (oldRoot != null) ? oldRoot.stat : null;
    newRoot.name = path;
    if (newStat != null) {
      if (oldStat == null) {
        newRoot.stat = newStat;
        newRoot.data = _zkClient.<T> readData(path, true);
        dataChanged = true;
      } else if (newStat.equals(oldStat)) {
        newRoot.stat = oldStat;
        newRoot.data = oldRoot.data;
      } else {
        dataChanged = true;
        newRoot.stat = newStat;
        newRoot.data = _zkClient.<T> readData(path, true);
      }
      if (newStat.getNumChildren() > 0) {
        List<String> children = _zkClient.getChildren(path);
        for (String child : children) {
          String newPath = path + "/" + child;
          Node<T> oldChild =
              (oldRoot != null && oldRoot.children != null) ? oldRoot.children.get(child) : null;
          if (newRoot.children == null) {
            newRoot.children = new ConcurrentHashMap<String, HierarchicalDataHolder.Node<T>>();
          }
          if (!newRoot.children.contains(child)) {
            newRoot.children.put(child, new Node<T>());
          }
          Node<T> newChild = newRoot.children.get(child);
          boolean childChanged = refreshRecursively(oldChild, newChild, newPath);
          dataChanged = dataChanged || childChanged;
        }
      }
    } else {
      logger.info(path + " does not exist");
    }
    return dataChanged;
  }

  static class Node<T> {
    String name;
    Stat stat;
    T data;
    ConcurrentHashMap<String, Node<T>> children;

  }

  public void print() {
    logger.info("START " + _rootPath);
    LinkedList<Node<T>> stack = new LinkedList<HierarchicalDataHolder.Node<T>>();
    stack.push(root.get());
    while (!stack.isEmpty()) {
      Node<T> pop = stack.pop();
      if (pop != null) {
        logger.info("name:" + pop.name);
        logger.info("\tdata:" + pop.data);
        if (pop.children != null) {
          for (Node<T> child : pop.children.values()) {
            stack.push(child);
          }
        }
      }
    }
    logger.info("END " + _rootPath);
  }
}
