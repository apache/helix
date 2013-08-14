package org.apache.helix;

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

import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.data.Stat;

/**
 * Generic interface for accessing and manipulating data on a backing store like Zookeeper.
 *
 * @param <T> The type of record to use
 */
public interface BaseDataAccessor<T>
{
  /**
   * This will always attempt to create the znode, if it exists it will return false. Will
   * create parents if they do not exist. For performance reasons, it may try to create
   * child first and only if it fails it will try to create parent
   * 
   * @param path path to the ZNode to create
   * @param record the data to write to the ZNode
   * @return true if creation succeeded, false otherwise (e.g. if the ZNode exists)
   */
  boolean create(String path, T record, int options);

  /**
   * This will always attempt to set the data on existing node. If the znode does not
   * exist it will create it.
   * 
   * @param path path to the ZNode to set
   * @param record the data to write to the ZNode
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return true if data was successfully set, false otherwise
   */
  boolean set(String path, T record, int options);

  /**
   * This will attempt to merge with existing data by calling znrecord.merge and if it
   * does not exist it will create it znode
   * 
   * @param path path to the ZNode to update
   * @param updater an update routine for the data to merge in
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return true if data merge succeeded, false otherwise
   */
  boolean update(String path, DataUpdater<T> updater, int options);

  /**
   * This will remove znode and all its child nodes if any
   * 
   * @param path path to the root ZNode to remove
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return true if the removal succeeded, false otherwise
   */
  boolean remove(String path, int options);

  /**
   * Use it when creating children under a parent node. This will use async api for better
   * performance. If the child already exists it will return false.
   * 
   * @param parentPath paths to the immediate parent ZNodes
   * @param record List of data to write to each of the children
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return For each child: true if creation succeeded, false otherwise (e.g. if the child exists)
   */
  boolean[] createChildren(List<String> paths, List<T> records, int options);

  /**
   * can set multiple children under a parent node. This will use async api for better
   * performance. If this child does not exist it will create it.
   * 
   * @param parentPath paths to the immediate parent ZNodes
   * @param record List of data with which to overwrite the corresponding ZNodes
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return For each child: true if the data was set, false otherwise
   */
  boolean[] setChildren(List<String> paths, List<T> records, int options);

  /**
   * Can update multiple nodes using async api for better performance. If a child does not
   * exist it will create it.
   * 
   * @param parentPath paths to the immediate parent ZNodes
   * @param updaters List of update routines for records to merge in
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return For each child, true if the data was merged in, false otherwise
   */
  boolean[] updateChildren(List<String> paths, List<DataUpdater<T>> updaters, int options);

  /**
   * remove multiple paths using async api. will remove any child nodes if any
   * 
   * @param paths paths to the ZNodes to remove
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return For each ZNode, true if successfully removed, false otherwise
   */
  boolean[] remove(List<String> paths, int options);

  /**
   * Get the {@link T} corresponding to the path
   * 
   * @param path path to the ZNode
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return the record data stored at the ZNode
   */
  T get(String path, Stat stat, int options);

  /**
   * Get List of {@link T} corresponding to the paths using async api
   * 
   * @param paths paths to the ZNodes
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return List of record data stored at each ZNode
   */
  List<T> get(List<String> paths, List<Stat> stats, int options);

  /**
   * Get the children under a parent path using async api
   * 
   * @param path path to the immediate parent ZNode
   * @param stats Zookeeper Stat objects corresponding to each child
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return A list of children of the parent ZNode
   */
  List<T> getChildren(String parentPath, List<Stat> stats, int options);

  /**
   * Returns the child names given a parent path
   * 
   * @param parentPath path to the immediate parent ZNode
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return a list of the names of all of the parent ZNode's children
   */
  List<String> getChildNames(String parentPath, int options);

  /**
   * checks if the path exists in zk
   * 
   * @param path path to the ZNode to test
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return true if the ZNode exists, false otherwise
   */
  boolean exists(String path, int options);

  /**
   * checks if the all the paths exists
   * 
   * @param paths paths to the ZNodes to test
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return for each path, true if a valid ZNode exists, false otherwise
   */
  boolean[] exists(List<String> paths, int options);

  /**
   * Get the stats of all the paths
   * 
   * @param paths paths of the ZNodes to query
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return Zookeeper Stat object for each path
   */
  Stat[] getStats(List<String> paths, int options);

  /**
   * Get the stats of a single path
   * 
   * @param path path of the ZNode to query
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return Zookeeper Stat object corresponding to the ZNode
   */
  Stat getStat(String path, int options);

  /**
   * Subscribe data listener to path
   * 
   * @param path path to the ZNode to listen to
   * @param listener the listener to register for changes
   */
  void subscribeDataChanges(String path, IZkDataListener listener);

  /**
   * Unsubscribe data listener to path
   * 
   * @param path path to the ZNode to stop listening to
   * @param listener the listener currently subscribed to the ZNode
   */
  void unsubscribeDataChanges(String path, IZkDataListener listener);

  /**
   * Subscribe child listener to path
   * 
   * @param path path to the immediate parent ZNode
   * @param listener the listener to register for changes
   * @return
   */
  List<String> subscribeChildChanges(String path, IZkChildListener listener);

  /**
   * Unsubscribe child listener to path
   * 
   * @param path path to the immediate parent ZNode
   * @param listener the listener currently subscribed to the children
   */
  void unsubscribeChildChanges(String path, IZkChildListener listener);

  /**
   * reset the cache if any, when session expiry happens
   */
  void reset();
}
