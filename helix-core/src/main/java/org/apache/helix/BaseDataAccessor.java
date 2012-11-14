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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public interface BaseDataAccessor<T>
{
  /**
   * This will always attempt to create the znode, if it exists it will return false. Will
   * create parents if they do not exist. For performance reasons, it may try to create
   * child first and only if it fails it will try to create parent
   * 
   * @param path
   * @param record
   * @return
   */
  boolean create(String path, T record, int options);

  /**
   * This will always attempt to set the data on existing node. If the znode does not
   * exist it will create it.
   * 
   * @param path
   * @param record
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  boolean set(String path, T record, int options);

  /**
   * This will attempt to merge with existing data by calling znrecord.merge and if it
   * does not exist it will create it znode
   * 
   * @param path
   * @param record
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  boolean update(String path, DataUpdater<T> updater, int options);

  /**
   * This will remove znode and all it's child nodes if any
   * 
   * @param path
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  boolean remove(String path, int options);

  /**
   * Use it when creating children under a parent node. This will use async api for better
   * performance. If the child already exists it will return false.
   * 
   * @param parentPath
   * @param record
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  boolean[] createChildren(List<String> paths, List<T> records, int options);

  /**
   * can set multiple children under a parent node. This will use async api for better
   * performance. If this child does not exist it will create it.
   * 
   * @param parentPath
   * @param record
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   */
  boolean[] setChildren(List<String> paths, List<T> records, int options);

  /**
   * Can update multiple nodes using async api for better performance. If a child does not
   * exist it will create it.
   * 
   * @param parentPath
   * @param record
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  boolean[] updateChildren(List<String> paths, List<DataUpdater<T>> updaters, int options);

  /**
   * remove multiple paths using async api. will remove any child nodes if any
   * 
   * @param paths
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  boolean[] remove(List<String> paths, int options);

  /**
   * Get the {@link T} corresponding to the path
   * 
   * @param path
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  T get(String path, Stat stat, int options);

  /**
   * Get List of {@link T} corresponding to the paths using async api
   * 
   * @param paths
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  List<T> get(List<String> paths, List<Stat> stats, int options);

  /**
   * Get the children under a parent path using async api
   * 
   * @param path
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  List<T> getChildren(String parentPath, List<Stat> stats, int options);

  /**
   * Returns the child names given a parent path
   * 
   * @param type
   * @param keys
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  List<String> getChildNames(String parentPath, int options);

  /**
   * checks if the path exists in zk
   * 
   * @param path
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  boolean exists(String path, int options);

  /**
   * checks if the all the paths exists
   * 
   * @param paths
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  boolean[] exists(List<String> paths, int options);

  /**
   * Get the stats of all the paths
   * 
   * @param paths
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  Stat[] getStats(List<String> paths, int options);

  /**
   * Get the stats of all the paths
   * 
   * @param paths
   * @param options Set the type of ZNode see the valid values in {@link AccessOption}
   * @return
   */
  Stat getStat(String path, int options);

  /**
   * Subscribe data listener to path
   * 
   * @param path
   * @param listener
   * @return
   */
  void subscribeDataChanges(String path, IZkDataListener listener);

  /**
   * Unsubscribe data listener to path
   * 
   * @param path
   * @param listener
   */
  void unsubscribeDataChanges(String path, IZkDataListener listener);

  /**
   * Subscribe child listener to path
   * 
   * @param path
   * @param listener
   * @return
   */
  List<String> subscribeChildChanges(String path, IZkChildListener listener);

  /**
   * Unsubscribe child listener to path
   * 
   * @param path
   * @param listener
   */
  void unsubscribeChildChanges(String path, IZkChildListener listener);

  /**
   * reset the cache if any, when session expiry happens
   */
  void reset();
}
