package org.apache.helix.zookeeper.zkclient;

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

import org.apache.zookeeper.Watcher;

/**
 * An {@link RecursivePersistListener} can be registered at a {@link ZkClient} for listening on all
 * zk children changes in a tree structure for a given path.
 *
 * The listener is a persist listener. No need to resubscribe.
 *
 */
public interface RecursivePersistListener {
  /**
   * invoked when there is a node added, removed or node data change in the tree structure of
   * that RecursivePersistListener subscribed path
   * @param dataPath The path of ZNode that change happened
   * @param eventType Event type, including NodeCreated, NodeDataChanged and NodeDeleted
   * @throws Exception
   */
  public void handleZNodeChange(String dataPath, Watcher.Event.EventType eventType)
      throws Exception;
}
