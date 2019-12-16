package org.apache.helix.manager.zk.zookeeper;

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

import org.apache.zookeeper.Watcher.Event.KeeperState;


public interface IZkStateListener {

  /**
   * Called when the zookeeper connection state has changed.
   *
   * @param state the new zookeeper state.
   * @throws Exception if any error occurs.
   */
  void handleStateChanged(KeeperState state) throws Exception;

  /**
   * Called after the zookeeper session has expired and a new session has been created. The new
   * session id has to be passed in as the parameter.
   * And you would have to re-create any ephemeral nodes here. This is a session aware operation.
   * The ephemeral nodes have to be created within the expected session id, which means passed-in
   * session id has to be checked with current zookeeper's session id. If the passed-in session id
   * does not match current zookeeper's session id, ephemeral nodes should not be created.
   * Otherwise, session race condition may occur and the newly created ephemeral nodes may not be in
   * the expected session.
   *
   * @param sessionId the new session's id. The ephemeral nodes are expected to be created in this
   *                  session. If this session id is expired, ephemeral nodes should not be created.
   * @throws Exception if any error occurs.
   */
  void handleNewSession(final String sessionId) throws Exception;

  /**
   * Called when a session cannot be re-established. This should be used to implement connection
   * failure handling e.g. retry to connect or pass the error up
   *
   * @param error
   *            The error that prevents a session from being established
   * @throws Exception
   *             On any error.
   */
  void handleSessionEstablishmentError(final Throwable error) throws Exception;
}
