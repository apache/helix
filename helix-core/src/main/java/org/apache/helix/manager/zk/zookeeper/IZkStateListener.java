/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.manager.zk.zookeeper;

import org.apache.zookeeper.Watcher.Event.KeeperState;

public interface IZkStateListener {

  /**
   * Called when the zookeeper connection state has changed.
   *
   * @param state
   *            The new state.
   * @throws Exception
   *             On any error.
   */
  void handleStateChanged(KeeperState state) throws Exception;

  /**
   * Called after the zookeeper session has expired and a new session has been created.
   * You would have to re-create any ephemeral nodes here.
   *
   * This is deprecated, because it may cause session race condition when creating ephemeral nodes.
   * Please use {@link #handleNewSession(String)} instead, which takes care of race condition.
   *
   * @throws Exception
   *             On any error.
   */
  @Deprecated
  default void handleNewSession() throws Exception {
    handleNewSession(null);
  }

  /**
   * Called after the zookeeper session has expired and a new session has been created. The new
   * session id has to be passed in as the parameter. And you would have to re-create any ephemeral
   * nodes here.
   *
   * @param sessionId The new session's id.
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
