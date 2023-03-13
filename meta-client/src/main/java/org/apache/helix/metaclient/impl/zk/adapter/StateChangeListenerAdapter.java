package org.apache.helix.metaclient.impl.zk.adapter;

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

import org.apache.helix.metaclient.api.ConnectStateChangeListener;
import org.apache.helix.metaclient.impl.zk.util.ZkMetaClientUtil;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;


public class StateChangeListenerAdapter implements IZkStateListener {
  private final ConnectStateChangeListener _listener;

  public StateChangeListenerAdapter(ConnectStateChangeListener listener) {
    _listener = listener;
  }

  @Override
  public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handleNewSession(String sessionId) throws Exception {
    // This function will be invoked when connection is established. It is a  no-op for metaclient.
    // MetaClient will expose this to user as 'handleStateChanged' already covers state change
    // notification for new connection establishment.
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) throws Exception {
      _listener.handleConnectionEstablishmentError(error);
  }

  @Override
  public void handleStateChanged(Watcher.Event.KeeperState prevState,
      Watcher.Event.KeeperState curState) throws Exception {
    _listener.handleConnectStateChanged(
        ZkMetaClientUtil.translateKeeperStateToMetaClientConnectState(prevState),
        ZkMetaClientUtil.translateKeeperStateToMetaClientConnectState(curState));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StateChangeListenerAdapter that = (StateChangeListenerAdapter) o;
    return _listener.equals(that._listener);
  }

  @Override
  public int hashCode() {
    return _listener.hashCode();
  }
}
