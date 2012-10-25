package org.apache.helix.manager.zk;

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

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZkStateChangeListener implements IZkStateListener
{
  private volatile boolean _isConnected;
  private volatile boolean _hasSessionExpired;
  private final ZKHelixManager _zkHelixManager;

  private static Logger logger = Logger.getLogger(ZkStateChangeListener.class);

  public ZkStateChangeListener(ZKHelixManager zkHelixManager)
  {
    this._zkHelixManager = zkHelixManager;

  }

  @Override
  public void handleNewSession()
  {
    // TODO:bug in zkclient .
    // zkclient does not invoke handleStateChanged when a session expires but
    // directly invokes handleNewSession
    _isConnected = true;
    _hasSessionExpired = false;
    _zkHelixManager.handleNewSession();
  }

  @Override
  public void handleStateChanged(KeeperState keeperState) throws Exception
  {
    switch (keeperState)
    {
    case SyncConnected:
      ZkConnection zkConnection =
          ((ZkConnection) _zkHelixManager._zkClient.getConnection());
      logger.info("KeeperState: " + keeperState + ", zookeeper:" + zkConnection.getZookeeper());
      _isConnected = true;
      break;
    case Disconnected:
      logger.info("KeeperState:" + keeperState + ", disconnectedSessionId: "
          + _zkHelixManager._sessionId + ", instance: "
          + _zkHelixManager.getInstanceName() + ", type: "
          + _zkHelixManager.getInstanceType());

      _isConnected = false;
      break;
    case Expired:
      logger.info("KeeperState:" + keeperState + ", expiredSessionId: "
          + _zkHelixManager._sessionId + ", instance: "
          + _zkHelixManager.getInstanceName() + ", type: "
          + _zkHelixManager.getInstanceType());

      _isConnected = false;
      _hasSessionExpired = true;
      break;
    }
  }

  boolean isConnected()
  {
    return _isConnected;
  }

  void disconnect()
  {
    _isConnected = false;
  }

  boolean hasSessionExpired()
  {
    return _hasSessionExpired;
  }
}
