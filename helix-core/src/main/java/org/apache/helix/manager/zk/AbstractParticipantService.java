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

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixParticipant;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;

import com.google.common.util.concurrent.AbstractService;

/**
 * A modeling of a helix participant as a self-contained service.
 */
public abstract class AbstractParticipantService extends AbstractService {
  private final ClusterId _clusterId;
  private final ParticipantId _participantId;
  private HelixParticipant _participant;
  private HelixConnection _connection;

  /**
   * Initialize the service.
   * @param connection A live Helix connection
   * @param clusterId the cluster to join
   * @param participantId a unique identifier that this participant will join with
   */
  public AbstractParticipantService(HelixConnection connection, ClusterId clusterId,
      ParticipantId participantId) {
    _connection = connection;
    _clusterId = clusterId;
    _participantId = participantId;
  }

  @Override
  protected void doStart() {
    _participant = _connection.createParticipant(_clusterId, _participantId);

    // add a preconnect callback
    _participant.addPreConnectCallback(new PreConnectCallback() {
      @Override
      public void onPreConnect() {
        onPreJoinCluster();
      }
    });

    // register state machine and other initialization
    init();

    // start and notify
    if (!_connection.isConnected()) {
      _connection.connect();
    }
    _participant.start();
    notifyStarted();
  }

  @Override
  protected void doStop() {
    _participant.stop();
    notifyStopped();
  }

  /**
   * Initialize the participant. For example, here is where you can register a state machine: <br/>
   * <br/>
   * <code>
   * HelixParticipant participant = getParticipant();
   * participant.getStateMachineEngine().registerStateModelFactory(stateModelDefId, factory);
   * </code><br/>
   * <br/>
   * This code is called prior to starting the participant.
   */
  public abstract void init();

  /**
   * Complete any tasks that require a live Helix connection. This function is called before the
   * participant declares itself ready to receive state transitions.
   */
  public abstract void onPreJoinCluster();

  /**
   * Get an instantiated participant instance.
   * @return HelixParticipant
   */
  public HelixParticipant getParticipant() {
    return _participant;
  }
}
