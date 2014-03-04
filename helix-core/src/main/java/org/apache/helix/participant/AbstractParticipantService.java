package org.apache.helix.participant;

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
  boolean initialized;

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
  protected final void doStart() {
    _participant = _connection.createParticipant(_clusterId, _participantId);

    // add a preconnect callback
    _participant.addPreConnectCallback(new PreConnectCallback() {
      @Override
      public void onPreConnect() {
        if (initialized) {
          onReconnect();
        } else {
          init();
          initialized = true;
        }
      }
    });

    // start and notify
    if (!_connection.isConnected()) {
      _connection.connect();
    }
    _participant.start();
    notifyStarted();
  }

  @Override
  protected final void doStop() {
    _participant.stop();
    notifyStopped();
  }

  /**
   * Invoked when connection is re-established to zookeeper. Typical scenario this is invoked is
   * when there is a long GC pause that causes the node to disconnect from the cluster and
   * reconnects. NOTE: When the service disconnects all its states are reset to initial state.
   */
  protected void onReconnect() {
    // default implementation does nothing.
  }

  /**
   * Initialize the participant. For example, here is where you can
   * <ul>
   * <li>Read configuration of the cluster,resource, node</li>
   * <li>Read configuration of the cluster,resource, node register a state machine: <br/>
   * <br/>
   * <code>
   * HelixParticipant participant = getParticipant();
   * participant.getStateMachineEngine().registerStateModelFactory(stateModelDefId, factory);
   * </code><br/>
   * <br/>
   * </li>
   * </ul>
   * This code is called after connecting to zookeeper but before creating the liveinstance.
   */
  protected abstract void init();

  /**
   * Get an instantiated participant instance.
   * @return HelixParticipant
   */
  public HelixParticipant getParticipant() {
    return _participant;
  }

  /**
   * @return ClusterId
   * @see {@link ClusterId}
   */
  public ClusterId getClusterId() {
    return _clusterId;
  }

  /**
   * @see {@link ParticipantId}
   * @return
   */
  public ParticipantId getParticipantId() {
    return _participantId;
  }

  /**
   * @see {@link HelixConnection}
   * @return HelixConnection
   */
  public HelixConnection getConnection() {
    return _connection;
  }

}
