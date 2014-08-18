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

import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.store.HelixPropertyStore;

/**
 * Helix connection (aka helix manager)
 */
public interface HelixConnection {

  /**
   * start connection
   */
  void connect();

  /**
   * close connection
   */
  void disconnect();

  /**
   * test if connection is started
   * @return true if connection is started, false otherwise
   */
  boolean isConnected();

  /**
   * get session id
   * @return session id of current connection
   */
  SessionId getSessionId();

  /**
   * get session timeout
   * @return session timeout in millisecond
   */
  int getSessionTimeout();

  /**
   * create a helix-participant
   * @param clusterId
   * @param participantId
   * @return helix-participant
   */
  HelixParticipant createParticipant(ClusterId clusterId, ParticipantId participantId);

  /**
   * create a helix-controller
   * @param clusterId
   * @param controllerId
   * @return helix-controller
   */
  HelixController createController(ClusterId clusterId, ControllerId controllerId);

  /**
   * create a multi-cluster controller
   * @param clusterId
   * @param controllerId
   * @return
   */
  HelixMultiClusterController createMultiClusterController(ClusterId clusterId, ControllerId controllerId);

  /**
   * create a cluster-accessor
   * @param clusterId
   * @return cluster-accessor
   */
  ClusterAccessor createClusterAccessor(ClusterId clusterId);

  /**
   * Provides admin interface to setup and modify cluster
   * @return instantiated HelixAdmin
   */
  HelixAdmin createClusterManagementTool();

  /**
   * create a default property-store for a cluster
   * @param clusterId
   * @return property-store
   */
  HelixPropertyStore<ZNRecord> createPropertyStore(ClusterId clusterId);

  /**
   * create a data-accessor
   * @param clusterId
   * @return data-accessor
   */
  HelixDataAccessor createDataAccessor(ClusterId clusterId);

  /**
   * get config accessor
   * @return config accessor
   */
  ConfigAccessor getConfigAccessor();

  /**
   * add ideal state change listener
   * @param role
   * @param listener
   * @param clusterId
   */
  void addIdealStateChangeListener(HelixRole role, IdealStateChangeListener listener,
      ClusterId clusterId);

  /**
   * add controller message listener
   * @param role
   * @param listener
   * @param clusterId
   */
  void addControllerMessageListener(HelixRole role, MessageListener listener, ClusterId clusterId);

  /**
   * add controller listener
   * @param role
   * @param listener
   * @param clusterId
   */
  void addControllerListener(HelixRole role, ControllerChangeListener listener, ClusterId clusterId);

  /**
   * add live-instance listener using this connection
   * @param role
   * @param listener
   * @param clusterId
   */
  void addLiveInstanceChangeListener(HelixRole role, LiveInstanceChangeListener listener,
      ClusterId clusterId);

  /**
   * add message listener
   * @param role
   * @param listener
   * @param clusterId
   * @param participantId
   */
  void addMessageListener(HelixRole role, MessageListener listener, ClusterId clusterId,
      ParticipantId participantId);

  /**
   * add instance config change listener
   * @see InstanceConfigChangeListener#onInstanceConfigChange(List, NotificationContext)
   * @param role
   * @param listener
   * @param clusterId
   */
  void addInstanceConfigChangeListener(HelixRole role, InstanceConfigChangeListener listener,
      ClusterId clusterId);

  /**
   * add config change listener for a scope
   * @see ScopedConfigChangeListener#onConfigChange(List, NotificationContext)
   * @param role
   * @param listener
   * @param clusterId
   * @param scope
   */
  void addConfigChangeListener(HelixRole role, ScopedConfigChangeListener listener,
      ClusterId clusterId, ConfigScopeProperty scope);

  /**
   * add current state change listener
   * @param role
   * @param listener
   * @param clusterId
   * @param participantId
   * @param sessionId
   */
  void addCurrentStateChangeListener(HelixRole role, CurrentStateChangeListener listener,
      ClusterId clusterId, ParticipantId participantId, SessionId sessionId);

  /**
   * add external view change listener
   * @see ExternalViewChangeListener#onExternalViewChange(List, NotificationContext)
   * @param listener
   */
  void addExternalViewChangeListener(HelixRole role, ExternalViewChangeListener listener,
      ClusterId clusterId);

  /**
   * remove a listener
   * @param role
   * @param listener
   * @param key
   * @return
   */
  boolean removeListener(HelixRole role, Object listener, PropertyKey key);

  /**
   * add connection state listener
   * @param listener
   */
  void addConnectionStateListener(HelixConnectionStateListener listener);

  /**
   * remove connection state listener
   * @param listener
   */
  void removeConnectionStateListener(HelixConnectionStateListener listener);

  /**
   * create messaging service using this connection
   * @param role
   * @return messaging-service
   */
  ClusterMessagingService createMessagingService(HelixRole role);

  /**
   * get helix version
   * @return helix version
   */
  String getHelixVersion();

  /**
   * get helix properties
   * @return helix-properties
   */
  HelixManagerProperties getHelixProperties();
}
