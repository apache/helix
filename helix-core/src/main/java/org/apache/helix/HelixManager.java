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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.CustomizedStateChangeListener;
import org.apache.helix.api.listeners.CustomizedStateConfigChangeListener;
import org.apache.helix.api.listeners.CustomizedStateRootChangeListener;
import org.apache.helix.api.listeners.CustomizedViewChangeListener;
import org.apache.helix.api.listeners.CustomizedViewRootChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.participant.HelixStateMachineEngine;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Class that represents the Helix Agent.
 * First class Object any process will interact with<br/>
 * General flow <blockquote>
 *
 * <pre>
 * manager = HelixManagerFactory.getZKHelixManager(
 *    clusterName, instanceName, ROLE, zkAddr);
 *    // ROLE can be participant, spectator or a controller<br/>
 * manager.addPreConnectCallback(cb);
 *    // cb is invoked after a connection is established, but before the node of type ROLE has
 *    // joined the cluster. This is where one can add additional listeners (e.g manager.addSOMEListener(listener);)
 * manager.connect();
 * After connect is invoked the subsequent interactions will be via listener onChange callbacks
 * There will be 3 scenarios for onChange callback, which can be determined using NotificationContext.type
 * INIT -> will be invoked the first time the listener is added
 * CALLBACK -> will be invoked due to datachange in the property value
 * FINALIZE -> will be invoked when listener is removed or session expires
 * manager.disconnect()
 * </pre>
 *
 * </blockquote> Default implementations available
 * @see HelixStateMachineEngine HelixStateMachineEngine for participant
 * @see RoutingTableProvider RoutingTableProvider for spectator
 * @see GenericHelixController RoutingTableProvider for controller
 */
public interface HelixManager {
  @Deprecated
  String ALLOW_PARTICIPANT_AUTO_JOIN =
      ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN;

  /**
   * Start participating in the cluster operations. All listeners will be
   * initialized and will be notified for every cluster state change This method
   * is not re-entrant. One cannot call this method twice.
   * @throws Exception
   */
  void connect() throws Exception;

  /**
   * Check if the connection is alive, code depending on cluster manager must
   * always do this if( manager.isConnected()){ //custom code } This will
   * prevent client in doing anything when its disconnected from the cluster.
   * There is no need to invoke connect again if isConnected return false.
   * @return true if connected, false otherwise
   */
  boolean isConnected();

  /**
   * Disconnect from the cluster. All the listeners will be removed and
   * disconnected from the server. Its important for the client to ensure that
   * new manager instance is used when it wants to connect again.
   */
  void disconnect();

  /**
   * @see IdealStateChangeListener#onIdealStateChange(List, NotificationContext)
   * @param listener
   * @throws Exception
   */
  void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception;

  /**
   * @see IdealStateChangeListener#onIdealStateChange(List, NotificationContext)
   * @param listener
   * @throws Exception
   */
  @Deprecated
  void addIdealStateChangeListener(org.apache.helix.IdealStateChangeListener listener) throws Exception;

  /**
   * @see LiveInstanceChangeListener#onLiveInstanceChange(List, NotificationContext)
   * @param listener
   */
  void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception;

  /**
   * @see LiveInstanceChangeListener#onLiveInstanceChange(List, NotificationContext)
   * @param listener
   */
  @Deprecated
  void addLiveInstanceChangeListener(org.apache.helix.LiveInstanceChangeListener listener) throws Exception;

  /**
   * @see ConfigChangeListener#onConfigChange(List, NotificationContext)
   * @param listener
   * @deprecated replaced by addInstanceConfigChangeListener()
   */
  @Deprecated
  void addConfigChangeListener(ConfigChangeListener listener) throws Exception;

  /**
   * @see InstanceConfigChangeListener#onInstanceConfigChange(List, NotificationContext)
   * @param listener
   */
  void addInstanceConfigChangeListener(InstanceConfigChangeListener listener) throws Exception;

  /**
   * @see InstanceConfigChangeListener#onInstanceConfigChange(List, NotificationContext)
   * @param listener
   */
  @Deprecated
  void addInstanceConfigChangeListener(org.apache.helix.InstanceConfigChangeListener listener) throws Exception;

  /**
   * @see ResourceConfigChangeListener#onResourceConfigChange(List, NotificationContext)
   * @param listener
   */
  void addResourceConfigChangeListener(ResourceConfigChangeListener listener) throws Exception;

  /**
   * @see CustomizedStateConfigChangeListener#onCustomizedStateConfigChange(CustomizedStateConfig,
   *      NotificationContext)
   * @param listener
   */

  void addCustomizedStateConfigChangeListener(
      CustomizedStateConfigChangeListener listener) throws Exception;

  /**
   * @see ClusterConfigChangeListener#onClusterConfigChange(ClusterConfig, NotificationContext)
   * @param listener
   */
  void addClusterfigChangeListener(ClusterConfigChangeListener listener) throws Exception;

  /**
   * @see ScopedConfigChangeListener#onConfigChange(List, NotificationContext)
   * @param listener
   * @param scope
   */
  void addConfigChangeListener(ScopedConfigChangeListener listener, ConfigScopeProperty scope)
      throws Exception;

  /**
   * @see ScopedConfigChangeListener#onConfigChange(List, NotificationContext)
   * @param listener
   * @param scope
   */
  @Deprecated
  void addConfigChangeListener(org.apache.helix.ScopedConfigChangeListener listener, ConfigScopeProperty scope)
      throws Exception;

  /**
   * @see MessageListener#onMessage(String, List, NotificationContext)
   * @param listener
   * @param instanceName
   */
  void addMessageListener(MessageListener listener, String instanceName) throws Exception;


  /**
   * @see MessageListener#onMessage(String, List, NotificationContext)
   * @param listener
   * @param instanceName
   */
  @Deprecated
  void addMessageListener(org.apache.helix.MessageListener listener, String instanceName)
      throws Exception;

  /**
   * @see CurrentStateChangeListener#onStateChange(String, List, NotificationContext)
   * @param listener
   * @param instanceName
   */
  void addCurrentStateChangeListener(CurrentStateChangeListener listener, String instanceName,
      String sessionId) throws Exception;

  /**
   * @see CurrentStateChangeListener#onStateChange(String, List, NotificationContext)
   * @param listener
   * @param instanceName
   */
  @Deprecated
  void addCurrentStateChangeListener(org.apache.helix.CurrentStateChangeListener listener, String instanceName,
      String sessionId) throws Exception;

  /**
   * Uses CurrentStateChangeListener since TaskCurrentState shares the same CurrentState model
   * @see CurrentStateChangeListener#onStateChange(String, List, NotificationContext)
   * @param listener
   * @param instanceName
   */
  default void addTaskCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**

   * @see CustomizedStateRootChangeListener#onCustomizedStateRootChange(String, NotificationContext)
   * @param listener
   * @param instanceName
   */
  void addCustomizedStateRootChangeListener(CustomizedStateRootChangeListener listener,
      String instanceName) throws Exception;

  /**
   * @see CustomizedStateChangeListener#onCustomizedStateChange(String, List, NotificationContext)
   * @param listener
   * @param instanceName
   */
  void addCustomizedStateChangeListener(CustomizedStateChangeListener listener, String instanceName,
      String stateName) throws Exception;

  /**
   * @see ExternalViewChangeListener#onExternalViewChange(List, NotificationContext)
   * @param listener
   */
  void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception;

  /**
   * @see CustomizedViewChangeListener#onCustomizedViewChange(List, NotificationContext)
   * @param listener
   */
  void addCustomizedViewChangeListener(CustomizedViewChangeListener listener, String customizedStateType) throws Exception;

  /**
   * @see CustomizedViewRootChangeListener#onCustomizedViewRootChange(List, NotificationContext)
   * @param listener
   */
  void addCustomizedViewRootChangeListener(CustomizedViewRootChangeListener listener) throws Exception;

  /**
   * @see ExternalViewChangeListener#onExternalViewChange(List, NotificationContext)
   * @param listener
   */
  void addTargetExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception;
  /**
   * @see ExternalViewChangeListener#onExternalViewChange(List, NotificationContext)
   * @param listener
   */
  @Deprecated
  void addExternalViewChangeListener(org.apache.helix.ExternalViewChangeListener listener)
      throws Exception;

  /**
   * Add listener for controller change
   * Used in distributed cluster controller
   */
  void addControllerListener(ControllerChangeListener listener);

  /**
   * Add listener for controller change
   * Used in distributed cluster controller
   */
  @Deprecated
  void addControllerListener(org.apache.helix.ControllerChangeListener listener);

  /**
   * Add message listener for controller
   * @param listener
   */
  void addControllerMessageListener(MessageListener listener);

  /**
   * Add message listener for controller
   * @param listener
   */
  @Deprecated
  void addControllerMessageListener(org.apache.helix.MessageListener listener);

  /**
   * Selectively enable controller pipeline using the given types. This will only take effect
   * when called before connect(), and instance type is CONTROLLER
   * @param types pipeline types to enable
   */
  void setEnabledControlPipelineTypes(Set<Pipeline.Type> types);

  /**
   * Removes the listener. If the same listener was used for multiple changes,
   * all change notifications will be removed.<br/>
   * This will invoke onChange method on the listener with
   * NotificationContext.type set to FINALIZE. Listener can clean up its state.<br/>
   * The data provided in this callback may not be reliable.<br/>
   * When a session expires all listeners will be removed and re-added
   * automatically. <br/>
   * This provides the ability for listeners to either reset their state or do
   * any cleanup tasks.<br/>
   * @param listener
   * @return true if removed successfully, false otherwise
   */
  boolean removeListener(PropertyKey key, Object listener);

  /**
   * Return the client to perform read/write operations on the cluster data
   * store
   * @return ClusterDataAccessor
   */
  HelixDataAccessor getHelixDataAccessor();

  /**
   * Get config accessor
   * @return ConfigAccessor
   */
  ConfigAccessor getConfigAccessor();

  /**
   * Returns the cluster name associated with this cluster manager
   * @return the associated cluster name
   */
  String getClusterName();

  /**
   * Returns a string that can be used to connect to metadata store for this HelixManager instance
   * i.e. for ZkHelixManager, this will have format "{zookeeper-address}:{port}"
   * @return a string used to connect to metadata store
   */
  String getMetadataStoreConnectionString();

  /**
   * Returns the instanceName used to connect to the cluster
   * @return the associated instance name
   */

  String getInstanceName();

  /**
   * Get the sessionId associated with the connection to cluster data store.
   * @return the session identifier
   */
  String getSessionId();

  /**
   * The time stamp is always updated when a notification is received. This can
   * be used to check if there was any new notification when previous
   * notification was being processed. This is updated based on the
   * notifications from listeners registered.
   * @return UNIX timestamp
   */
  long getLastNotificationTime();

  /**
   * Provides admin interface to setup and modify cluster.
   * @return instantiated HelixAdmin
   */
  HelixAdmin getClusterManagmentTool();

  /**
   * Get property store
   * @return the property store that works with ZNRecord objects
   */
  ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore();

  /**
   * Messaging service which can be used to send cluster wide messages.
   * @return messaging service
   */
  ClusterMessagingService getMessagingService();

  /**
   * Get cluster manager instance type
   * @return instance type (e.g. PARTICIPANT, CONTROLLER, SPECTATOR)
   */
  InstanceType getInstanceType();

  /**
   * Get cluster manager version
   * @return the cluster manager version
   */
  String getVersion();

  /**
   * Get helix manager properties read from
   * helix-core/src/main/resources/cluster-manager.properties
   * @return deserialized properties
   */
  HelixManagerProperties getProperties();

  /**
   * @return the state machine engine
   */
  StateMachineEngine getStateMachineEngine();

  /**
   * @return the session start time
   */
  Long getSessionStartTime();

  /**
   * Checks whether the cluster manager is leader and returns the session ID associated to the
   * connection of cluster data store, if and only if it is leader.
   *
   * @return {@code Optional<String>} session ID is present inside the {@code Optional} object
   * if the cluster manager is leader. Otherwise, returns an empty {@code Optional} object.
   */
  default Optional<String> getSessionIdIfLead() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Check if the cluster manager is the leader
   * @return true if this is a controller and a leader of the cluster
   */
  boolean isLeader();

  /**
   * start timer tasks when becomes leader
   */
  void startTimerTasks();

  /**
   * stop timer tasks when becomes standby
   */
  void stopTimerTasks();

  /**
   * Add a callback that is invoked before a participant joins the cluster.</br>
   * This zookeeper connection is established at this time and one can read existing cluster
   * data</br>
   * The purpose of this method is to allow application to have additional logic to validate their
   * existing state and check for any errors.
   * If the validation fails, throw exception/disable the instance. s
   * @see PreConnectCallback#onPreConnect()
   * @param callback
   */
  void addPreConnectCallback(PreConnectCallback callback);

  /**
   * Add a LiveInstanceInfoProvider that is invoked before creating liveInstance.</br>
   * This allows applications to provide additional metadata that will be published to zk and made
   * available for discovery</br>
   * @see LiveInstanceInfoProvider#getAdditionalLiveInstanceInfo()
   * @param liveInstanceInfoProvider
   */
  void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider);

  /**
   * Participant only component that periodically update participant health
   * report to cluster manager server.
   * @return ParticipantHealthReportCollector
   */
  ParticipantHealthReportCollector getHealthReportCollector();
}