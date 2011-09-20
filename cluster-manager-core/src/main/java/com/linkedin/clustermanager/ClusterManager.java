package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.controller.GenericClusterController;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.spectator.RoutingTableProvider;
import com.linkedin.clustermanager.store.PropertySerializer;
import com.linkedin.clustermanager.store.PropertyStore;

/**
 * First class Object any process will interact with<br/>
 * General flow 
 * <blockquote>
 * <pre> 
 * manager = ClusterManagerFactory.getManagerFor<ROLE>(); ROLE can be participant, spectator or a controller<br/>
 * manager.connect();
 * manager.addSOMEListener(); 
 * manager.start() 
 * After start is invoked the subsequent interactions will be via listener onChange callbacks 
 * There will be 3 scenarios for onChange callback, which can be determined using NotificationContext.type 
 * INIT -> will be invoked the first time the listener is added
 * CALLBACK -> will be invoked due to datachange in the property value
 * FINALIZE -> will be invoked when listener is removed or session expires
 * manager.disconnect()
 * </pre>
 * </blockquote>
 * Default implementations available 
 * @see StateMachineEngine for participant
 * @see RoutingTableProvider for spectator
 * @see GenericClusterController  for controller
 * @author kgopalak
 */
public interface ClusterManager
{

  /**
   * Start participating in the cluster operations. All listeners will be
   * initialized and will be notified for every cluster state change This method
   * is not re-entrant. One cannot call this method twice.
   * 
   * @throws Exception
   */
  void connect() throws Exception;

  /**
   * Check if the connection is alive, code depending on cluster manager must
   * always do this if( manager.isConnected()){ //custom code } This will
   * prevent client in doing anything when its disconnected from the cluster.
   * There is no need to invoke connect again if isConnected return false.
   * 
   * @return
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
  void addIdealStateChangeListener(IdealStateChangeListener listener)
      throws Exception;

  /**
   * @see LiveInstanceChangeListener#onLiveInstanceChange(List,
   *      NotificationContext)
   * @param listener
   */
  void addLiveInstanceChangeListener(LiveInstanceChangeListener listener)
      throws Exception;

  /**
   * @see ConfigChangeListener#onConfigChange(List, NotificationContext)
   * @param listener
   */
  void addConfigChangeListener(ConfigChangeListener listener) throws Exception;

  /**
   * @see MessageListener#onMessage(String, List, NotificationContext)
   * @param listener
   * @param instanceName
   */
  void addMessageListener(MessageListener listener, String instanceName)
      throws Exception;

  /**
   * @see CurrentStateChangeListener#onStateChange(String, List,
   *      NotificationContext)
   * @param listener
   * @param instanceName
   */

  void addCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception;

  /**
   * @see ExternalViewChangeListener#onExternalViewChange(List,
   *      NotificationContext)
   * @param listener
   */
  void addExternalViewChangeListener(ExternalViewChangeListener listener)
      throws Exception;
  /**
   * Removes the listener. If the same listener was used for multiple changes, all change notifications will be removed.<br/>
   * This will invoke onChange method on the listener with NotificationContext.type set to FINALIZE. Listener can clean up its state.<br/>
   * The data provided in this callback may not be reliable.<br/>
   * When a session expires all listeners will be removed and re-added automatically. <br/>
   * This provides the ability for listeners to either reset their state or do any cleanup tasks.<br/>
   * @param listener
   * @return
   */
  boolean removeListener(Object listener);
  /**
   * Return the client to perform read/write operations on the cluster data
   * store
   * 
   * @return ClusterDataAccessor
   */
  ClusterDataAccessor getDataAccessor();

  /**
   * Returns the cluster name associated with this cluster manager
   * 
   * @return
   */
  String getClusterName();

  /**
   * Returns the instanceName used to connect to the cluster
   * 
   * @return
   */

  String getInstanceName();

  /**
   * Get the sessionId associated with the connection to cluster data store.
   */
  String getSessionId();
  
  /**
   * The time stamp is always updated when a notification is received.
   * This can be used to check if there was any new notification when previous notification was being processed.
   * This is updated based on the notifications from listeners registered.
   */
  long getLastNotificationTime();
  
  // distributed cluster controller
  /**
   * Add listener for controller change
   */
  void addControllerListener(ControllerChangeListener listener);
  /**
   * Provides admin interface to setup and modify cluster.
   * @return
   */
  ClusterManagementService getClusterManagmentTool();

  /**
   * Provide get property store for a cluster
   * @param rootNamespace
   * @param serializer
   * @return
   */
  <T> PropertyStore<T> getPropertyStore(String rootNamespace, PropertySerializer<T> serializer);

  /**
   * Messaging service which can be used to send cluster wide messages.
   * 
   */
  ClusterMessagingService getMessagingService();
  
  /**
   * 
   * @return
   */
  InstanceType getInstanceType();
}
