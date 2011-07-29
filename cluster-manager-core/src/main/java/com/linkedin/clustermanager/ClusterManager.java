package com.linkedin.clustermanager;

import java.util.List;

/**
 * First class Object any process will interact with General flow manager =
 * ClusterManagerFactory.getManager(); manager.connect();
 * manager.addSOMEListener(); manager.start() After start is invoked the
 * interactions will be via listener callback functions ... ..
 * manager.disconnect()
 * 
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
      String instanceName) throws Exception;

  /**
   * @see ExternalViewChangeListener#onExternalViewChange(List,
   *      NotificationContext)
   * @param listener
   */
  void addExternalViewChangeListener(ExternalViewChangeListener listener)
      throws Exception;

  // void addListeners(List<Object> listeners);

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
}
