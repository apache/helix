package com.linkedin.clustermanager.core;

import java.util.List;

import com.linkedin.clustermanager.core.listeners.ConfigChangeListener;
import com.linkedin.clustermanager.core.listeners.CurrentStateChangeListener;
import com.linkedin.clustermanager.core.listeners.ExternalViewChangeListener;
import com.linkedin.clustermanager.core.listeners.IdealStateChangeListener;
import com.linkedin.clustermanager.core.listeners.LiveInstanceChangeListener;
import com.linkedin.clustermanager.core.listeners.MessageListener;

/**
 * First class Object any process will interact with General flow manager =
 * ClusterManagerFactory.getManager(); manager.connect();
 * manager.addSOMEListener(); manager.start() After start is invoked the
 * interactions will be via listener callback functions ... ..
 * manager.disconnect()
 * 
 * @author kgopalak
 */
public interface ClusterManager {

	/**
	 * Start participating in the cluster operations. All listeners will be
	 * initialized and will be notified for every cluster state change This
	 * method is not re-entrant. One cannot call this method twice.
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
	 * @see IdealStateChangeListener#onIdealStateChange(List,
	 *      NotificationContext)
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
	void addLiveInstanceChangeListener(LiveInstanceChangeListener listener);

	/**
	 * @see ConfigChangeListener#onConfigChange(List, NotificationContext)
	 * @param listener
	 */
	void addConfigChangeListener(ConfigChangeListener listener);

	/**
	 * @see MessageListener#onMessage(String, List, NotificationContext)
	 * @param listener
	 * @param instanceName
	 */
	void addMessageListener(MessageListener listener, String instanceName);

	/**
	 * @see CurrentStateChangeListener#onStateChange(String, List,
	 *      NotificationContext)
	 * @param listener
	 * @param instanceName
	 */

	void addCurrentStateChangeListener(CurrentStateChangeListener listener,
			String instanceName);

	/**
	 * @see ExternalViewChangeListener#onExternalViewChange(List,
	 *      NotificationContext)
	 * @param listener
	 */
	void addExternalViewChangeListener(ExternalViewChangeListener listener);

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
}
