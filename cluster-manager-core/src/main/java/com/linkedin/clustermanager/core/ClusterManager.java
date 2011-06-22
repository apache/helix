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
 * @author kgopalak
 */
public interface ClusterManager
{
    /**
     * disconnect from the cluster
     */
    void disconnect();

    /**
     * listener.handleIdealStateChange will be invoked when there is change any
     * IdealState
     * @param listener
     * @throws Exception
     */
    void addIdealStateChangeListener(IdealStateChangeListener listener)
            throws Exception;

    void addLiveInstanceChangeListener(LiveInstanceChangeListener listener);

    void addConfigChangeListener(ConfigChangeListener listener);

    void addMessageListener(MessageListener listener, String instanceName);

    void addCurrentStateChangeListener(CurrentStateChangeListener listener,
            String instanceName);

    void addExternalViewChangeListener(ExternalViewChangeListener listener);

    // void addListeners(List<Object> listeners);

    /**
     * Return the client to perform operations on the cluster
     * @return
     */
    ClusterDataAccessor getClient();

    String getClusterName();

    String getInstanceName();

    /**
     * Start participating in the cluster operations. All listeners will be
     * initialized and will be notified for every cluster state change
     */
    void start();

    String getSessionId();
}
