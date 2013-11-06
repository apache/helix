package org.apache.helix;

import org.apache.helix.api.id.ControllerId;
import org.apache.helix.participant.StateMachineEngine;

/**
 * Autonomous controller
 */
public interface HelixAutoController extends HelixRole, HelixService, HelixConnectionStateListener {
  /**
   * get controller id
   * @return controller id
   */
  ControllerId getControllerId();

  /**
   * get state machine engine
   * @return state machine engine
   */
  StateMachineEngine getStateMachineEngine();

  /**
   * add pre-connect callback
   * @param callback
   */
  void addPreConnectCallback(PreConnectCallback callback);

  /**
   * Add a LiveInstanceInfoProvider that is invoked before creating liveInstance.</br>
   * This allows applications to provide additional information that will be published to zookeeper
   * and become available for discovery</br>
   * @see LiveInstanceInfoProvider#getAdditionalLiveInstanceInfo()
   * @param liveInstanceInfoProvider
   */
  void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider);

  /**
   * tell if this controller is leader of cluster
   * @return
   */
  boolean isLeader();

}
