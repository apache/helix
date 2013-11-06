package org.apache.helix;

import org.apache.helix.api.id.ControllerId;

public interface HelixController extends HelixRole, HelixService, HelixConnectionStateListener {

  /**
   * get controller id
   * @return controller id
   */
  ControllerId getControllerId();

  /**
   * tell if this controller is leader of cluster
   * @return
   */
  boolean isLeader();
}
