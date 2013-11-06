package org.apache.helix;

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;

/**
 * helix-role i.e. participant, controller, auto-controller
 */
public interface HelixRole {
  /**
   * get the underlying connection
   * @return helix-connection
   */
  HelixConnection getConnection();

  /**
   * get cluster id to which this role belongs
   * @return cluster id
   */
  ClusterId getClusterId();

  /**
   * get id of this helix-role
   * @return id
   */
  Id getId();

  /**
   * helix-role type
   * @return
   */
  InstanceType getType();

  /**
   * get the messaging-service
   * @return messaging-service
   */
  ClusterMessagingService getMessagingService();

}
