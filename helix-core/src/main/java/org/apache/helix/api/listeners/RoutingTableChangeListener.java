package org.apache.helix.api.listeners;

import org.apache.helix.spectator.RoutingTableSnapshot;

/**
 * Interface to implement to listen for changes to RoutingTable on changes
 */
public interface RoutingTableChangeListener {

  /**
   * Invoked when RoutingTable on changes
   *
   * @param routingTableSnapshot
   * @param context
   */
  void onRoutingTableChange(RoutingTableSnapshot routingTableSnapshot, Object context);
}
