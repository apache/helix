package org.apache.helix;

public interface HelixConnectionStateListener {
  /**
   * called after connection is established
   */
  void onConnected();

  /**
   * called before disconnect
   */
  void onDisconnecting();
}
