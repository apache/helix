package org.apache.helix;

public interface PreConnectCallback
{
  /**
   * Callback function that is called by HelixManager before connected to zookeeper. If
   * exception are thrown HelixManager will not connect and no live instance is created
   * 
   * @see ZkHelixManager#handleNewSessionAsParticipant()
   */
  public void onPreConnect();
}
