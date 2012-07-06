package com.linkedin.helix;

/**
 * Callback function that is called by HelixManager before connected to zookeeper.
 * If exception are thrown HelixManager will not connect.
 * @see ZkHelixManager#handleNewSessionAsParticipant()
 * */
public interface PreConnectCallback
{
  public void onPreConnect();
}