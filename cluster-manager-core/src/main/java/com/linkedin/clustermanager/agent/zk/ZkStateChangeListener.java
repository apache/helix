package com.linkedin.clustermanager.agent.zk;

import org.I0Itec.zkclient.IZkStateListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZkStateChangeListener implements IZkStateListener
{
  private volatile boolean _isConnected;
  private volatile boolean _hasSessionExpired;
  private final ZKClusterManager _zkClusterManager;

  private static Logger logger = Logger.getLogger(ZkStateChangeListener.class);

  public ZkStateChangeListener(ZKClusterManager zkClusterManager)
  {
    this._zkClusterManager = zkClusterManager;

  }

  @Override
  public void handleNewSession()
  {
    _zkClusterManager.handleNewSession();
  }

  @Override
  public void handleStateChanged(KeeperState keeperState) throws Exception
  {
    logger.info("KeeperState:" + keeperState);
    
    switch (keeperState)
    {

    case SyncConnected:
      _isConnected = true;
      break;
    case Disconnected:
      _isConnected = false;
      break;
    case Expired:
      _isConnected = false;
      _hasSessionExpired = true;
      break;
    }
  }

  boolean isConnected()
  {
    return _isConnected;
  }

  void disconnect()
  {
    _isConnected = false;
  }
  
  boolean hasSessionExpired()
  {
    return _hasSessionExpired;
  }
}
