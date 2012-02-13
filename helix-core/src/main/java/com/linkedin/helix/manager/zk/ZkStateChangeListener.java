package com.linkedin.helix.manager.zk;

import org.I0Itec.zkclient.IZkStateListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZkStateChangeListener implements IZkStateListener
{
  private volatile boolean _isConnected;
  private volatile boolean _hasSessionExpired;
  private final ZKHelixManager _zkHelixManager;

  private static Logger logger = Logger.getLogger(ZkStateChangeListener.class);

  public ZkStateChangeListener(ZKHelixManager zkHelixManager)
  {
    this._zkHelixManager = zkHelixManager;

  }

  @Override
  public void handleNewSession()
  {
    //TODO:bug in zkclient . 
    //zkclient does not invoke handleStateChanged when a session expires but 
    //directly invokes handleNewSession 
    _isConnected = true;
    _hasSessionExpired = false;
    _zkHelixManager.handleNewSession();
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
