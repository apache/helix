package com.linkedin.clustermanager.controller;

/**
 * when ClusterManagerMain gets interrupted
 * need to close the zk connection to do a clean shutdown
 *  
 */

import com.linkedin.clustermanager.ClusterManager;

public class ClusterManagerMainException extends Exception
{
  private final ClusterManager _manager;
  
  public ClusterManagerMainException(ClusterManager manager, String msg)
  {
    super(msg);
    _manager = manager;
  }
  
  public ClusterManager getManager()
  {
    return _manager;
  }
}
