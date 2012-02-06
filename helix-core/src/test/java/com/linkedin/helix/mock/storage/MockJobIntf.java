package com.linkedin.helix.mock.storage;

import com.linkedin.helix.ClusterManager;

public interface MockJobIntf
{
  public void doPreConnectJob(ClusterManager manager);
  public void doPostConnectJob(ClusterManager manager);
}
