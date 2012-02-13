package com.linkedin.helix.mock.storage;

import com.linkedin.helix.HelixManager;

public interface MockJobIntf
{
  public void doPreConnectJob(HelixManager manager);
  public void doPostConnectJob(HelixManager manager);
}
