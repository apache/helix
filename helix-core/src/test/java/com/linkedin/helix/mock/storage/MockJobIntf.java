package com.linkedin.helix.mock.storage;

import com.linkedin.helix.HelixAgent;

public interface MockJobIntf
{
  public void doPreConnectJob(HelixAgent manager);
  public void doPostConnectJob(HelixAgent manager);
}
