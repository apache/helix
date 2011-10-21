package com.linkedin.clustermanager.controller.stages;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.Mocks;

public class BaseStageTest
{
  ClusterManager _manager;
  ClusterDataAccessor _accessor;

  public void setup()
  {
    String clusterName= "testCluster";
    _manager = new Mocks.MockManager(clusterName);
    
  }
}
