package com.linkedin.clustermanager.controller.stages;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.store.PropertyStore;

public class BaseStageTest
{
  ClusterManager _manager;
  ClusterDataAccessor _accessor;

  public void setup()
  {

    _manager = new Mocks.MockManager()
    {
      public ClusterDataAccessor getDataAccessor()
      {

        return _accessor;
      };
    };
  }
}
