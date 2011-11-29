package com.linkedin.clustermanager;

public interface CMConstants
{

  enum ZNAttribute
  {
    SESSION_ID, CURRENT_STATE, HOST, PORT, CLUSTER_MANAGER_VERSION, LEADER
  }

  enum ChangeType
  {
    IDEAL_STATE, CONFIG, LIVE_INSTANCE, CURRENT_STATE, MESSAGE, EXTERNAL_VIEW,
    CONTROLLER
  }
}
