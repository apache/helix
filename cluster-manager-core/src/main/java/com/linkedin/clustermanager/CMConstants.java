package com.linkedin.clustermanager;

public interface CMConstants
{
  // TODO duplicated; remove this; use PropertyType instead
  enum ChangeType
  {
    IDEAL_STATE, CONFIG, LIVE_INSTANCE, CURRENT_STATE, MESSAGE, EXTERNAL_VIEW,
    CONTROLLER, MESSAGES_CONTROLLER, HEALTH
  }
}
