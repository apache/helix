package com.linkedin.helix;

public interface CMConstants
{
  // ChangeType and PropertyType are the same; remove this
  enum ChangeType
  {
    IDEAL_STATE, CONFIG, LIVE_INSTANCE, CURRENT_STATE, MESSAGE, EXTERNAL_VIEW,
    CONTROLLER, MESSAGES_CONTROLLER, HEALTH
  }
  
  enum StateModelToken
  {
    ANY_LIVEINSTANCE
  }
}
