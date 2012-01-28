package com.linkedin.helix;

public interface CMConstants
{
  enum ChangeType
  {
    IDEAL_STATE, CONFIG, LIVE_INSTANCE, CURRENT_STATE, MESSAGE, EXTERNAL_VIEW,
    CONTROLLER, MESSAGES_CONTROLLER, HEALTH
  }
}
