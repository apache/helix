package com.linkedin.helix;

public interface HelixConstants
{
  // ChangeType and PropertyType are the same; remove this
  enum ChangeType
  {
    //@formatter:off
    IDEAL_STATE, CONFIG, LIVE_INSTANCE, CURRENT_STATE,
    MESSAGE, EXTERNAL_VIEW, CONTROLLER, MESSAGES_CONTROLLER, HEALTH
    //@formatter:on
  }

  enum StateModelToken
  {
    ANY_LIVEINSTANCE
  }
}
