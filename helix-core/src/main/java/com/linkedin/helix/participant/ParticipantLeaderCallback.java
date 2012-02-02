package com.linkedin.helix.participant;

import com.linkedin.helix.NotificationContext;

public interface ParticipantLeaderCallback
{
  public void onCallback(NotificationContext context);
}
