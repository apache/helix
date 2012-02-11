package com.linkedin.helix.participant;

import java.util.List;

import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class ParticipantLeaderStateModelFactory 
  extends StateModelFactory<ParticipantLeaderStateModel>
{
  private final ParticipantLeaderCallback _callback;
  private final List<ChangeType> _notificationTypes;
  public ParticipantLeaderStateModelFactory(ParticipantLeaderCallback callback,
                                            List<ChangeType> notificationTypes)
  {
    if (callback == null || notificationTypes == null || notificationTypes.size() == 0)
    {
      throw new IllegalArgumentException("Require: callback | notificationTypes");
    }
    _callback = callback;
    _notificationTypes = notificationTypes;
  }

  @Override
  public ParticipantLeaderStateModel createNewStateModel(String partitionKey)
  {
    return new ParticipantLeaderStateModel(_callback, _notificationTypes, partitionKey);
  }
}
