package com.linkedin.helix.participant;

import java.util.List;

import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class GenericLeaderStandbyStateModelFactory 
  extends StateModelFactory<GenericLeaderStandbyModel>
{
  private final CustomCodeCallbackHandler _callback;
  private final List<ChangeType> _notificationTypes;
  public GenericLeaderStandbyStateModelFactory(CustomCodeCallbackHandler callback,
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
  public GenericLeaderStandbyModel createNewStateModel(String partitionKey)
  {
    return new GenericLeaderStandbyModel(_callback, _notificationTypes, partitionKey);
  }
}
