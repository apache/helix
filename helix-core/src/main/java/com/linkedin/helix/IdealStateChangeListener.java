package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.model.IdealState;

public interface IdealStateChangeListener
{

  void onIdealStateChange(List<IdealState> idealState,
      NotificationContext changeContext);

}
