package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.model.IdealState;

public interface IdealStateChangeListener
{

  void onIdealStateChange(List<IdealState> idealState,
      NotificationContext changeContext);

}
