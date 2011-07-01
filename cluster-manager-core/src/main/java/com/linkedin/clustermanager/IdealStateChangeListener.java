package com.linkedin.clustermanager;

import java.util.List;

public interface IdealStateChangeListener
{

  void onIdealStateChange(List<ZNRecord> idealState,
      NotificationContext changeContext);

}
