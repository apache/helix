package com.linkedin.clustermanager.monitoring;

import java.util.List;

public interface SensorRegistryListener
{
  void onSensorAdded(Sensor<?> sensor);
  
  List<TagFilter> getContextTagFilterList();
}
