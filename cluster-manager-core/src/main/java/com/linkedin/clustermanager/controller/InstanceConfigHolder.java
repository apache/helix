package com.linkedin.clustermanager.controller;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;

public class InstanceConfigHolder
{
  private List<ZNRecord> _instanceConfigList;

  private Map<String, Boolean> _instanceEnabledMap = new TreeMap<String, Boolean>();

  public void refresh(List<ZNRecord> instanceConfigs)
  {
    _instanceConfigList = instanceConfigs;
    updateEnableMap();
  }

  void updateEnableMap()
  {
    _instanceEnabledMap.clear();
    for (ZNRecord config : _instanceConfigList)
    {
      boolean enabled = Boolean.parseBoolean(config
          .getSimpleField(InstanceConfigProperty.ENABLED.toString()));
      _instanceEnabledMap.put(config.getId(), enabled);
    }
  }

  public boolean isEnabled(String instanceName)
  {
    if (!_instanceEnabledMap.containsKey(instanceName))
    {
      return false;
    }
    return _instanceEnabledMap.get(instanceName);
  }
}
