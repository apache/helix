package com.linkedin.clustermanager.model;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.linkedin.clustermanager.ZNRecord;

public class ExternalView
{

  private final ZNRecord _record;

  public ExternalView(ZNRecord record)
  {
    _record = record;
  }

  public void setState(String resourceKeyName, String instance, String state)
  {
    if (_record.getMapField(resourceKeyName) == null)
    {
      _record.setMapField(resourceKeyName, new TreeMap<String, String>());
    }
    _record.getMapField(resourceKeyName).put(instance, state);
  }

  public void setStateMap(String resourceKeyName,
      Map<String, String> currentStateMap)
  {
    _record.setMapField(resourceKeyName, currentStateMap);
  }

  public ZNRecord getRecord()
  {
    return _record;
  }

  public Set<String> getResourceKeys()
  {
    return _record.getMapFields().keySet();
  }

  public Map<String, String> getStateMap(String resourceKeyName)
  {
    return _record.getMapField(resourceKeyName);
  }

  public String getResourceGroup()
  {
    return _record.getId();
  }
}
