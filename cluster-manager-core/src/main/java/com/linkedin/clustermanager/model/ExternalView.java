package com.linkedin.clustermanager.model;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordDecorator;

/**
 * External view is an aggregation (across all instances)
 *  of current states for the resources in a resource group
 */
public class ExternalView extends ZNRecordDecorator
{
  public ExternalView(String resourceGroup)
  {
    super(new ZNRecord(resourceGroup));
  }

  public ExternalView(ZNRecord record)
  {
    super(record);
  }

  public void setState(String resourceKeyName, String instance, String state)
  {
    if(_record.getMapField(resourceKeyName) == null)
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

  @Override
  public boolean isValid()
  {
    return true;
  }
}
