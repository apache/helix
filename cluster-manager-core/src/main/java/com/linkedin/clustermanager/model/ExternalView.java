package com.linkedin.clustermanager.model;

import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;

public class ExternalView
{

  private final ZNRecord _record;

  public ExternalView(ZNRecord record)
  {
    _record = record;
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

}
