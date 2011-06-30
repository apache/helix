package com.linkedin.clustermanager.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;

public class ExternalViewHolder
{
  private Map<String, List<ZNRecord>> _externalViewMap;

  public ExternalViewHolder()
  {
    _externalViewMap = new HashMap<String, List<ZNRecord>>();
  }

  public void refresh(List<ZNRecord> currentStates)
  {
    for (ZNRecord record : currentStates)
    {
      String stateUnitGroup = record.getId();
      _externalViewMap.put(stateUnitGroup, currentStates);
    }
  }

  public Map<String, List<ZNRecord>> getCurrentStatesListMap()
  {
    return _externalViewMap;
  }

}
