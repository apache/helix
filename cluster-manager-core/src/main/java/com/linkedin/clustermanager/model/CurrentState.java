package com.linkedin.clustermanager.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.clustermanager.CMConstants.ZNAttribute.*;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message.Attributes;

public class CurrentState
{
  private final ZNRecord record;

  public CurrentState(ZNRecord record)
  {
    this.record = record;
  }

  public String getResourceGroupName()
  {
    return record.getId();
  }

  public Map<String, String> getResourceKeyStateMap()
  {
    Map<String, String> map = new HashMap<String, String>();
    Map<String, Map<String, String>> mapFields = record.getMapFields();
    for (String resourceKey : mapFields.keySet())
    {
      Map<String, String> tempMap = mapFields.get(resourceKey);
      if (tempMap != null)
      {
        map.put(resourceKey, tempMap.get(CURRENT_STATE.toString()));
      }
    }
    return map;
  }

  public String getSessionId()
  {
    return record.getSimpleField(SESSION_ID.toString());
  }

  public String getState(String resourceKeyStr)
  {
    Map<String, String> mapField = record.getMapField(resourceKeyStr);
    if (mapField != null)
    {
      return mapField.get(CURRENT_STATE.toString());
    }
    return null;
  }
  
  public String getStateModelDefRef()
  {
    return record.getSimpleField(Attributes.STATE_MODEL_DEF.toString());
  }

}
