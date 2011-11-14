package com.linkedin.clustermanager.model;

import static com.linkedin.clustermanager.CMConstants.ZNAttribute.CURRENT_STATE;
import static com.linkedin.clustermanager.CMConstants.ZNAttribute.SESSION_ID;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
  public void setSessionId(String sessionId)
  {
    record.setSimpleField(SESSION_ID.toString(), sessionId);
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

  public void setStateModelDefRef(String stateModelName)
  {
    record
        .setSimpleField(Attributes.STATE_MODEL_DEF.toString(), stateModelName);
  }

  public String getStateModelDefRef()
  {
    return record.getSimpleField(Attributes.STATE_MODEL_DEF.toString());
  }

  public void setState(String resourceKeyStr, String state)
  {
    if (record.getMapField(resourceKeyStr) == null)
    {
      record.setMapField(resourceKeyStr, new TreeMap<String, String>());
    }
    record.getMapField(resourceKeyStr).put(CURRENT_STATE.toString(), state);

  }

  public ZNRecord getRecord()
  {
    return record;
  }
}
