package com.linkedin.clustermanager.model;

import static com.linkedin.clustermanager.CMConstants.ZNAttribute.CURRENT_STATE;
import static com.linkedin.clustermanager.CMConstants.ZNAttribute.SESSION_ID;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordAndStat;
import com.linkedin.clustermanager.model.Message.Attributes;

public class CurrentState extends ZNRecordAndStat
{
//  private final ZNRecord record;

  public CurrentState(ZNRecord record)
  {
    this(record, null);
  }

  public CurrentState(ZNRecord record, Stat stat)
  {
    super(record, stat);
//    this.record = record;
  }

  public String getResourceGroupName()
  {
    return _record.getId();
  }

  public Map<String, String> getResourceKeyStateMap()
  {
    Map<String, String> map = new HashMap<String, String>();
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
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
    return _record.getSimpleField(SESSION_ID.toString());
  }
  public void setSessionId(String sessionId)
  {
    _record.setSimpleField(SESSION_ID.toString(), sessionId);
  }

  public String getState(String resourceKeyStr)
  {
    Map<String, String> mapField = _record.getMapField(resourceKeyStr);
    if (mapField != null)
    {
      return mapField.get(CURRENT_STATE.toString());
    }
    return null;
  }

  public void setStateModelDefRef(String stateModelName)
  {
    _record
        .setSimpleField(Attributes.STATE_MODEL_DEF.toString(), stateModelName);
  }

  public String getStateModelDefRef()
  {
    return _record.getSimpleField(Attributes.STATE_MODEL_DEF.toString());
  }

  public void setState(String resourceKeyStr, String state)
  {
    if (_record.getMapField(resourceKeyStr) == null)
    {
      _record.setMapField(resourceKeyStr, new TreeMap<String, String>());
    }
    _record.getMapField(resourceKeyStr).put(CURRENT_STATE.toString(), state);

  }

//  @Override
//  public ZNRecord getRecord()
//  {
//    return record;
//  }
}
