package com.linkedin.helix.model;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;

/**
 * Current states for resources in a resource group
 */
public class CurrentState extends ZNRecordDecorator
{
  private static Logger LOG = Logger.getLogger(CurrentState.class);

  public enum CurrentStateProperty
  {
    SESSION_ID,
    CURRENT_STATE,
    STATE_MODEL_DEF,
    RESOURCE_GROUP,
    STATE_MODEL_FACTORY_NAME,
  }

  public CurrentState(String id)
  {
    super(id);
  }

  public CurrentState(ZNRecord record)
  {
    super(record);
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
        map.put(resourceKey, tempMap.get(CurrentStateProperty.CURRENT_STATE.toString()));
      }
    }
    return map;
  }

  public String getSessionId()
  {
    return _record.getSimpleField(CurrentStateProperty.SESSION_ID.toString());
  }
  public void setSessionId(String sessionId)
  {
    _record.setSimpleField(CurrentStateProperty.SESSION_ID.toString(), sessionId);
  }

  public String getState(String resourceKeyStr)
  {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    Map<String, String> mapField = mapFields.get(resourceKeyStr);
    if (mapField != null)
    {
      return mapField.get(CurrentStateProperty.CURRENT_STATE.toString());
    }
    return null;
  }

  public void setStateModelDefRef(String stateModelName)
  {
    _record.setSimpleField(CurrentStateProperty.STATE_MODEL_DEF.toString(), stateModelName);
  }

  public String getStateModelDefRef()
  {
    return _record.getSimpleField(CurrentStateProperty.STATE_MODEL_DEF.toString());
  }

  public void setState(String resourceKeyStr, String state)
  {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    if (mapFields.get(resourceKeyStr) == null)
    {
      mapFields.put(resourceKeyStr, new TreeMap<String, String>());
    }
    mapFields.get(resourceKeyStr).put(CurrentStateProperty.CURRENT_STATE.toString(), state);
  }

  // public void resetState(String resourceKey)
  // {
  // Map<String, String> mapField = _record.getMapField(resourceKey);
  // if (mapField != null)
  // {
  // String state = mapField.get(CurrentStateProperty.CURRENT_STATE.toString());
  // if (state.equals("ERROR"))
  // {
  // _record.getMapFields().remove(resourceKey);
  // }
  // else
  // {
  // LOG.error("Skip resetting resource state " + resourceKey
  // + "; because it's current state is not ERROR ( was " + state + ")");
  // }
  // }
  // else
  // {
  // LOG.error("Skip resetting resource state " + resourceKey
  // + "; because it's current state does NOT exist");
  // }
  //
  // }


  public void setResourceGroup(String resourceKey, String resourceGroup)
  {
    Map<String, Map<String, String>> mapFields = _record.getMapFields();
    if (mapFields.get(resourceKey) == null)
    {
      mapFields.put(resourceKey, new TreeMap<String, String>());
    }
    mapFields.get(resourceKey).put(CurrentStateProperty.RESOURCE_GROUP.toString(), resourceGroup);

  }

  @Override
  public boolean isValid()
  {
    if(getStateModelDefRef() == null)
    {
      LOG.error("Current state does not contain state model ref. id:" + getResourceGroupName());
      return false;
    }
    if(getSessionId() == null)
    {
      LOG.error("CurrentState does not contain session id, id : " + getResourceGroupName());
      return false;
    }
    return true;
  }

}
